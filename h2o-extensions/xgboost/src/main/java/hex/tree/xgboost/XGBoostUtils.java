package hex.tree.xgboost;

import hex.DataInfo;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoostError;
import water.Key;
import water.MemoryManager;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;
import water.util.VecUtils;

import java.util.*;

import static water.H2O.technote;
import static water.MemoryManager.getFreeMemory;
import static water.MemoryManager.malloc4;
import static water.MemoryManager.malloc4f;

public class XGBoostUtils {

    private static final int ALLOCATED_ARRAY_LEN = 1048576; // 1 << 20

    public static String makeFeatureMap(Frame f, DataInfo di) {
        // set the names for the (expanded) columns
        String[] coefnames = di.coefNames();
        StringBuilder sb = new StringBuilder();
        assert(coefnames.length == di.fullN());
        for (int i = 0; i < di.fullN(); ++i) {
            sb.append(i).append(" ").append(coefnames[i].replaceAll("\\s*","")).append(" ");
            int catCols = di._catOffsets[di._catOffsets.length-1];
            if (i < catCols || f.vec(i-catCols).isBinary())
                sb.append("i");
            else if (f.vec(i-catCols).isInt())
                sb.append("int");
            else
                sb.append("q");
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * convert an H2O Frame to a sparse DMatrix
     * @param f H2O Frame
     * @param onlyLocal if true uses only chunks local to this node
     * @param response name of the response column
     * @param weight name of the weight column
     * @param fold name of the fold assignment column
     * @return DMatrix
     * @throws XGBoostError
     */
    public static DMatrix convertFrameToDMatrix(Key<DataInfo> dataInfoKey,
                                         Frame f,
                                         boolean onlyLocal,
                                         String response,
                                         String weight,
                                         String fold,
                                         boolean sparse) throws XGBoostError {

        int[] chunks;
        Vec vec = f.anyVec();
        if(!onlyLocal) {
            // All chunks
            chunks = new int[f.anyVec().nChunks()];
            for(int i = 0; i < chunks.length; i++) {
                chunks[i] = i;
            }
        } else {
            chunks = VecUtils.getLocalChunkIds(f.anyVec());
        }

        long nRowsL = 0;
        for(int chId : chunks) {
            nRowsL += vec.chunkLen(chId);
        }

        if(0 == nRowsL) {
            return null;
        }

        int nRows = (int) nRowsL;

        final DataInfo di = dataInfoKey.get();
        final DMatrix trainMat;
        Vec.Reader w = weight == null ? null : f.vec(weight).new Reader();
        Vec.Reader[] vecs = new Vec.Reader[f.numCols()];
        for (int i = 0; i < vecs.length; ++i) {
            vecs[i] = f.vec(i).new Reader();
        }

        // In the future this 2 arrays might also need to be rewritten into float[][],
        // but only if we want to handle datasets over 2^31-1 on a single machine. For now I'd leave it as it is.
        float[] resp = malloc4f(nRows);
        float[] weights = null;
        if(null != w) {
            weights = malloc4f(nRows);
        }
        try {
            if (sparse) {
                Log.info("Treating matrix as sparse.");
                // 1 0 2 0
                // 4 0 0 3
                // 3 1 2 0
                boolean csc = false; //di._cats == 0;

                // truly sparse matrix - no categoricals
                // collect all nonzeros column by column (in parallel), then stitch together into final data structures
                if (csc) {
                    trainMat = csc(f, chunks, w, f.vec(response).new Reader(), nRows, di, resp, weights);
                } else {
                    trainMat = csr(f, chunks, vecs, w, f.vec(response).new Reader(), nRows, di, resp, weights);
                }
            } else {
                Log.info("Treating matrix as dense.");

                int cols = di.fullN();
                float[][] data = new float[getDataRows(chunks, vec, cols)][];
                data[0] = malloc4f(ALLOCATED_ARRAY_LEN);
                long actualRows = denseChunk(data, chunks, f, vecs, w, di, cols, resp, weights, f.vec(response).new Reader());
                int lastRowSize = (int)(actualRows * cols % ARRAY_MAX);
                if(data[data.length - 1].length > lastRowSize) {
                    data[data.length - 1] = Arrays.copyOf(data[data.length - 1], lastRowSize);
                }
                trainMat = new DMatrix(data, actualRows, cols, Float.NaN);
                assert trainMat.rowNum() == actualRows;
            }
        } catch (NegativeArraySizeException e) {
            throw new IllegalArgumentException(
                    technote(11, "Data is too large to fit into the 32-bit Java float[] array that needs to be passed to the XGBoost C++ backend. Use H2O GBM instead."),
                    e
            );
        }

        int len = (int) trainMat.rowNum();
        resp = Arrays.copyOf(resp, len);
        trainMat.setLabel(resp);
        if (w!=null) {
            weights = Arrays.copyOf(weights, len);
            trainMat.setWeight(weights);
        }
//    trainMat.setGroup(null); //fold //FIXME - only needed if CV is internally done in XGBoost
        return trainMat;
    }

    // FIXME this and the other method should subtract rows where response is 0
    private static int getDataRows(Chunk[] chunks, Frame f, int[] chunksIds, int cols) {
        double totalRows = 0;
        if(null != chunks) {
            for (Chunk ch : chunks) {
                totalRows += ch.len();
            }
        } else {
            for(int chunkId : chunksIds) {
                totalRows += f.anyVec().chunkLen(chunkId);
            }
        }
        return (int) Math.ceil(totalRows * cols / ARRAY_MAX);
    }

    private static int getDataRows(int[] chunks, Vec vec, int cols) {
        double totalRows = 0;
        for(int ch : chunks) {
            totalRows += vec.chunkLen(ch);
        }
        return (int) Math.ceil(totalRows * cols / ARRAY_MAX);
    }

    private static int setResponseAndWeight(Chunk[] chunks, int respIdx, int weightIdx, float[] resp, float[] weights, int j, int i) {
        if (weightIdx != -1) {
            if(chunks[weightIdx].atd(i) == 0) {
                return j;
            }
            weights[j] = (float) chunks[weightIdx].atd(i);
        }
        resp[j++] = (float) chunks[respIdx].atd(i);
        return j;
    }

    private static int setResponseAndWeight(Vec.Reader w, float[] resp, float[] weights, Vec.Reader respVec, int j, long i) {
        if (w != null) {
            if(w.at(i) == 0) {
                return j;
            }
            weights[j] = (float) w.at(i);
        }
        resp[j++] = (float) respVec.at(i);
        return j;
    }

    private static int getNzCount(Frame f, int[] chunks, final Vec.Reader w, int nCols, List<SparseItem>[] col, int nzCount) {
        for (int i=0;i<nCols;++i) { //TODO: parallelize over columns
            Vec v = f.vec(i);
            for (Integer c : chunks) {
                Chunk ck = v.chunkForChunkIdx(c);
                int[] nnz = new int[ck.sparseLenZero()];
                int nnzCount = ck.nonzeros(nnz);
                nzCount = getNzCount(new ZeroWeight() {
                    @Override
                    public boolean zeroWeight(int idx) {
                        return w != null && w.at(idx) == 0;
                    }
                }, col[i], nzCount, ck, nnz, nnzCount, false);
            }
        }
        return nzCount;
    }

    interface ZeroWeight {
        boolean zeroWeight(int idx);
    }

    private static int getNzCount(final Chunk[] chunks, final int weight, int nCols, List<SparseItem>[] col, int nzCount) {
        for (int i=0;i<nCols;++i) { //TODO: parallelize over columns
            final Chunk ck = chunks[i];
            int[] nnz = new int[ck.sparseLenZero()];
            int nnzCount = ck.nonzeros(nnz);
            nzCount = getNzCount(new ZeroWeight() {
                @Override
                public boolean zeroWeight(int idx) {
                    return weight != -1 && ck.atd(idx) == 0;
                }
            }, col[i], nzCount, ck, nnz, nnzCount, true);
        }
        return nzCount;
    }

    private static int getNzCount(ZeroWeight zw, List<SparseItem> sparseItems, int nzCount, Chunk ck, int[] nnz, int nnzCount, boolean localWeight) {
        for (int k=0;k<nnzCount;++k) {
            SparseItem item = new SparseItem();
            int localIdx = nnz[k];
            item.pos = (int)ck.start() + localIdx;
            // both 0 and NA are omitted in the sparse DMatrix
            if (zw.zeroWeight(localWeight ? localIdx : item.pos)) continue;
            if (ck.isNA(localIdx)) continue;
            item.val = ck.atd(localIdx);
            sparseItems.add(item);
            nzCount++;
        }
        return nzCount;
    }

    /**
     * convert a set of H2O chunks (representing a part of a vector) to a sparse DMatrix
     * @param response name of the response column
     * @param weight name of the weight column
     * @param fold name of the fold assignment column
     * @return DMatrix
     * @throws XGBoostError
     */
    public static DMatrix convertChunksToDMatrix(Key<DataInfo> dataInfoKey,
                                          Chunk[] chunks,
                                          int response,
                                          int weight,
                                          int fold,
                                          boolean sparse) throws XGBoostError {
        long nRows = chunks[0]._len;

        DMatrix trainMat;

        DataInfo di = dataInfoKey.get();

        float[] resp = malloc4f((int) nRows);
        float[] weights = null;
        if(-1 != weight) {
            weights = malloc4f((int) nRows);
        }
        try {
            if (sparse) {
                Log.info("Treating matrix as sparse.");
                // 1 0 2 0
                // 4 0 0 3
                // 3 1 2 0
                boolean csc = false; //di._cats == 0;

                // truly sparse matrix - no categoricals
                // collect all nonzeros column by column (in parallel), then stitch together into final data structures
                if (csc) {
                    trainMat = csc(chunks, weight, nRows, di, resp, weights);
                } else {
                    trainMat = csr(chunks, weight, response, (int) nRows, di, resp, weights);
                }
            } else {
                trainMat = dense(chunks, weight, di, response, resp, weights);
            }
        } catch (NegativeArraySizeException e) {
            throw new IllegalArgumentException(technote(11, "Data is too large to fit into the 32-bit Java float[] array that needs to be passed to the XGBoost C++ backend. Use H2O GBM instead."));
        }

        int len = (int) trainMat.rowNum();
        resp = Arrays.copyOf(resp, len);
        trainMat.setLabel(resp);
        if (weight!=-1){
            weights = Arrays.copyOf(weights, len);
            trainMat.setWeight(weights);
        }
//    trainMat.setGroup(null); //fold //FIXME - only needed if CV is internally done in XGBoost
        return trainMat;
    }

    /****************************************************************************************************************
     ************************************** DMatrix creation for dense matrices *************************************
     ****************************************************************************************************************/

    private static DMatrix dense(Chunk[] chunks, int weight, DataInfo di, int respIdx, float[] resp, float[] weights) throws XGBoostError {
        DMatrix trainMat;
        Log.info("Treating matrix as dense.");

        // extract predictors
        int cols = di.fullN();
        float[][] data = new float[getDataRows(chunks, null, null, cols)][];
        data[0] = malloc4f(ALLOCATED_ARRAY_LEN);

        long actualRows = denseChunk(data, chunks, weight, respIdx, di, cols, resp, weights);
        int lastRowSize = (int)((double)actualRows * cols % ARRAY_MAX);
        if(data[data.length - 1].length > lastRowSize) {
            data[data.length - 1] = Arrays.copyOf(data[data.length - 1], lastRowSize);
        }
        trainMat = new DMatrix(data, actualRows, cols, Float.NaN);
        assert trainMat.rowNum() == actualRows;
        return trainMat;
    }

    private static final int ARRAY_MAX = Integer.MAX_VALUE - 10;

    private static long denseChunk(float[][] data,
                                  int[] chunks, Frame f, // for MR task
                                  Vec.Reader[] vecs, Vec.Reader w, // for setupLocal
                                  DataInfo di, int cols,
                                  float[] resp, float[] weights, Vec.Reader respVec) {
        int currentRow = 0;
        int currentCol = 0;
        long actualRows = 0;
        int rwRow = 0;
        for (Integer chunk : chunks) {
            for(long i = f.anyVec().espc()[chunk]; i < f.anyVec().espc()[chunk+1]; i++) {
                if (w != null && w.at(i) == 0) continue;

                enlargeFloatTable(data, cols, currentRow, currentCol);

                for (int j = 0; j < di._cats; ++j) {
                    int offset = di._catOffsets[j+1] - di._catOffsets[j];
                    int pos;
                    if (vecs[j].isNA(i)) {
                        pos = di.getCategoricalId(j, Double.NaN);
                    } else {
                        pos = di.getCategoricalId(j, vecs[j].at8(i));
                    }
                    // Relative position, not absolute
                    pos -= di._catOffsets[j];

                    // currentCol + pos might overflow int and the size of current row
                    if (currentCol + pos < data[currentRow].length && currentCol + pos >= 0) {
                        data[currentRow][currentCol + pos] = 1;
                    }
                    if (currentCol + offset >= data[currentRow].length || currentCol + offset < 0) { // did we advance to next row?
                        pos = currentCol + pos - data[currentRow].length;
                        offset = currentCol + offset - data[currentRow].length;
                        currentRow++;

                        if(currentRow > ARRAY_MAX) {
                            throw new IllegalStateException(
                                    "Data too big to be used in XGBoost. Currently we can handle only up to "  +
                                            ((double)ARRAY_MAX * (double)ARRAY_MAX) +
                                            " entries (after encodings etc.)."
                            );
                        }

                        currentCol = 0;
                        if (pos >= 0) { // was not written in previous row, need to write here
                            data[currentRow][currentCol + pos] = 1;
                        }
                    }
                    currentCol += offset;
                }
                for (int j = 0; j < di._nums; ++j) {
                    if(currentCol == ARRAY_MAX) {
                        currentCol = 0;
                        currentRow++;
                    }
                    if (vecs[di._cats + j].isNA(i)) {
                        data[currentRow][currentCol++] = Float.NaN;
                    }
                    else {
                        data[currentRow][currentCol++] = (float) vecs[di._cats + j].at(i);
                    }
                }
                actualRows++;

                rwRow = setResponseAndWeight(w, resp, weights, respVec, rwRow, i);
            }
        }
        return actualRows;
    }

    private static long denseChunk(float[][] data, Chunk[] chunks, int weight, int respIdx, DataInfo di, int cols, float[] resp, float[] weights) {
        int currentRow = 0;
        int currentCol = 0;
        long actualRows = 0;
        int rwRow = 0;

        for (int i = 0; i < chunks[0].len(); i++) {
            if (weight != -1 && chunks[weight].atd(i) == 0) continue;
            // Enlarge the table if necessary
            enlargeFloatTable(data, cols, currentRow, currentCol);

            for (int j = 0; j < di._cats; ++j) {
                int offset = di._catOffsets[j+1] - di._catOffsets[j];
                int pos;
                if (chunks[j].isNA(i)) {
                    pos = di.getCategoricalId(j, Double.NaN);
                } else {
                    pos = di.getCategoricalId(j, chunks[j].at8(i));
                }
                // Relative position, not absolute
                pos -= di._catOffsets[j];

                if (currentCol + pos < data[currentRow].length) {
                    data[currentRow][currentCol + pos] = 1;
                }
                if (currentCol + offset >= data[currentRow].length) { // did we advance to next row?
                    pos = currentCol + pos - data[currentRow].length;
                    offset = currentCol + offset - data[currentRow].length;
                    currentRow++;
                    currentCol = 0;
                    if (pos >= 0) { // was not written in previous row, need to write here
                        data[currentRow][currentCol + pos] = 1;
                    }
                }
                currentCol += offset;
            }

            for (int j = 0; j < di._nums; ++j) {
                if(currentCol == ARRAY_MAX) {
                    currentCol = 0;
                    currentRow++;
                }
                if (chunks[di._cats + j].isNA(i)) {
                    data[currentRow][currentCol++] = Float.NaN;
                }
                else {
                    data[currentRow][currentCol++] = (float) chunks[di._cats + j].atd(i);
                }
            }
            assert di._catOffsets[di._catOffsets.length - 1] + di._nums == cols;
            actualRows++;

            rwRow = setResponseAndWeight(chunks, respIdx, weight, resp, weights, rwRow, i);
        }
        return actualRows;
    }

    /****************************************************************************************************************
     *********************************** DMatrix creation for sparse (CSR) matrices *********************************
     ****************************************************************************************************************/

    private static DMatrix csr(Frame f, int[] chunksIds, Vec.Reader[] vecs, Vec.Reader w, Vec.Reader respReader, // for setupLocal
                               int nRows, DataInfo di, float[] resp, float[] weights)
            throws XGBoostError {
        return csr(null, -1, -1, f, chunksIds, vecs, w, respReader, nRows, di, resp, weights);
    }

    private static DMatrix csr(Chunk[] chunks, int weight, int respIdx, // for MR task
                               int nRows, DataInfo di, float[] resp, float[] weights) throws XGBoostError {
        return csr(chunks, weight, respIdx, null, null, null, null, null, nRows, di, resp, weights);
    }

    private static DMatrix csr(Chunk[] chunks, int weight, int respIdx, // for MR task
                               Frame f, int[] chunksIds, Vec.Reader[] vecs, Vec.Reader w, Vec.Reader respReader, // for setupLocal
                               int nRows, DataInfo di, float[] resp, float[] weights)
            throws XGBoostError {
        DMatrix trainMat;
        int actualRows = 0;
        // CSR:
//    long[] rowHeaders = new long[] {0,      2,      4,         7}; //offsets
//    float[] data = new float[]     {1f,2f,  4f,3f,  3f,1f,2f};     //non-zeros across each row
//    int[] colIndex = new int[]     {0, 2,   0, 3,   0, 1, 2};      //col index for each non-zero

        long[][] rowHeaders = new long[1][nRows + 1];
        int initial_size = 1 << 20;
        float[][] data = new float[getDataRows(chunks, f, chunksIds, di.fullN())][initial_size];
        int[][] colIndex = new int[1][initial_size];

        // extract predictors
        rowHeaders[0][0] = 0;
        if(null != chunks) {
            actualRows = initalizeFromChunks(
                    chunks, weight,
                    di, actualRows, rowHeaders, data, colIndex,
                    respIdx, resp, weights);
        } else {
            actualRows = initalizeFromChunkIds(
                    f, chunksIds, vecs, w,
                    di, actualRows, rowHeaders, data, colIndex,
                    respReader, resp, weights);
        }


        long size = 0;
        for(int i = 0; i < data.length; i++) {
            size += data[i].length;
        }

        int rowHeadersSize = 0;
        for(int i = 0; i < rowHeaders.length; i++) {
            rowHeadersSize += rowHeaders[i].length;
        }

        trainMat = new DMatrix(rowHeaders, colIndex, data, DMatrix.SparseType.CSR, di.fullN(), rowHeadersSize, size);
        assert trainMat.rowNum() == actualRows;
        return trainMat;
    }

    private static int initalizeFromChunkIds(Frame f, int[] chunks, Vec.Reader[] vecs, Vec.Reader w, DataInfo di, int actualRows,
                                             long[][] rowHeaders, float[][] data, int[][] colIndex,
                                             Vec.Reader respVec, float[] resp, float[] weights) {
        // CSR:
//    long[] rowHeaders = new long[] {0,      2,      4,         7}; //offsets
//    float[] data = new float[]     {1f,2f,  4f,3f,  3f,1f,2f};     //non-zeros across each row
//    int[] colIndex = new int[]     {0, 2,   0, 3,   0, 1, 2};      //col index for each non-zero

        // extract predictors
        int nz = 0;
        int currentRow = 0;
        int currentCol = 0;
        int rwRow = 0;
        rowHeaders[0][0] = 0;
        for (Integer chunk : chunks) {
            for(long i = f.anyVec().espc()[chunk]; i < f.anyVec().espc()[chunk+1]; i++) {
                if (w != null && w.at(i) == 0) continue;
                int nzstart = nz;
                // enlarge final data arrays by 2x if needed
                enlargeTables(data, colIndex, di._cats + di._nums, currentRow, currentCol);

                for (int j = 0; j < di._cats; ++j) {
                    if (!vecs[j].isNA(i)) {
                        data[currentRow][currentCol] = 1; //one-hot encoding
                        colIndex[currentRow][currentCol++] = di.getCategoricalId(j, vecs[j].at8(i));
                        nz++;
                    } else {
                        // NA == 0 for sparse -> no need to fill
//            data[nz] = 1; //one-hot encoding
//            colIndex[nz] = di.getCategoricalId(j, Double.NaN); //Fill NA bucket
//            nz++;
                    }
                }

                for (int j = 0; j < di._nums; ++j) {
                    float val = (float) vecs[di._cats + j].at(i);
                    if (!Float.isNaN(val) && val != 0) {
                        data[currentRow][currentCol] = val;
                        colIndex[currentRow][currentCol++] = di._catOffsets[di._catOffsets.length - 1] + j;
                        nz++;
                    }
                }
                if (nz == nzstart) {
                    // for the corner case where there are no categorical values, and all numerical values are 0, we need to
                    // assign a 0 value to any one column to have a consistent number of rows between the predictors and the special vecs (weight/response/etc.)
                    data[currentRow][currentCol] = 0;
                    colIndex[currentRow][currentCol++] = 0;
                    nz++;
                }
                rowHeaders[0][++actualRows] = nz;

                rwRow = setResponseAndWeight(w, resp, weights, respVec, rwRow, i);
            }
        }

        data[data.length - 1] = Arrays.copyOf(data[data.length - 1], nz % ARRAY_MAX);
        colIndex[colIndex.length - 1] = Arrays.copyOf(colIndex[colIndex.length - 1], nz % ARRAY_MAX);

        rowHeaders[0] = Arrays.copyOf(rowHeaders[rowHeaders.length - 1], actualRows + 1);
        return actualRows;
    }

    private static int initalizeFromChunks(Chunk[] chunks, int weight, DataInfo di, int actualRows, long[][] rowHeaders, float[][] data, int[][] colIndex, int respIdx, float[] resp, float[] weights) {
        int nz = 0;
        int currentRow = 0;
        int currentCol = 0;
        int rwRow = 0;

        for (int i = 0; i < chunks[0].len(); i++) {
            if (weight != -1 && chunks[weight].atd(i) == 0) continue;
            int nzstart = nz;

            enlargeTables(data, colIndex, di._cats + di._nums, currentRow, currentCol);

            for (int j = 0; j < di._cats; ++j) {
                if (!chunks[j].isNA(i)) {
                    data[currentRow][currentCol] = 1; //one-hot encoding
                    colIndex[currentRow][currentCol++] = di.getCategoricalId(j, chunks[j].at8(i));
                    nz++;
                } else {
                    // NA == 0 for sparse -> no need to fill
//            data[nz] = 1; //one-hot encoding
//            colIndex[nz] = di.getCategoricalId(j, Double.NaN); //Fill NA bucket
//            nz++;
                }
            }
            for (int j = 0; j < di._nums; ++j) {
                float val = (float) chunks[di._cats + j].atd(i);
                if (!Float.isNaN(val) && val != 0) {
                    data[currentRow][currentCol] = val;
                    colIndex[currentRow][currentCol++] = di._catOffsets[di._catOffsets.length - 1] + j;
                    nz++;
                }
            }
            if (nz == nzstart) {
                // for the corner case where there are no categorical values, and all numerical values are 0, we need to
                // assign a 0 value to any one column to have a consistent number of rows between the predictors and the special vecs (weight/response/etc.)
                data[currentRow][currentCol] = 0;
                colIndex[currentRow][currentCol++] = 0;
                nz++;
            }
            rowHeaders[0][++actualRows] = nz;

            rwRow = setResponseAndWeight(chunks, respIdx, weight, resp, weights, rwRow, i);
        }

        data[data.length - 1] = Arrays.copyOf(data[data.length - 1], nz % ARRAY_MAX);
        colIndex[colIndex.length - 1] = Arrays.copyOf(colIndex[colIndex.length - 1], nz % ARRAY_MAX);

        rowHeaders[0] = Arrays.copyOf(rowHeaders[rowHeaders.length - 1], actualRows + 1);
        return actualRows;
    }

    static class SparseItem {
        int pos;
        double val;
    }

    /****************************************************************************************************************
     *********************************** DMatrix creation for sparse (CSC) matrices *********************************
     ****************************************************************************************************************/

    private static DMatrix csc(Chunk[] chunks, int weight,
                               long nRows, DataInfo di,
                               float[] resp, float[] weights) throws XGBoostError {
        return csc(chunks, weight, null, null, null, null, nRows, di, resp, weights);
    }

    private static DMatrix csc(Frame f, int[] chunksIds, Vec.Reader w, Vec.Reader respReader,
                               long nRows, DataInfo di,
                               float[] resp, float[] weights) throws XGBoostError {
        return csc(null, -1, f, chunksIds, w, respReader, nRows, di, resp, weights);
    }

    private static DMatrix csc(Chunk[] chunks, int weight, // for MR tasks
                               Frame f, int[] chunksIds, Vec.Reader w, Vec.Reader respReader, // for setupLocal computation
                               long nRows, DataInfo di,
                               float[] resp, float[] weights) throws XGBoostError {
        DMatrix trainMat;

        // CSC:
        //    long[] colHeaders = new long[] {0,        3,  4,     6,    7}; //offsets
        //    float[] data = new float[]     {1f,4f,3f, 1f, 2f,2f, 3f};      //non-zeros down each column
        //    int[] rowIndex = new int[]     {0,1,2,    2,  0, 2,  1};       //row index for each non-zero

        int nCols = di._nums;

        List<SparseItem>[] col = new List[nCols]; //TODO: use more efficient storage (no GC)
        // allocate
        for (int i=0;i<nCols;++i) {
            col[i] = new ArrayList<>((int)Math.min(nRows, 10000));
        }

        // collect non-zeros
        int nzCount = 0;
        if(null != chunks) {
            nzCount = getNzCount(chunks, weight, nCols, col, nzCount);
        } else {
            nzCount = getNzCount(f, chunksIds, w, nCols, col, nzCount);
        }

        int currentRow = 0;
        int currentCol = 0;
        int nz = 0;
        long[][] colHeaders = new long[1][nCols + 1];
        float[][] data = new float[getDataRows(chunks, f, chunksIds, di.fullN())][nzCount];
        int[][] rowIndex = new int[1][nzCount];
        int rwRow = 0;
        // fill data for DMatrix
        for (int i=0;i<nCols;++i) { //TODO: parallelize over columns
            List sparseCol = col[i];
            colHeaders[0][i] = nz;

            enlargeTables(data, rowIndex, sparseCol.size(), currentRow, currentCol);

            for (int j=0;j<sparseCol.size();++j) {
                if(currentCol == ARRAY_MAX) {
                    currentCol = 0;
                    currentRow++;
                }

                SparseItem si = (SparseItem)sparseCol.get(j);
                rowIndex[currentRow][currentCol] = si.pos;
                data[currentRow][currentCol] = (float)si.val;
                assert(si.val != 0);
                assert(!Double.isNaN(si.val));
//                assert(weight == -1 || chunks[weight].atd((int)(si.pos - chunks[weight].start())) != 0);
                nz++;
                currentCol++;

                // Do only once
                if(0 == i) {
                    rwRow = setResponseAndWeight(w, resp, weights, respReader, rwRow, j);
                }
            }
        }
        colHeaders[0][nCols] = nz;
        data[data.length - 1] = Arrays.copyOf(data[data.length - 1], nz % ARRAY_MAX);
        rowIndex[rowIndex.length - 1] = Arrays.copyOf(rowIndex[rowIndex.length - 1], nz % ARRAY_MAX);
        int actualRows = countUnique(rowIndex);

        trainMat = new DMatrix(colHeaders, rowIndex, data, DMatrix.SparseType.CSC, actualRows, di.fullN(), nz);
        assert trainMat.rowNum() == actualRows;
        assert trainMat.rowNum() == rwRow;
        return trainMat;
    }

    private static int countUnique(int[][] array) {
        if (array.length == 0) {
            return 0;
        }

        BitSet values = new BitSet(ARRAY_MAX);

        int count = 1;
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[i].length - 1; j++) {
                if (!values.get(array[i][j])) {
                    count++;
                    values.set(array[i][j]);
                }
            }
        }
        return count;
    }

    // Assumes both matrices are getting filled at the same rate and will require the same amount of space
    private static void enlargeTables(float[][] data, int[][] rowIndex, int cols, int currentRow, int currentCol) {
        while (data[currentRow].length < currentCol + cols) {
            if(data[currentRow].length == ARRAY_MAX) {
                currentCol = 0;
                cols -= (data[currentRow].length - currentCol);
                currentRow++;
                data[currentRow] = malloc4f(ALLOCATED_ARRAY_LEN);
                rowIndex[currentRow] = malloc4(ALLOCATED_ARRAY_LEN);
            } else {
                int newLen = (int) Math.min(Math.min((long) data[currentRow].length << 1L, getFreeMemory() / 4), (long) ARRAY_MAX);
                Log.info("Enlarging dense data structures row from " + data[currentRow].length + " float entries to " + newLen + " entries.");
                data[currentRow] = Arrays.copyOf(data[currentRow], newLen);
                rowIndex[currentRow] = Arrays.copyOf(rowIndex[currentRow], newLen);
            }
        }
    }

    private static void enlargeFloatTable(float[][] data, int cols, int currentRow, int currentCol) {
        while (data[currentRow].length < (long)currentCol + cols) {
            if(data[currentRow].length == ARRAY_MAX) {
                currentCol = 0;
                cols -= (data[currentRow].length - currentCol);
                currentRow++;
                int allocatedMemory = getFreeMemory() / 4 > ALLOCATED_ARRAY_LEN ? ALLOCATED_ARRAY_LEN : (int) getFreeMemory() / 4;
                data[currentRow] = malloc4f(allocatedMemory);
            } else {
                int newLen = (int) Math.min(Math.min((long) data[currentRow].length << 1L, getFreeMemory() / 4), (long) ARRAY_MAX);
                Log.info("Enlarging dense data structure row from " + data[currentRow].length + " bytes to " + newLen + " bytes.");
                data[currentRow] = MemoryManager.arrayCopyOf(data[currentRow], newLen);
            }
        }
    }

}
