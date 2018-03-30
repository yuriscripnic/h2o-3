from __future__ import print_function
from builtins import range
import sys
import h2o
from h2o.estimators.glm import H2OGeneralizedLinearEstimator
import math
import os
import subprocess
from subprocess import STDOUT,PIPE
import re
import random

def verify_glm_coeffs():
  """
  In this function, I did the following:
  1. import a data file containing dataset to be predicted
  2. import a file containing the coefficients.
  3. Build H2O glm model and replace with coefficients from file
  4. Perform h2o predict with coefficients and data
  5. Manually calulate predictions and compare with H2O
  6. Generate mojo from glm mode with arbitrary coefficients;
  7. Predict with h2o mojo/pojo
  8. Compare h2o pred and h2o mojo/pojo predict.

  """
  h2o.init(strict_version_check=False) # start new or connect with h2o already started
  data = h2o.import_file("/Users/wendycwong/temp/paola/sample2.csv", header=1) # replace with your data
  cnames = data.names
  cnames.remove("flag_fr")
  # generate our glm model and be ready to replace it
  glm1 = H2OGeneralizedLinearEstimator(family="binomial", standardize=False, ignore_const_cols=False)
  glm1.train(x=cnames, y="flag_fr", training_frame=data)

  # make a dict out of coefficients used by outsiders
  coeffs = h2o.import_file("/Users/wendycwong/temp/paola/pf2_params.csv", header=0, col_types=["string","numeric"])
  cdict = dict()
  for rind in range(coeffs.nrows):
    if "intercept" in coeffs[rind,0]:
      cdict["Intercept"]=coeffs[rind,1]
    else:
      cdict[coeffs[rind,0]]=coeffs[rind,1]

  # h2o predict with arbitray coefficients
  glm2 = H2OGeneralizedLinearEstimator.makeGLMModel(model=glm1,coefs=cdict)
  h2opred = glm2.predict(data)
  # manual prediction with same coefficients and dataset
  manual_preds = manual_logistic(cdict, data, cnames)
  print("H2O Predict values: {0}".format(h2opred.as_data_frame(use_pandas=False)))
  print("Manual predict values for class 1: {0}".format(manual_preds))
  assert compareH2OManual(h2opred, manual_preds), "The two list are different"
  print("Everything Works!  H2O Predict and manual predict agrees!")

  # generate mojo and calculate mojo predict
  MOJONAME = getMojoName(glm2._id)  # grab the mojo name H2O generated automatically
  # change this path to your desired path.  Right now, it is automatically generated depending on where this file is
  TMPDIR = os.path.normpath(os.path.join(os.path.dirname(os.path.realpath('__file__')), "..", "results", MOJONAME))
  if not os.path.exists(TMPDIR):  # create directory to store mojo/pojo and related files
    os.makedirs(TMPDIR)
  h2o.download_csv(data, os.path.join(TMPDIR, 'in.csv'))  # save test file, h2o predict/mojo use same file
  pred_h2o, pred_mojo = mojo_predict(glm2, TMPDIR, MOJONAME)  # load model and perform predict
  h2o.download_csv(pred_h2o, os.path.join(TMPDIR, "h2oPred.csv"))
  print("Comparing mojo predict and h2o predict...")
  compare_frames_local(pred_h2o, pred_mojo, 0.1, tol=1e-10)
  pred_pojo = pojo_predict(glm2, TMPDIR, MOJONAME)
  compare_frames_local(pred_h2o, pred_pojo, 0.1, tol=1e-10)

# perform pojo predict.  Frame containing pojo predict is returned.
def pojo_predict(model, tmpdir, pojoname):
  """
  This method will show you how to generate a pojo from a model and perform
  prediction using that pojo from Python.

  :param model: H2O model that you want a pojo of
  :param tmpdir: directory where you want to save the pojo in
  :param pojoname: name of the pojo file
  :return: prediction from your pojo from dataset stored in tmpdir/in.csv
  """
  h2o.download_pojo(model, path=tmpdir)
  h2o_genmodel_jar = os.path.join(tmpdir, "h2o-genmodel.jar")
  java_file = os.path.join(tmpdir, pojoname + ".java")

  in_csv = (os.path.join(tmpdir, 'in.csv'))   # import the test dataset
  print("Compiling Java Pojo")
  javac_cmd = ["javac", "-cp", h2o_genmodel_jar, "-J-Xmx12g", java_file]
  subprocess.check_call(javac_cmd)

  out_pojo_csv = os.path.join(tmpdir, "out_pojo.csv")
  cp_sep = ";" if sys.platform == "win32" else ":"
  java_cmd = ["java", "-ea", "-cp", h2o_genmodel_jar + cp_sep + tmpdir, "-Xmx12g",
              "-XX:ReservedCodeCacheSize=256m", "hex.genmodel.tools.PredictCsv",
              "--pojo", pojoname, "--input", in_csv, "--output", out_pojo_csv, "--decimal"]

  p = subprocess.Popen(java_cmd, stdout=PIPE, stderr=STDOUT)
  o, e = p.communicate()
  print("Java output: {0}".format(o))
  assert os.path.exists(out_pojo_csv), "Expected file {0} to exist, but it does not.".format(out_pojo_csv)
  predict_pojo = h2o.import_file(out_pojo_csv, header=1)
  return predict_pojo

# grab the automatically generated mojo/pojo name used by H2O
def getMojoName(modelID):
  regex = re.compile("[+\\-* !@#$%^&()={}\\[\\]|;:'\"<>,.?/]")
  return regex.sub("_", modelID)

# only work for Python 2
def compareH2OManual2(h2oPred, mPred):
  h2olocal = h2oPred.as_data_frame()
  for rind in range(h2oPred.nrows):
    if abs(h2olocal["p1"].iloc[rind]-mPred[rind]) > 1e-10:
      return False
  return True

# only work for Python 3
def compareH2OManual(h2oPred, mPred):
  h2olocal = h2oPred.as_data_frame()#may need to set use_pandas=False
  for rind in range(h2oPred.nrows):
    if abs(float(h2olocal[rind+1][2])-mPred[rind]) > 1e-10:
      return False
  return True

# compare two numerical frames and make sure they are equal to within tolerance
def compare_frames_local(f1, f2, prob=0.5, tol=1e-6):
  temp1 = f1.as_data_frame(use_pandas=False)
  temp2 = f2.as_data_frame(use_pandas=False)
  assert (f1.nrow==f2.nrow) and (f1.ncol==f2.ncol), "The two frames are of different sizes."
  for colInd in range(f1.ncol):
    for rowInd in range(1,f2.nrow):
      if (random.uniform(0,1) < prob):
        if (math.isnan(float(temp1[rowInd][colInd]))):
          assert math.isnan(float(temp2[rowInd][colInd])), "Failed frame values check at row {2} and column {3}! " \
                                                           "frame1 value: {0}, frame2 value: " \
                                                           "{1}".format(temp1[rowInd][colInd], temp2[rowInd][colInd], rowInd, colInd)
        else:
          v1 = float(temp1[rowInd][colInd])
          v2 = float(temp2[rowInd][colInd])
          diff = abs(v1-v2)/max(1.0, abs(v1), abs(v2))
          assert diff<=tol, "Failed frame values check at row {2} and column {3}! frame1 value: {0}, frame2 value: " \
                            "{1}".format(v1, v2, rowInd, colInd)

# only work for Python 2
def manual_logistic2(coeffs, data, cnames):
  """
  The function given coefficients of a logistic glm in a dictionary format, dataset, column names of
  dataset in cnames will generate the probability that a data row = 1.

  :param coeffs: dict() containing the column names and the coefficient values for that column plus Intercept
  :param data: h2o frame containing dataset that needs prediction
  :param cnames: column names excluding the response column name
  :return: prediction that the dataset belongs to class 1.
  """
  preds = [coeffs.get("Intercept")]*data.nrow
  datalocal = data.as_data_frame()
  for rind in range(data.nrows):
    temp = coeffs.get("Intercept")
    for cname in cnames:
      #ind = datalocal[0].index(cname)
      temp = temp+coeffs.get(cname)*datalocal[cname].iloc[rind]
    preds[rind]=1.0/(1.0+math.exp(-temp))

  return preds

# only work for Python 3
def manual_logistic(coeffs, data, cnames):
  """
  The function given coefficients of a logistic glm in a dictionary format, dataset, column names of
  dataset in cnames will generate the probability that a data row = 1.

  :param coeffs: dict() containing the column names and the coefficient values for that column plus Intercept
  :param data: h2o frame containing dataset that needs prediction
  :param cnames: column names excluding the response column name
  :return: prediction that the dataset belongs to class 1.

  Megan Kurka added speedup to perform vector operations instead of element-wise
  """
  #preds = [coeffs.get("Intercept")]*data.nrow
  #preds=[]
  preds = [coeffs.get("Intercept")]*data.nrow
  datalocal = data.as_data_frame()

  for rind in range(data.nrows):
    temp = coeffs.get("Intercept")
    for cname in cnames:
      ind = datalocal[0].index(cname)
      temp = temp + coeffs.get(cname) * float(datalocal[rind+1][ind])
    #preds.append(1.0/(1.0+math.exp(-1.0*temp)))
    preds[rind] = 1.0/(1.0+math.exp(-1.0*temp))
  return preds

def mojo_predict(model,tmpdir, mojoname):
  """
  perform h2o predict and mojo predict.  Frames containing h2o prediction is returned and mojo predict are returned.
  It is assumed that the input data set is saved as in.csv in tmpdir directory.

  :param model: h2o model where you want to use to perform prediction
  :param tmpdir: directory where your mojo zip files are stired
  :param mojoname: name of your mojo zip file.
  :return: the h2o prediction frame and the mojo prediction frame
  """
  newTest = h2o.import_file(os.path.join(tmpdir, 'in.csv'), header=1)   # Make sure h2o and mojo use same in.csv
  predict_h2o = model.predict(newTest)  # generate h2o predict from model directly

  # load mojo and have it do predict
  outFileName = os.path.join(tmpdir, 'out_mojo.csv')  # generate filename to store mojo predict
  model.download_mojo(path=tmpdir)    # save mojo
  mojoZip = os.path.join(tmpdir, mojoname) + ".zip" # generate file name with path to store mojo
  genJarDir = str.split(str(tmpdir),'/')
  genJarDir = '/'.join(genJarDir[0:genJarDir.index('h2o-py')])    # locate directory of genmodel.jar
  java_cmd = ["java", "-ea", "-cp", os.path.join(genJarDir, "h2o-assemblies/genmodel/build/libs/genmodel.jar"),
              "-Xmx12g", "-XX:MaxPermSize=2g", "-XX:ReservedCodeCacheSize=256m", "hex.genmodel.tools.PredictCsv",
              "--input", os.path.join(tmpdir, 'in.csv'), "--output",
              outFileName, "--mojo", mojoZip, "--decimal"]
  p = subprocess.Popen(java_cmd, stdout=PIPE, stderr=STDOUT)
  o, e = p.communicate()
  pred_mojo = h2o.import_file(os.path.join(tmpdir, 'out_mojo.csv'), header=1)  # load mojo prediction into a frame and compare
  #    os.remove(mojoZip)
  return predict_h2o, pred_mojo


if __name__ == "__main__":
  verify_glm_coeffs()
