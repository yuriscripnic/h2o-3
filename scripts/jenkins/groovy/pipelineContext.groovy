def call(final String h2o3Root, final String mode, final scmEnv, final boolean ignoreChanges) {
    return call(h2o3Root, mode, scmEnv, ignoreChanges, null)
}

def call(final String h2o3Root, final String mode, final scmEnv, final boolean ignoreChanges, final List<String> gradleOpts) {
    final String BUILD_SUMMARY_SCRIPT_NAME = 'buildSummary.groovy'
    final String BUILD_CONFIG_SCRIPT_NAME = 'buildConfig.groovy'
    final String PIPELINE_UTILS_SCRIPT_NAME = 'pipelineUtils.groovy'
    final String EMAILER_SCRIPT_NAME = 'emailer.groovy'

    env.COMMIT_MESSAGE = sh(script: "cd ${h2o3Root} && git log -1 --pretty=%B", returnStdout: true).trim()
    env.BRANCH_NAME = scmEnv['GIT_BRANCH'].replaceAll('origin/', '')
    env.GIT_SHA = scmEnv['GIT_COMMIT']
    env.GIT_DATE = sh(script: "cd ${h2o3Root} && git show -s --format=%ci", returnStdout: true).trim()

    def final buildSummaryFactory = load("${h2o3Root}/scripts/jenkins/groovy/${BUILD_SUMMARY_SCRIPT_NAME}")
    def final buildConfigFactory = load("${h2o3Root}/scripts/jenkins/groovy/${BUILD_CONFIG_SCRIPT_NAME}")
    def final pipelineUtilsFactory = load("${h2o3Root}/scripts/jenkins/groovy/${PIPELINE_UTILS_SCRIPT_NAME}")
    def final emailerFactory = load("${h2o3Root}/scripts/jenkins/groovy/${EMAILER_SCRIPT_NAME}")

    def final buildinfoPath = "${h2o3Root}/h2o-dist/buildinfo.json"

    def final pipelineUtils = pipelineUtilsFactory()

    pipelineContext = new PipelineContext(
            buildConfigFactory(this, mode, env.COMMIT_MESSAGE, getChanges(h2o3Root), ignoreChanges,
                    pipelineUtils.readSupportedHadoopDistributions(this, buildinfoPath), gradleOpts,
                    pipelineUtils.readCurrentXGBVersion(this, h2o3Root)
            ),
            buildSummaryFactory(true),
            pipelineUtils,
            emailerFactory(),
    )
    pipelineContext.readPodTemplates(this)
    return pipelineContext
}

private List<String> getChanges(final String h2o3Root) {
    sh """
        cd ${h2o3Root}
        git fetch --no-tags --progress https://github.com/h2oai/h2o-3 +refs/heads/master:refs/remotes/origin/master
    """
    final String mergeBaseSHA = sh(script: "cd ${h2o3Root} && git merge-base HEAD origin/master", returnStdout: true).trim()
    return sh(script: "cd ${h2o3Root} && git diff --name-only ${mergeBaseSHA}", returnStdout: true).trim().tokenize('\n')
}

class PipelineContext {

    final static String POD_TIER_SMALL = 'small'
    final static String POD_TIER_MEDIUM = 'medium'
    final static String POD_TIER_LARGE = 'large'

    private final static List POD_TIERS = ['small', 'medium', 'large']
    private final static String DEFAULT_POD_CONTAINER = 'h2o-3-container'

    private final buildConfig
    private final buildSummary
    private final pipelineUtils
    private final emailer
    private final podTemplates = [:]
    private prepareBenchmarkDirStruct

    private PipelineContext(final buildConfig, final buildSummary, final pipelineUtils, final emailer) {
        this.buildConfig = buildConfig
        this.buildSummary = buildSummary
        this.pipelineUtils = pipelineUtils
        this.emailer = emailer
    }

    def getBuildConfig() {
        return buildConfig
    }

    def getBuildSummary() {
        return buildSummary
    }

    def getUtils() {
        return pipelineUtils
    }

    def getEmailer() {
        return emailer
    }

    def getPrepareBenchmarkDirStruct(final context, final mlBenchmarkRoot) {
        if (prepareBenchmarkDirStruct == null) {
            prepareBenchmarkDirStruct = context.load("${mlBenchmarkRoot}/jenkins/groovy/prepareBenchmarkDirStruct.groovy")
        }
        return prepareBenchmarkDirStruct
    }

    void readPodTemplates(final context) {
        for (def tier : POD_TIERS) {
            podTemplates[tier] = context.readFile("h2o-3/scripts/jenkins/yaml/h2o-3-${tier}.yaml")
        }

    }

    void insidePod(final context, final tier, final Closure body) {
        if (POD_TIERS.contains(tier)) {
            final def label = "h2o-3-pod-${tier}-${UUID.randomUUID().toString()}"
            context.podTemplate(label: label, name: "h2o-3-pod-${tier}", yaml: podTemplates[tier]) {
                context.node(label) {
                    context.container(DEFAULT_POD_CONTAINER) {
                        context.withCredentials([context.file(credentialsId: 'c096a055-bb45-4dac-ba5e-10e6e470f37e', variable: 'JUNIT_CORE_SITE_PATH'), [$class: 'AmazonWebServicesCredentialsBinding', accessKeyVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 'AWS S3 Credentials', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
                            body()
                        }
                    }
                }
            }
        } else {
            context.error "Pod of tier '${tier}' not yet supported!"
        }
    }
}

return this