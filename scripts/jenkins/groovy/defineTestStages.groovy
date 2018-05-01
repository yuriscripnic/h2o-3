def call(final pipelineContext) {

  def MODE_PR_CODE = 0
  def MODE_BENCHMARK_CODE = 1
  def MODE_HADOOP_CODE = 2
  def MODE_XGB_CODE = 3
  def MODE_COVERAGE_CODE = 4
  def MODE_SINGLE_TEST_CODE = 5
  def MODE_MASTER_CODE = 10
  def MODE_NIGHTLY_CODE = 20
  def MODES = [
    [name: 'MODE_PR', code: MODE_PR_CODE],
    [name: 'MODE_HADOOP', code: MODE_HADOOP_CODE],
    [name: 'MODE_XGB', code: MODE_XGB_CODE],
    [name: 'MODE_COVERAGE', code: MODE_COVERAGE_CODE],
    [name: 'MODE_SINGLE_TEST', code: MODE_SINGLE_TEST_CODE],
    [name: 'MODE_BENCHMARK', code: MODE_BENCHMARK_CODE],
    [name: 'MODE_MASTER', code: MODE_MASTER_CODE],
    [name: 'MODE_NIGHTLY', code: MODE_NIGHTLY_CODE]
  ]

  def modeCode = MODES.find{it['name'] == pipelineContext.getBuildConfig().getMode()}['code']

  // Job will execute PR_STAGES only if these are green.
  def SMOKE_STAGES = [
    [
      stageName: 'Py2.7 Smoke', target: 'test-py-smoke', pythonVersion: '2.7',timeoutValue: 8,
      component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'R3.4 Smoke', target: 'test-r-smoke', rVersion: '3.4.1',timeoutValue: 8,
      component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'PhantomJS Smoke', target: 'test-phantom-js-smoke',timeoutValue: 20,
      component: pipelineContext.getBuildConfig().COMPONENT_JS
    ],
    [
      stageName: 'Java8 Smoke', target: 'test-junit-smoke',timeoutValue: 20,
      component: pipelineContext.getBuildConfig().COMPONENT_JAVA, tier: pipelineContext.POD_TIER_MEDIUM
    ]
  ]

  // Stages executed after each push to PR branch.
  def PR_STAGES = [
    [
      stageName: 'Py2.7 Booklets', target: 'test-py-booklets', pythonVersion: '2.7',
      timeoutValue: 40, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'Py2.7 Demos', target: 'test-py-demos', pythonVersion: '2.7',
      timeoutValue: 30, component: pipelineContext.getBuildConfig().COMPONENT_PY, tier: pipelineContext.POD_TIER_MEDIUM
    ],
    [
      stageName: 'Py2.7 Init', target: 'test-py-init', pythonVersion: '2.7',
      timeoutValue: 5, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'Py2.7 Small', target: 'test-pyunit-small', pythonVersion: '2.7',
      timeoutValue: 90, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'Py3.5 Small', target: 'test-pyunit-small', pythonVersion: '3.5',
      timeoutValue: 90, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'Py3.6 Small', target: 'test-pyunit-small', pythonVersion: '3.6',
      timeoutValue: 90, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'R3.4 Init', target: 'test-r-init', rVersion: '3.4.1',
      timeoutValue: 5, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.4 Small', target: 'test-r-small', rVersion: '3.4.1',
      timeoutValue: 125, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.4 Small Client Mode', target: 'test-r-small-client-mode', rVersion: '3.4.1',
      timeoutValue: 155, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.4 CMD Check', target: 'test-r-cmd-check', rVersion: '3.4.1',
      timeoutValue: 15, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.4 CMD Check as CRAN', target: 'test-r-cmd-check-as-cran', rVersion: '3.4.1',
      timeoutValue: 10, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.4 Booklets', target: 'test-r-booklets', rVersion: '3.4.1',
      timeoutValue: 50, component: pipelineContext.getBuildConfig().COMPONENT_R, tier: pipelineContext.POD_TIER_MEDIUM
    ],
    [
      stageName: 'R3.4 Demos Small', target: 'test-r-demos-small', rVersion: '3.4.1',
      timeoutValue: 15, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'PhantomJS', target: 'test-phantom-js',
      timeoutValue: 75, component: pipelineContext.getBuildConfig().COMPONENT_JS
    ],
    [
      stageName: 'Py3.6 Medium-large', target: 'test-pyunit-medium-large', pythonVersion: '3.5',
      timeoutValue: 120, component: pipelineContext.getBuildConfig().COMPONENT_PY, tier: pipelineContext.POD_TIER_LARGE
    ],
    [
      stageName: 'R3.4 Medium-large', target: 'test-r-medium-large', rVersion: '3.4.1',
      timeoutValue: 80, component: pipelineContext.getBuildConfig().COMPONENT_R, tier: pipelineContext.POD_TIER_LARGE
    ],
    [
      stageName: 'R3.4 Demos Medium-large', target: 'test-r-demos-medium-large', rVersion: '3.4.1',
      timeoutValue: 140, component: pipelineContext.getBuildConfig().COMPONENT_R, tier: pipelineContext.POD_TIER_MEDIUM
    ],
    [
      stageName: 'INFO Check', target: 'test-info',
      timeoutValue: 10, component: pipelineContext.getBuildConfig().COMPONENT_ANY, additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_R]
    ],
    [
      stageName: 'Py3.6 Test Demos', target: 'test-demos', pythonVersion: '3.6',
      timeoutValue: 10, component: pipelineContext.getBuildConfig().COMPONENT_PY
    ],
    [
      stageName: 'Java 8 JUnit', target: 'test-junit-jenkins', pythonVersion: '2.7',
      timeoutValue: 90, component: pipelineContext.getBuildConfig().COMPONENT_JAVA,
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_PY], tier: pipelineContext.POD_TIER_MEDIUM
    ],
    [
      stageName: 'R3.4 Generate Docs', target: 'r-generate-docs-jenkins', archiveFiles: false,
      timeoutValue: 5, component: pipelineContext.getBuildConfig().COMPONENT_R, hasJUnit: false,
      archiveAdditionalFiles: ['h2o-r/h2o-package/**/*.html', 'r-generated-docs.zip']
    ]
  ]

  def BENCHMARK_STAGES = [
    [
      stageName: 'GBM Benchmark', executionScript: 'h2o-3/scripts/jenkins/groovy/benchmarkStage.groovy',
      timeoutValue: 120, target: 'benchmark', component: pipelineContext.getBuildConfig().COMPONENT_ANY,
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_R], image: pipelineContext.getBuildConfig().BENCHMARK_IMAGE,
      customData: [algorithm: 'gbm'], makefilePath: pipelineContext.getBuildConfig().BENCHMARK_MAKEFILE_PATH,
    ],
    [
      stageName: 'H2O XGB Benchmark', executionScript: 'h2o-3/scripts/jenkins/groovy/benchmarkStage.groovy',
      timeoutValue: 120, target: 'benchmark', component: pipelineContext.getBuildConfig().COMPONENT_ANY,
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_R], image: pipelineContext.getBuildConfig().BENCHMARK_IMAGE,
      customData: [algorithm: 'xgb'], makefilePath: pipelineContext.getBuildConfig().BENCHMARK_MAKEFILE_PATH,
    ],
    [
      stageName: 'Vanilla XGB Benchmark', executionScript: 'h2o-3/scripts/jenkins/groovy/benchmarkStage.groovy',
      timeoutValue: 120, target: 'benchmark-xgb-vanilla', component: pipelineContext.getBuildConfig().COMPONENT_ANY,
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_PY], image: pipelineContext.getBuildConfig().BENCHMARK_IMAGE,
      customData: [algorithm: 'xgb-vanilla'], makefilePath: pipelineContext.getBuildConfig().BENCHMARK_MAKEFILE_PATH,
    ]
  ]

  // Stages executed in addition to PR_STAGES after merge to master.
  def MASTER_STAGES = [
    [
      stageName: 'Py2.7 Medium-large', target: 'test-pyunit-medium-large', pythonVersion: '2.7',
      timeoutValue: 120, component: pipelineContext.getBuildConfig().COMPONENT_PY, tier: pipelineContext.POD_TIER_LARGE
    ],
    [
      stageName: 'Py3.5 Medium-large', target: 'test-pyunit-medium-large', pythonVersion: '3.5',
      timeoutValue: 120, component: pipelineContext.getBuildConfig().COMPONENT_PY, tier: pipelineContext.POD_TIER_LARGE
    ],
    [
      stageName: 'R3.4 Datatable', target: 'test-r-datatable', rVersion: '3.4.1',
      timeoutValue: 40, component: pipelineContext.getBuildConfig().COMPONENT_R, tier: pipelineContext.POD_TIER_MEDIUM
    ],
    [
      stageName: 'PhantomJS Small', target: 'test-phantom-js-small',
      timeoutValue: 75, component: pipelineContext.getBuildConfig().COMPONENT_JS
    ],
    [
      stageName: 'PhantomJS Medium', target: 'test-phantom-js-medium',
      timeoutValue: 75, component: pipelineContext.getBuildConfig().COMPONENT_JS, tier: pipelineContext.POD_TIER_LARGE
    ]
  ]
  MASTER_STAGES += BENCHMARK_STAGES

  // Stages executed in addition to MASTER_STAGES, used for nightly builds.
  def NIGHTLY_STAGES = [
    [
      stageName: 'R3.3 Medium-large', target: 'test-r-medium-large', rVersion: '3.3.3',
      timeoutValue: 70, component: pipelineContext.getBuildConfig().COMPONENT_R, tier: pipelineContext.POD_TIER_LARGE
    ],
    [
      stageName: 'R3.3 Small', target: 'test-r-small', rVersion: '3.3.3',
      timeoutValue: 125, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.3 Small Client Mode', target: 'test-r-small-client-mode', rVersion: '3.3.3',
      timeoutValue: 155, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.3 CMD Check', target: 'test-r-cmd-check', rVersion: '3.3.3',
      timeoutValue: 15, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_R
    ],
    [
      stageName: 'R3.3 CMD Check as CRAN', target: 'test-r-cmd-check-as-cran', rVersion: '3.3.3',
      timeoutValue: 10, hasJUnit: false, component: pipelineContext.getBuildConfig().COMPONENT_R
    ]
  ]

  def HADOOP_STAGES = []
  for (distribution in pipelineContext.getBuildConfig().getSupportedHadoopDistributions()) {
    HADOOP_STAGES += [
      stageName: "${distribution.name.toUpperCase()} ${distribution.version} Smoke", target: 'test-hadoop-smoke',
      timeoutValue: 25, component: pipelineContext.getBuildConfig().COMPONENT_ANY,
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_HADOOP, pipelineContext.getBuildConfig().COMPONENT_PY],
      customData: [
        distribution: distribution.name,
        version: distribution.version,
        ldapConfigPath: 'scripts/jenkins/ldap-conf.txt'
      ], pythonVersion: '2.7',
      executionScript: 'h2o-3/scripts/jenkins/groovy/hadoopStage.groovy'
    ]
  }

  def XGB_STAGES = []
  for (String osName: pipelineContext.getBuildConfig().getSupportedXGBEnvironments().keySet()) {
    final def xgbEnvs = pipelineContext.getBuildConfig().getSupportedXGBEnvironments()[osName]
    xgbEnvs.each {xgbEnv ->
      final def stageDefinition = [
        stageName: "XGB on ${xgbEnv.name}", target: "test-xgb-smoke-${xgbEnv.targetName}-jenkins", activateR: false,
        timeoutValue: 15, component: pipelineContext.getBuildConfig().COMPONENT_ANY,
        additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_JAVA], pythonVersion: '3.5',
        image: pipelineContext.getBuildConfig().getXGBImageForEnvironment(osName, xgbEnv),
      ]
      if (xgbEnv.targetName == pipelineContext.getBuildConfig().XGB_TARGET_GPU) {
        stageDefinition['executionScript'] = 'h2o-3/scripts/jenkins/groovy/xgbGPUStage.groovy'
      }
      XGB_STAGES += stageDefinition
    }
  }

  def COVERAGE_STAGES = [
    [
      stageName: 'h2o-algos Coverage', target: 'coverage-junit-algos', pythonVersion: '2.7', timeoutValue: 5 * 60,
      executionScript: 'h2o-3/scripts/jenkins/groovy/coverageStage.groovy',
      component: pipelineContext.getBuildConfig().COMPONENT_JAVA, archiveAdditionalFiles: ['build/reports/jacoco/*.exec'],
      additionalTestPackages: [pipelineContext.getBuildConfig().COMPONENT_PY]
    ]
  ]

  def SINGLE_TEST_STAGES = []
  if (modeCode == MODE_SINGLE_TEST_CODE) {
    if (params.testPath == null || params.testPath == '') {
      error 'Parameter testPath must be set.'
    }

    env.SINGLE_TEST_PATH = params.testPath.trim()
    env.SINGLE_TEST_XMX = params.singleTestXmx
    env.SINGLE_TEST_NUM_NODES = params.singleTestNumNodes

    def target
    def additionalTestPackage
    switch (params.testComponent) {
      case 'Python':
        target = 'test-py-single-test'
        additionalTestPackage = pipelineContext.getBuildConfig().COMPONENT_PY
        break
      case 'R':
        target = 'test-r-single-test'
        additionalTestPackage = pipelineContext.getBuildConfig().COMPONENT_R
        break
      default:
        error "Test Component ${params.testComponent} not supported"
    }
    def numRunsNum = -1
    try {
      numRunsNum = Integer.parseInt(params.singleTestNumRuns)
    } catch (NumberFormatException e) {
      error "singleTestNumRuns must be a valid number"
    }
    numRunsNum.times {
      SINGLE_TEST_STAGES += [
        stageName: "Test ${params.testPath.split('/').last()} #${(it + 1)}", target: target, timeoutValue: 25,
        component: pipelineContext.getBuildConfig().COMPONENT_ANY, additionalTestPackages: [additionalTestPackage],
        pythonVersion: params.singleTestPyVersion, rVersion: params.singleTestRVersion
      ]
    }
  }

  if (modeCode == MODE_BENCHMARK_CODE) {
    executeInParallel(BENCHMARK_STAGES, pipelineContext)
  } else if (modeCode == MODE_HADOOP_CODE) {
    executeInParallel(HADOOP_STAGES, pipelineContext)
  } else if (modeCode == MODE_XGB_CODE) {
    executeInParallel(XGB_STAGES, pipelineContext)
  } else if (modeCode == MODE_COVERAGE_CODE) {
    executeInParallel(COVERAGE_STAGES, pipelineContext)
  } else if (modeCode == MODE_SINGLE_TEST_CODE) {
    executeInParallel(SINGLE_TEST_STAGES, pipelineContext)
  } else {
    executeInParallel(SMOKE_STAGES, pipelineContext)
    def jobs = PR_STAGES
    if (modeCode >= MODE_MASTER_CODE) {
      jobs += MASTER_STAGES
    }
    if (modeCode >= MODE_NIGHTLY_CODE) {
      jobs += NIGHTLY_STAGES
    }
    executeInParallel(jobs, pipelineContext)
  }
}

private void executeInParallel(final jobs, final pipelineContext) {
  parallel(jobs.collectEntries { c ->
    [
      c['stageName'], {
        invokeStage(pipelineContext) {
          stageName = c['stageName']
          target = c['target']
          pythonVersion = c['pythonVersion']
          rVersion = c['rVersion']
          timeoutValue = c['timeoutValue']
          hasJUnit = c['hasJUnit']
          component = c['component']
          additionalTestPackages = c['additionalTestPackages']
          tier = c['tier']
          executionScript = c['executionScript']
          image = c['image']
          customData = c['customData']
          makefilePath = c['makefilePath']
          archiveAdditionalFiles = c['archiveAdditionalFiles']
          excludeAdditionalFiles = c['excludeAdditionalFiles']
          archiveFiles = c['archiveFiles']
          activatePythonEnv = c['activatePythonEnv']
          activateR = c['activateR']
        }
      }
    ]
  })
}

private void invokeStage(final pipelineContext, final body) {

  final String DEFAULT_PYTHON = '3.5'
  final String DEFAULT_R = '3.4.1'
  final int DEFAULT_TIMEOUT = 60
  final String DEFAULT_EXECUTION_SCRIPT = 'h2o-3/scripts/jenkins/groovy/defaultStage.groovy'
  final int HEALTH_CHECK_RETRIES = 5

  def config = [:]

  body.resolveStrategy = Closure.DELEGATE_FIRST
  body.delegate = config
  body()

  config.stageDir = pipelineContext.getUtils().stageNameToDirName(config.stageName)

  config.pythonVersion = config.pythonVersion ?: DEFAULT_PYTHON
  config.rVersion = config.rVersion ?: DEFAULT_R
  config.timeoutValue = config.timeoutValue ?: DEFAULT_TIMEOUT
  if (config.hasJUnit == null) {
    config.hasJUnit = true
  }
  config.additionalTestPackages = config.additionalTestPackages ?: []
  config.tier = config.tier ?: pipelineContext.POD_TIER_SMALL
  config.executionScript = config.executionScript ?: DEFAULT_EXECUTION_SCRIPT
  config.image = config.image ?: pipelineContext.getBuildConfig().DEFAULT_IMAGE
  config.makefilePath = config.makefilePath ?: pipelineContext.getBuildConfig().MAKEFILE_PATH
  config.archiveAdditionalFiles = config.archiveAdditionalFiles ?: []
  config.excludeAdditionalFiles = config.excludeAdditionalFiles ?: []
  if (config.archiveFiles == null) {
    config.archiveFiles = true
  }

  if (pipelineContext.getBuildConfig().componentChanged(config.component)) {
    pipelineContext.getBuildSummary().addStageSummary(this, config.stageName, config.stageDir)
    pipelineContext.insidePod(this, config.tier) {
      if (params.executeFailedOnly && pipelineContext.getUtils().wasStageSuccessful(this, config.stageName)) {
        echo "###### Stage was successful in previous build ######"
        pipelineContext.getBuildSummary().setStageDetails(this, config.stageName, 'Skipped', 'N/A')
        pipelineContext.getBuildSummary().markStageSuccessful(this, config.stageName)
      } else {
        withCustomCommitStates(scm, 'h2o-ops-personal-auth-token', "${pipelineContext.getBuildConfig().getGitHubCommitStateContext(config.stageName)}") {
          try {
            stage(config.stageName) {
              echo "###### Unstash scripts. ######"
              pipelineContext.getUtils().unstashScripts(this)

              pipelineContext.getBuildSummary().setStageDetails(this, config.stageName, env.NODE_NAME, env.WORKSPACE)

              sh "rm -rf ${config.stageDir}"

              def script = load(config.executionScript)
              script(pipelineContext, config)
              pipelineContext.getBuildSummary().markStageSuccessful(this, config.stageName)
            }
          } catch (Exception e) {
            pipelineContext.getBuildSummary().markStageFailed(this, config.stageName)
            throw e
          }
        }
      }
    }
  } else {
    echo "###### Changes for ${config.component} NOT detected, skipping ${config.stageName}. ######"
  }
}

return this
