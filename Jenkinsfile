#!groovy

// Global scope required for multi-stage persistence
def artifactoryStr = 'art-p-01'
artServer = Artifactory.server "${artifactoryStr}"
buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.1'
def artifactVersion

def pushToPyPiArtifactoryRepo_temp(String projectName, String version, String sourceDistLocation = 'python/dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDistLocation} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}


pipeline {
    libraries {
        lib('jenkins-pipeline-shared@feature/dap-ci-scripts')
    }

    environment {
        ARTIFACTORY_CREDS       = 's_jenkins_epds'
        ARTIFACTORY_PYPI_REPO   = 'LR_EPDS_pypi'
        PROJECT_NAME            = 'cprices'
        MASTER_BRANCH           = 'master'
        // If test coverage falls below this number the build will fail
        MIN_COVERAGE_PC         = '0'

    }

    options {
        skipDefaultCheckout true
    }

    agent any

    stages {
        stage('Checkout') {
            agent { label 'download.jenkins.slave' }
            steps {
                onStage()
                colourText('info', "Checking out code from source control.")

                checkout scm
                script {
                    buildInfo.name = "${PROJECT_NAME}"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                colourText('info', "BuildInfo: ${buildInfo.name}-${buildInfo.number}")
                stash name: 'Checkout', useDefaultExcludes: false
            }
        }

        stage('Unit Test and coverage') {
            agent { label "test.${agentPython3Version}" }
            steps {
                onStage()
                colourText('info', "Running unit tests and code coverage.")
                unstash name: 'Checkout'

                sh 'pip3 install pypandoc'
                sh 'pip3 install pyspark==2.3.0'
                sh 'pip3 install -r requirements.txt'
                sh 'pip3 install -e .'
                // Running coverage first runs the tests
                sh 'coverage run --branch --source=./${PROJECT_NAME} -m unittest discover -s ./tests'
                sh 'coverage xml -o python_coverage.xml && coverage report -m --fail-under=${MIN_COVERAGE_PC}'

                cobertura autoUpdateHealth: false,
                        autoUpdateStability: false,
                        coberturaReportFile: 'python_coverage.xml',
                        conditionalCoverageTargets: '70, 0, 0',
                        failUnhealthy: false,
                        failUnstable: false,
                        lineCoverageTargets: '80, 0, 0',
                        maxNumberOfBuilds: 0,
                        methodCoverageTargets: '80, 0, 0',
                        onlyStable: false,
                        zoomCoverageChart: false

            }
        }

        stage('Build and publish Python Package') {
            when {
                branch MASTER_BRANCH
                beforeAgent true
            }
            agent { label "test.${agentPython3Version}" }
            steps {
                onStage()
                colourText('info', "Building Python package.")
                unstash name: 'Checkout'

                sh 'pip3 install wheel==0.29.0'
                sh 'python3 setup.py build bdist_wheel'

                script {
                    pushToPyPiArtifactoryRepo_temp("${buildInfo.name}", "", "dist/*")
                }
            }
            post {
                success { onSuccess() }
                failure { onFailure() }
            }
        }
    }
}
