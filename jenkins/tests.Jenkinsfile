pipeline {
    agent {
        label 'buildagent-matcher'
    }
    options {
        ansiColor('xterm')
        timeout(time: 70, unit: 'MINUTES')
        timestamps()
        disableConcurrentBuilds()
    }
    environment {
        SBT_HOME = tool name: 'sbt-1.2.6', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
        SBT_THREAD_NUMBER = "6"
        SBT_OPTS = '-Xmx12g -XX:ReservedCodeCacheSize=128m -XX:+CMSClassUnloadingEnabled'
        PATH = "${env.SBT_HOME}/bin:${env.PATH}"
        SCALATEST_EXCLUDE_TAGS = 'com.wavesplatform.it.tags.DexItKafkaRequired com.wavesplatform.it.tags.DexItExternalKafkaRequired com.wavesplatform.it.tags.DexMultipleVersions'
    }
    stages {
        stage('Cleanup') {
            steps {
                script {
                    if (!(BRANCH_NAME ==~ /(origin\/)?(DEX\-.*|master|merge\-.*|version\-.*|v?\d+\.\d+\.\d+(\.\d+)?)/)) {
                        currentBuild.result = 'ABORTED'
                        error("The branch '${BRANCH_NAME}' have an incorrect name. Allowed names: master, version-, DEX-")
                    }
                }
                sh 'git fetch --tags'
                sh "rm -rf allure-results || true"
                sh 'docker rmi `docker images --format "{{.Repository}}:{{.Tag}}" | grep "wavesplatform"` || true'
                sh 'docker system prune -f || true'
                sh 'find ~/.sbt/1.0/staging/*/waves -type d -name target | xargs -I{} rm -rf {}'
                sh 'find . -type d \\( -name "test-reports" -o -name "allure-results" -o -name "target" \\) | xargs -I{} rm -rf {}'
                sh 'sbt cleanAll'
            }
        }
        stage('Build & Check formatting & Run All Tests') {
            steps {
                sh 'sbt fmtCheck'
                sh 'sbt fullCheck'
            }
        }
        stage ('Push images') {
            steps {
                build job: 'Waves.Exchange/Matcher/Matcher Server - OS - Docker', propagate: false, wait: false, parameters: [
                  [$class: 'StringParameterValue', name: 'BRANCH', value: "${BRANCH_NAME}"]
                ]
            }
        }
    }
    post {
        always {
            sh 'tar zcf logs.tar.gz ./dex-it/target/logs* ./waves-integration-it/target/logs* || true'
            archiveArtifacts artifacts: 'logs.tar.gz', fingerprint: true
            junit '**/test-reports/*.xml'
            sh "mkdir allure-results || true"
            sh '''echo "TARGET_NODE=$(cat wavesNode.sbt | grep -Po '[0-9].[0-9].[0-9]+')" > ./allure-results/environment.properties'''
            allure results: [[path: 'allure-results']]
        }
        cleanup {
            cleanWs()
        }
    }
}
