pipeline {
    agent {
        label 'buildagent-matcher'
    }
    options {
        ansiColor('xterm')
    }
    parameters {
        string(name: 'SBT_THREAD_NUMBER', defaultValue: '6', description: '')
    }
    environment {
        SBT_HOME = tool name: 'sbt-1.2.6', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
        SBT_THREAD_NUMBER = "${SBT_THREAD_NUMBER}"
        SBT_OPTS = '-Xmx10g -XX:ReservedCodeCacheSize=128m -XX:+CMSClassUnloadingEnabled'
        PATH = "${env.SBT_HOME}/bin:${env.PATH}"
        SCALATEST_INCLUDE_TAGS = 'com.wavesplatform.it.tags.DexMultipleVersions'
        KAFKA_SERVER = "${KAFKA_SERVER}"
        OTHER_DEX_IMAGE = "${OTHER_DEX_IMAGE}"
        OTHER_NODE_IMAGE = "${OTHER_NODE_IMAGE}"
    }
    stages {
        stage('Cleanup') {
            steps {
                script {
                    currentBuild.displayName = "${params.BRANCH}"
                }
                sh 'git fetch --tags'
                sh 'docker rmi `docker images --format "{{.Repository}}:{{.Tag}}" | grep "wavesplatform"` || true'
                sh 'docker system prune -f || true'
                sh 'find ~/.sbt/1.0/staging/*/waves -type d -name target | xargs -I{} rm -rf {}'
                sh 'find . -type d -name target | xargs -I{} rm -rf {}'
                sh 'sbt "cleanAll"'
            }
        }
        stage('Build Docker') {
            steps {
                sh 'sbt dex-it/docker'
            }
        }
        stage ('Run Integration Tests with multiple versions') {
            steps {
                sh 'sbt dex-it/test'
            }
        }
    }
    post {
        always {
            sh 'tar zcf logs.tar.gz ./dex-it/target/logs* ./waves-integration-it/target/logs* || true'
            archiveArtifacts artifacts: 'logs.tar.gz', fingerprint: true
            junit '**/test-reports/*.xml'
            sh "mkdir allure-results || true"
            sh "echo 'KAFKA_SERVER=${KAFKA_SERVER}\r\nOTHER_DEX_IMAGE=${OTHER_DEX_IMAGE}\r\nOTHER_NODE_IMAGE=${OTHER_NODE_IMAGE}' > allure-results/environment.properties"
            allure results: [[path: 'allure-results']]
        }
        cleanup {
            cleanWs()
        }
    }
}