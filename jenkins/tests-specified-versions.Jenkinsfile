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
    parameters {
        string(name: 'LABEL', defaultValue: '', description: '')
        booleanParam(name: 'JENKINS_SHOULD_BUILD_DOCKER_IMAGE', defaultValue: true, description: '')
    }
    environment {
        SBT_HOME = tool name: 'sbt-1.2.6', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
        SBT_THREAD_NUMBER = "6"
        SBT_OPTS = '-Xmx10g -XX:ReservedCodeCacheSize=128m -XX:+CMSClassUnloadingEnabled'
        PATH = "${env.SBT_HOME}/bin:${env.PATH}"
        SCALATEST_EXCLUDE_TAGS = 'com.wavesplatform.it.tags.DexItKafkaRequired com.wavesplatform.it.tags.DexItExternalKafkaRequired com.wavesplatform.it.tags.DexMultipleVersions'
        KAFKA_SERVER = "${KAFKA_SERVER}"
        DEX_IMAGE = "${REGISTRY}/waves/dex/${DEX_IMAGE}"
        NODE_IMAGE = "${REGISTRY}/waves/dex/${NODE_IMAGE}"
        JENKINS_SHOULD_BUILD_DOCKER_IMAGE = "${JENKINS_SHOULD_BUILD_DOCKER_IMAGE}"
    }
    stages {
        stage('Cleanup') {
            steps {
                script {
                    currentBuild.displayName = "${params.LABEL}"
                    currentBuild.description = "<a href='https://${REGISTRY}/harbor/projects/11/repositories/waves%2Fdex%2Fdex-it/tags/${params.DEX_IMAGE.replaceAll('dex-it:','')}'>Dex image</a> <br/> <a href='https://${REGISTRY}/harbor/projects/11/repositories/waves%2Fdex%2Fwaves-integration-it/tags/${params.NODE_IMAGE.replaceAll('waves-integration-it:','')}'>Node image</a>"
                }
                sh 'git fetch --tags'
                sh 'docker rmi `docker images --format "{{.Repository}}:{{.Tag}}" | grep "wavesplatform"` || true'
                sh 'docker system prune -f || true'
                sh 'find ~/.sbt/1.0/staging/*/waves -type d -name target | xargs -I{} rm -rf {}'
                sh 'find . -type d \\( -name "test-reports" -o -name "allure-results" -o -name "target" \\) | xargs -I{} rm -rf {}'
                sh 'sbt "cleanAll"'
            }
        }
        stage('Build Docker') {
            when {
                expression { JENKINS_SHOULD_BUILD_DOCKER_IMAGE == "true" }
            }
            steps {
                sh 'sbt dex-it/docker'
            }
        }
        stage ('Run Integration Tests with specified versions') {
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
            sh "echo 'KAFKA_SERVER=${KAFKA_SERVER}\r\nDEX_IMAGE=${DEX_IMAGE}\r\nNODE_IMAGE=${NODE_IMAGE}' > allure-results/environment.properties"
            allure results: [[path: 'allure-results']]
        }
        cleanup {
            cleanWs()
        }
    }
}
