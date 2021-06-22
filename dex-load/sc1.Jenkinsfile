pipeline {
    agent {
        label 'buildagent-matcher-load-test'
    }
    options {
        ansiColor('xterm')
        disableConcurrentBuilds()
        timestamps()
    }
    parameters {
        string(name: 'SEED', defaultValue: 'test-seed', description: 'Seed prefix of generated accounts')
        string(name: 'AN', defaultValue: '6000', description: 'Count of generated accounts')
        string(name: 'RC', defaultValue: '216060', description: 'Count of requests')
        string(name: 'RT', defaultValue: '6', description: 'Generation type')
        string(name: 'AS', defaultValue: 'D', description: 'Chain ID')
        string(name: 'LABEL', defaultValue: '', description: 'Label')
    }
    environment {
        SBT_HOME = tool name: 'sbt-1.2.6', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'
        SBT_THREAD_NUMBER = "6"
        SBT_OPTS = '-Xmx10g -XX:ReservedCodeCacheSize=128m -XX:+CMSClassUnloadingEnabled'
        PATH = "${env.SBT_HOME}/bin:${env.PATH}"
        SEED = "${SEED}"
        AN = "${AN}"
        RC = "${RC}"
        RT = "${RT}"
        AS = "${AS}"
        NODE = "${NODE}"
        MATCHER = "${MATCHER}"
        AIM = "${AIM}"
    }
    stages {
        stage('Cleanup') {
            steps {
                sh 'rm -rf ./dex-load/*.txt'
                sh 'git fetch --tags'
                sh 'find ~/.sbt/1.0/staging/*/waves -type d -name target | xargs -I{} rm -rf {}'
                sh 'find . -type d -name target | xargs -I{} rm -rf {}'
                sh 'sbt "cleanAll"'
            }
        }
        stage('Generate ammo file') {
            steps {
                sh 'sbt "dex-load/generateAmmo"'
            }
        }
        stage('Performance Test') {
            steps {
                sshagent (credentials: ['buildagent-matcher']) {
                     sh "ssh -o StrictHostKeyChecking=no -l buildagent-matcher ${LOADGEN} hostname"
                     sh "scp ./dex-load/requests-*.txt buildagent-matcher@${LOADGEN}:/home/buildagent-matcher"
                     sh "scp ./dex-load/pairs.txt buildagent-matcher@${LOADGEN}:/home/buildagent-matcher"
                     sh "scp ./dex-load/src/main/resources/runLoadTest.sh buildagent-matcher@${LOADGEN}:/home/buildagent-matcher"
                     sh "ssh -q buildagent-matcher@${LOADGEN} sudo sh runLoadTest.sh SC1_${params.LABEL}"
                }
            }
        }
    }
    post {
        always {
            sshagent (credentials: ['buildagent-matcher']) {
                script {
                    OVERLOAD = sh(script:"ssh -q buildagent-matcher@${LOADGEN} ls /home/yatank/loadtest/logs/lunapark", returnStdout: true)
                    GRAFANA = sh(script: '''
                                            echo "https://${GRAFANA_URL}/d/WsyjIiHiz/system-metrics?orgId=5&var-hostname=${MATCHER_URL}&from=$(date -d '- 10 minutes' +'%s')000&to=$(date -d '+ 5 minutes' +'%s')000"
                                          ''', returnStdout: true)
                    if(params.LABEL != '') {
                        currentBuild.displayName = "${params.LABEL}"
                    }
                    currentBuild.description = "<a href='https://overload.yandex.net/${OVERLOAD}'>Yandex</a> <br/> <a href='${GRAFANA}'>Grafana</a>"
                }
            }
        }
        cleanup {
            cleanWs()
        }
    }
}
