pipeline {

    agent any
    tools {
        maven "maven 3.6.3"
    }

    options {
        buildDiscarder logRotator(
                    daysToKeepStr: '16',
                    numToKeepStr: '7'
            )
    }

    stages {

        stage('Cleanup Workspace') {
            steps {
                cleanWs()
                sh """
                echo "Cleaned Up Workspace For Project"
                """
            }
        }


        stage('Checkout SCM') {
            steps {
                echo 'Pulling...' + env.BRANCH_NAME
                checkout scm
            }
        }


        stage('build') {
            steps {
                script {
                   sh """
                   java -version
                   mvn --version
                   mvn clean package -DskipTests -U
                   echo "Executing stage -- build --"
                   """
                }
            }
        }

        stage('acceptance') {
            steps {
                script {
                  sh """
                  #mvn verify
                  echo "Executing stage -- acceptance --"
                  """
                }
            }
        }

        stage('develop-release') {
            when {
                branch 'develop'
            }
            steps {
                script {
                   def releaseVersion  = "latest"
                   sh """
                   echo ${releaseVersion}
                   echo "Executing stage -- nexus --"
                   mvn deploy -X
                   echo "Executing stage -- sonarqube --"
                   """
                }
            }
        }

        stage('master-release') {
            when {
                branch 'master'
            }
            steps {
                withCredentials([[$class: 'UsernamePasswordMultiBinding',
                	credentialsId: 'github',
                	usernameVariable: 'GIT_USERNAME',
                	passwordVariable: 'GIT_PASSWORD'
                ]])
                script {
                   developmentVersion = readMavenPom().getVersion()
                   releaseVersion = developmentVersion.replace('-SNAPSHOT', '')
                   sh """
                   echo ${releaseVersion}
                   echo "Executing release"
                   echo "username:- $GIT_USERNAME"
                   mvn -B release:clean
                   mvn -B release:prepare release:perform -Dresume=false -Darguments=\"-DskipTests\" -Dusername=$GIT_USERNAME -Dpassword=$GIT_PASSWORD -Dtag=v${releaseVersion}
                   #mvn deploy  -Drevision=v${releaseVersion}
                   """
                }
            }
        }

        stage("docker") {
            steps {
                script {
                    if(BRANCH_NAME == 'jenkins-test') {
                        def releaseVersion  = "latest"
                        echo "${releaseVersion}"
                    }
                    else if(BRANCH_NAME == 'master') {
                        developmentVersion = readMavenPom().getVersion()
                        releaseVersion = developmentVersion.replace('-SNAPSHOT', '')
                        echo "${releaseVersion}"
                    }
                }
           }
       }
 
        stage('Trigger Branch Build') {
        steps {
                    build job: "alice-logger", wait: false
        }
    }



    }
}

