pipeline {

    agent any
    tools {
        maven "maven 3.6.3"
        dockerTool "docker"
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
                   echo 'Executing stage -- build --'
                   mvn clean package -DskipTests -U
                   """
                }
            }
        }

        stage('acceptance') {
            steps {
                script {
                  sh """
                  echo "Executing stage -- acceptance --"
                  mvn verify
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
                   echo "Executing stage -- nexus --"
                   mvn deploy -X
                   """
                }
            }
        }

        stage('hotfix-release') {
            when {
                branch 'hotfix*'
            }
            steps {
                script {
                   def releaseVersion  = "latest"
                   sh """
                   echo "Executing stage -- nexus --"
                   mvn deploy -X
                   """
                }
            }
        }

        stage('master-release') {
            when {
                branch 'master'
            }
            steps {
  		withCredentials([usernamePassword(credentialsId: 'github', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                        script {
                            developmentVersion = readMavenPom().getVersion()
                            releaseVersion = developmentVersion.replace('-SNAPSHOT', '')
                            sh """
                            echo ${releaseVersion}
                            echo "Executing release"
                            mvn -B release:clean
                            mvn -B release:prepare release:perform -Dresume=false -Darguments=\"-DskipTests\" -Dusername=$USERNAME -Dpassword=$PASSWORD -Dtag=v${releaseVersion}
                            """
                        }
                }
            }
        }

        stage("docker") {
            steps {
                withCredentials([usernamePassword(credentialsId: 'docker', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                	script {
                            docker login -u $USERNAME -p $PASSWORD
                    	    if(BRANCH_NAME == 'develop') {
                        	def releaseVersion  = "latest"
                        	echo "${releaseVersion}
                        
                    	    }
                    	    else if(BRANCH_NAME == 'master') {
                        	developmentVersion = readMavenPom().getVersion()
                        	releaseVersion = developmentVersion.replace('-SNAPSHOT', '')
                        	echo "${releaseVersion}"
			    }
                                docker build -t auchoudhari/aef:${releaseVersion} .
				docker push auchoudhari/aef:${releaseVersion}
     
				sleep 5
				docker rmi auchoudhari/aef:${releaseVersion}
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

