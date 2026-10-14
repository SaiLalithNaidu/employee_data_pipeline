
pipeline {
    agent any

    environment {
        AWS_DEFAULT_REGION = 'ap-southeast-2'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Terraform Validate and Plan') {
            steps {
                withCredentials([
                    usernamePassword(
                        credentialsId: 'aws-terraform-credentials',
                        usernameVariable: 'AWS_ACCESS_KEY_ID',
                        passwordVariable: 'AWS_SECRET_ACCESS_KEY'
                    )
                ]) {
                    dir('terraform') {
                        sh 'terraform init'
                        sh 'terraform validate'
                        sh 'terraform plan'
                    }
                }
            }
        }
    }
}
