pipeline {

    agent any

    environment {
        AIRFLOW_URL = 'http://airflow-webserver:8080'
        AIRFLOW_USER = 'admin'
        AIRFLOW_PASSWORD = 'admin'

        MONGODB_URI = 'mongodb://mongodb:27017'

        DAG_ID = 'ecommerce_sales_pipeline'
    }

    stages {

        // ====================================================
        // 1. CHECKOUT
        // ====================================================

        stage('Checkout') {
            steps {
                echo 'Récupération du code source...'

                checkout scm
            }
        }


        // ====================================================
        // 2. INSTALL DEPENDENCIES
        // ====================================================

        stage('Install dependencies') {
            steps {
                echo 'Installation des dépendances Python...'

                sh '''
                    python3 -m venv .venv
                    . .venv/bin/activate

                    python3 -m pip install --upgrade pip
                    python3 -m pip install -r requirements.txt
                '''
            }
        }


        // ====================================================
        // 3. RUN TESTS
        // ====================================================

        stage('Run tests') {
            steps {
                echo 'Exécution des tests pytest...'

                sh '''
                    . .venv/bin/activate

                    python3 -m pytest -v tests/test_pipeline.py
                '''
            }
        }


        // ====================================================
        // 4. VALIDATE DAG
        // ====================================================

        stage('Validate DAG') {
            steps {
                echo 'Validation syntaxique du DAG Airflow...'

                sh '''
                    python3 -m py_compile \
                    dags/ecommerce_sales_pipeline.py
                '''
            }
        }


        // ====================================================
        // 5. DEPLOY DAG
        // ====================================================

        stage('Deploy DAG') {
            steps {
                echo 'Déploiement du DAG vers Airflow...'

                sh '''
                    cp dags/ecommerce_sales_pipeline.py \
                    /project/dags/ecommerce_sales_pipeline.py
                '''
            }
        }


        // ====================================================
        // 6. TRIGGER DAG
        // ====================================================

        stage('Trigger DAG') {
            steps {
                echo 'Déclenchement du DAG Airflow...'

                sh '''
                    curl --fail \
                    -u "$AIRFLOW_USER:$AIRFLOW_PASSWORD" \
                    -X POST \
                    "$AIRFLOW_URL/api/v1/dags/$DAG_ID/dagRuns" \
                    -H "Content-Type: application/json" \
                    -d '{"conf": {}}'
                '''
            }
        }


        // ====================================================
        // ATTENDRE LE TRAITEMENT AIRFLOW
        // ====================================================

        stage('Wait for Airflow') {
            steps {
                echo 'Attente de la fin du traitement Airflow...'

                sleep time: 90, unit: 'SECONDS'
            }
        }


        // ====================================================
        // 7. VERIFY MONGODB
        // ====================================================

        stage('Verify MongoDB') {
            steps {
                echo 'Vérification des données MongoDB...'

                sh '''
                    . .venv/bin/activate

                    MONGODB_URI="$MONGODB_URI" \
                    python3 scripts/check_mongodb.py
                '''
            }
        }
    }


    post {

        success {
            echo '========================================'
            echo 'PIPELINE JENKINS TERMINÉ AVEC SUCCÈS'
            echo '========================================'
        }

        failure {
            echo '========================================'
            echo 'ECHEC DU PIPELINE JENKINS'
            echo '========================================'
        }

        always {
            echo 'Fin du pipeline CI/CD.'
        }
    }
}
