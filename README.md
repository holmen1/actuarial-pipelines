# AIRFLOW-PIPELINE

Get some data from a file which is hosted online and insert it into our local database  We also need to look at removing duplicate rows while inserting

https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html

* Make expected directories and set an expected environment variable  
mkdir -p ./logs ./plugins

* Initialize the database  
docker compose up airflow-init

* Start up all services  
docker compose up

After all services have started up, the web UI will be available at: http://localhost:8080. The default account has the username airflow and the password airflow

* Add a new record to the list of connections  
Connection Id: tutorial_pg_conn
Connection Type: postgres  
Host: postgres  
Schema: airflow  
Login: airflow  
Password: airflow  
Port: 5432



 * Cleaning-up  
docker compose down --volumes --remove-orphans 