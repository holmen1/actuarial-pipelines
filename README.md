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

* Add connections via cli 

```bash
 docker compose run airflow-cli connections add 'tutorial_pg_conn' \
    --conn-type 'postgres' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'airflow'
```

```bash
 docker compose run airflow-cli connections add 'smithwilson_api' \
    --conn-type 'http' \
    --conn-host 'sw-api' \
    --conn-port '8000'
```

* Stop all services  
docker compose down

 * Stop all services and clean-up  
docker compose down --volumes --remove-orphans 