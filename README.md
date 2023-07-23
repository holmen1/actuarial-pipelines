# ACTUARIAL-PIPELINES

Running Airflow locally in Docker modifiying  
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


### Setup
Make expected directories and set an expected environment variable
```bash 
mkdir -p ./logs ./plugins
```

Initialize the database
```bash
docker compose up airflow-init
```

Start up all services
```bash
docker compose up
```

After all services have started up, the web UI will be available at: http://localhost:8080. The default account has the username airflow and the password airflow

Add connections via cli 
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
 docker compose run airflow-cli connections add 'fs_default' \
    --conn-type 'file'
```

When running locally, smith-wilson-par are mounted into the container
```bash
 docker compose run airflow-cli connections add 'smithwilson_api' \
    --conn-type 'http' \
    --conn-host 'sw-api' \
    --conn-port '8000'
```


Create a container
```bash
az container create \
  --name 'aciliabilities' \
  --resource-group 'actuarial-apps-rg' \
  --image 'holmen1/estimate-liabilities-api' \
  --ports 80 \
  --dns-name-label 'aciliabilities'
```

```bash
 docker compose run airflow-cli connections add 'liabilities_api' \
    --conn-type 'http' \
    --conn-host 'aciliabilities.northeurope.azurecontainer.io'
```

## DAGs
### process-rates
This DAG creates 3 tables in the database. Then waits for swap.csv to be available on the web. Once it is available, it downloads the file and inserts the data into the database. It then removes any duplicate rows from the database. Then it pings the API, when up posts the data to the API and inserts the response into the database.
Finally it deletes the swap.csv file from the web.



### deploy-container
#### Setup Azure
Create a Service Principal in Azure Active Directory:  
App registrations -> New registration: "airflow-app"  
select "Accounts in this organizational directory only"  
In the app registration details page, note down the "Application (client) ID" and the "Directory (tenant) ID"  
Click on the "Certificates & secrets" button and then click on the "New client secret" button  
Note down the value of the client secret! 

Create a resource group  
```bash
az group create --name "actuarial-apps-rg" --location "northeurope"
```

Add the service principal to the resource group in with Azure UI:  
Access control (IAM) -> Add -> Add role assignment -> Role: Contributor -> Select: "airflow-app" -> Save  

Airflow UI -> Admin -> Connections -> Create  
Connection Id: azure_container_conn_id  
Connection Type: Azure Container Instance  
Login: Application (client) ID e586d25a-d60d-409c-8e2a-b9b027fd83f0  
Password: Client secret kRU8Q~i4x1U.cBpoIRWCOHXH-AO5pfSsYAsDLbCr  
Extra:{
  "tenantId": "a21218f6-3dd1-4b7c-8c33-cfbaec966166",
  "subscriptionId": "9eb54f6a-d591-4438-8472-6a5cdff53b85"
}  


## Close down and remove all resources

Stop all services
```bash
docker compose down
```

 Stop all services and clean-up
 ```bash
docker compose down --volumes --remove-orphans
