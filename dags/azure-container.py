import datetime
import pendulum

from airflow.decorators import dag
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator




@dag(
    dag_id="deploy-container",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def DeployContainer():

    opr_run_container = AzureContainerInstancesOperator(
        task_id='run_container',
        ci_conn_id='azure_container_conn_id',
        registry_conn_id=None,
        resource_group='actuarial-apps-rg',
        name='acismithwilson',
        image='holmen1/smith-wilson-api',
        region='northeurope',
        cpu=1,
        memory_in_gb=1.5,
        ports=[{'protocol': 'TCP', 'port': 8000}],
        restart_policy='Always',
        ip_address={
            'type': 'Public',
            'ports': [{'protocol': 'TCP', 'port': 8000}],
            'dnsNameLabel': 'acismithwilson',
            'fqdn': 'acismithwilson.northeurope.azurecontainer.io'
        },
        environment_variables={'PORT': 8000}
    )

    opr_run_container

dag = DeployContainer()



