import datetime
import json
import pendulum
import os
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
from airflow.models import XCom


def get_request_data():
    postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(\"ValueDate\") FROM holmen.contract")
    max_value_date = cur.fetchone()[0]
    cur.execute("SELECT MAX(\"ContractNo\") FROM holmen.contract WHERE \"ValueDate\" = %s", (max_value_date,))
    max_contract = cur.fetchone()[0]

    cur.execute("SELECT \"ContractNo\", \"ValueDate\", \"BirthDate\", \"Sex\", \"VestingAge\", \"Guarantee\", \"PayPeriod\", \"Table\"  FROM holmen.contract WHERE \"ContractNo\" = %s AND \"ValueDate\" = %s", (max_contract, max_value_date))
    row = cur.fetchone()

    return {
        "value_date": max_value_date.strftime("%Y-%m-%d"),
        "data": {
            "contractNo": row[0],
            "valueDate": row[1].strftime("%Y-%m-%d"),
            "birthDate": row[2].strftime("%Y-%m-%d"),
            "sex": row[3],
            "z": int(row[4]),
            "guarantee": float(row[5]),
            "payPeriod": row[6],
            "table": row[7]
        }
    }

@dag(
    dag_id="process-contracts",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessContracts():
    poke_swaps = FileSensor(
        task_id="poke_swaps",
        filepath="/opt/airflow/dags/files/swaps.csv",
        poke_interval=10,
        timeout=300,
    )

    create_holmen_schema = PostgresOperator(
        task_id="create_holmen1_schema",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""CREATE SCHEMA IF NOT EXISTS holmen;""",
    )

    #curl -X 'POST' 'acilifeholmen1.northeurope.azurecontainer.io/cashflows' -H 'accept: */*' -H 'Content-Type: application/json' -d 
    #'{"contractNo":42341,"valueDate":"2023-04-30","birthDate":"1973-04-30",
    #"sex":"F","z":65,"guarantee":1000,"payPeriod":5,"table":"APG"}' 

    create_contract_table = PostgresOperator(
        task_id="create_contract_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.contract (
                "ContractNo" INTEGER,
                "ValueDate" DATE,
                "BirthDate" DATE,
                "Sex" TEXT,
                "VestingAge" NUMERIC,
                "Guarantee" NUMERIC,
                "PayPeriod" INTEGER,
                "Table" TEXT,
                PRIMARY KEY ("ContractNo", "ValueDate")
            );""",
    )

    create_contract_temp_table = PostgresOperator(
        task_id="create_contract_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            DROP TABLE IF EXISTS holmen.contract_temp;
            CREATE TABLE holmen.contract_temp (
                "ContractNo" INTEGER,
                "ValueDate" DATE,
                "BirthDate" DATE,
                "Sex" TEXT,
                "VestingAge" NUMERIC,
                "Guarantee" NUMERIC,
                "PayPeriod" INTEGER,
                "Table" TEXT,
                PRIMARY KEY ("ContractNo", "ValueDate")
            );""",
    )

    create_cashflow_table = PostgresOperator(
        task_id="create_cashflow_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.cashflow (
                "ContractNo" INTEGER,
                "ValueDate" DATE,
                "Month" INTEGER,
                "Benefit" NUMERIC
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/dags/files/contracts.csv"

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY holmen.contract_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        sql = r"""
            INSERT INTO holmen.contract
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM holmen.contract_temp
            ) t
            ON CONFLICT ON CONSTRAINT contract_pkey DO UPDATE
            SET "BirthDate" = excluded."BirthDate",
                "Sex" = excluded."Sex",
                "VestingAge" = excluded."VestingAge",
                "Guarantee" = excluded."Guarantee",
                "PayPeriod" = excluded."PayPeriod",
                "Table" = excluded."Table";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    task_http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="liabilities_api",
        endpoint="/",
    )

    @task
    def get_cashflows(**kwargs):
        http_hook = HttpHook(method="POST", http_conn_id="liabilities_api")
        request_data = get_request_data()
        ti = kwargs['ti']
        ti.xcom_push(key='request_parameters', value=request_data)
        request = request_data['data']
        response = http_hook.run(
            endpoint="/cashflows",
            data=json.dumps(request),
            headers={"Content-Type": "application/json"},
        )
        # insert response into holmen.cashflow
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        month = 0
        for benefit in response.json():
            cur.execute("INSERT INTO holmen.cashflow VALUES (%s, %s, %s, %s)", (request['contractNo'], request['valueDate'], month, benefit))
            month += 1
        conn.commit()


    create_holmen_schema >> [create_contract_table, create_contract_temp_table] >> \
    get_data() >> merge_data() >> \
    task_http_sensor_check >> get_cashflows()


dag = ProcessContracts()
