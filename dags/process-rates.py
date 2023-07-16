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
    cur.execute("SELECT MAX(\"ValueDate\") FROM holmen.swap")
    max_value_date = cur.fetchone()[0]

    cur.execute("SELECT \"Tenor\", \"Value\" FROM holmen.swap WHERE \"ValueDate\" = %s", (max_value_date,))
    rows = cur.fetchall()
    tenors = [row[0] for row in rows]
    values = [float(row[1]) for row in rows]

    return {
        "value_date": max_value_date.strftime("%Y-%m-%d"),
        "data": {
            "par_rates": values,
            "par_maturities": tenors,
            "projection": [1, 151],
            "ufr": 0.0345,
            "convergence_maturity": 20,
            "tol": 1E-4
        }
    }


@dag(
    dag_id="process-rates",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProjectSwaps():
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

    create_swap_table = PostgresOperator(
        task_id="create_swap_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.swap (
                "ValueDate" DATE,
                "Currency" TEXT,
                "Tenor" INTEGER,
                "SettlementFreq" INTEGER,
                "Value" NUMERIC,
                PRIMARY KEY ("ValueDate", "Currency", "Tenor")
            );""",
    )

    create_swap_temp_table = PostgresOperator(
        task_id="create_swap_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            DROP TABLE IF EXISTS holmen.swap_temp;
            CREATE TABLE holmen.swap_temp (
                "ValueDate" DATE,
                "Currency" TEXT,
                "Tenor" INTEGER,
                "SettlementFreq" INTEGER,
                "Value" NUMERIC,
                PRIMARY KEY ("ValueDate", "Currency", "Tenor")
            );""",
    )

    create_riskfreerate_data_table = PostgresOperator(
        task_id="create_riskfree_rate_data_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.rate_data (
                "ProjectionId" INTEGER,
                "Maturity" NUMERIC,
                "SpotValue" NUMERIC,
                PRIMARY KEY ("ProjectionId", "Maturity")
            );""",
    )

    create_riskfreerate_table = PostgresOperator(
        task_id="create_riskfree_rate_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.rate (
                "ProjectionId" INTEGER PRIMARY KEY,
                "ValueDate" DATE,
                "Alpha" NUMERIC,
                "RequestParameters" TEXT,
                "LastUpdated" DATE
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/dags/files/swaps.csv"

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY holmen.swap_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data():
        sql = r"""
            INSERT INTO holmen.swap
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM holmen.swap_temp
            ) t
            ON CONFLICT ON CONSTRAINT swap_pkey DO UPDATE
            SET "SettlementFreq" = excluded."SettlementFreq",
                "Value" = excluded."Value";
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
        http_conn_id="smithwilson_api",
        endpoint="/",
    )

    @task
    def get_projection(**kwargs):
        http_hook = HttpHook(method="POST", http_conn_id="smithwilson_api")
        request_data = get_request_data()
        request = request_data['data']
        response = http_hook.run(
            endpoint="/rfr/api/rates",
            data=json.dumps(request),
            headers={"Content-Type": "application/json"},
        )
        ti = kwargs['ti']
        ti.xcom_push(key='request_parameters', value=request_data)
        ti.xcom_push(key='projection_result', value=response.json())

    @task
    def insert_projection(**kwargs):
        ti = kwargs['ti']
        request_parameters = ti.xcom_pull(key='request_parameters', task_ids='get_projection')
        projection_result = ti.xcom_pull(key='projection_result', task_ids='get_projection')
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(\"ProjectionId\") FROM holmen.rate")
        max_projection_id = cur.fetchone()[0]
        if max_projection_id is None:
            max_projection_id = 0
        else:
            max_projection_id += 1
        cur.execute(
            "INSERT INTO holmen.rate VALUES (%s, %s, %s, %s, %s)",
            (
                max_projection_id,
                request_parameters["value_date"],
                projection_result["alpha"],
                json.dumps(request_parameters["data"]),
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )

        start_year = request_parameters["data"]["projection"][0]
        end_year = request_parameters["data"]["projection"][1]  # not inclusive
        intervals_per_year = request_parameters["data"]["projection"][2] if len(
            request_parameters["data"]["projection"]) == 3 else 1
        maturities = np.arange(start_year, end_year, 1 / intervals_per_year).round(6).tolist()
        for maturity, value in zip(maturities, projection_result["rfr"]):
            cur.execute(
                r"INSERT INTO holmen.rate_data VALUES (%s, %s, %s)",
                (max_projection_id, maturity, value),
            )
            maturity += 1
        conn.commit()
        cur.close()
        conn.close()

    create_holmen_schema >> [create_swap_table, create_swap_temp_table, \
                             create_riskfreerate_table, create_riskfreerate_data_table] >> \
    poke_swaps >> get_data() >> merge_data() >> \
    task_http_sensor_check >> get_projection() >> insert_projection()


dag = ProjectSwaps()
