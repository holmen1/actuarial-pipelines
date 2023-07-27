import datetime
import json
import pendulum
import os
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import XCom


def get_request_data():
    postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("""SELECT MAX("ValueDate") FROM holmen.contract""")
    max_value_date = cur.fetchone()[0]
    cur.execute("""SELECT MAX("ContractNo") FROM holmen.contract
                   WHERE "ValueDate" = %s""", (max_value_date,))
    max_contract = cur.fetchone()[0]

    cur.execute("""SELECT "ContractNo", "ValueDate", "BirthDate", "Sex", "VestingAge", "Guarantee", "PayPeriod", "Table"
                   FROM holmen.contract
                   WHERE "ContractNo" = %s AND "ValueDate" = %s""", (max_contract, max_value_date))
    row = cur.fetchone()

    return {
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
    dag_id="project-cashflows",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProjectCashflows():

    create_holmen_schema = PostgresOperator(
        task_id="create_holmen1_schema",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""CREATE SCHEMA IF NOT EXISTS holmen;""",
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

    task_http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="liabilities_api",
        endpoint="/",
    )

    @task
    def get_cashflows(**kwargs):
        http_hook = HttpHook(method="POST", http_conn_id="liabilities_api")
        request_data = get_request_data()
        request = request_data['data']
        response = http_hook.run(
            endpoint="/cashflows",
            data=json.dumps(request),
            headers={"Content-Type": "application/json"},
        )
        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""TRUNCATE TABLE holmen.cashflow""")
        month = 0
        for benefit in response.json():
            cur.execute("""INSERT INTO holmen.cashflow VALUES (%s, %s, %s, %s)""",
            (request['contractNo'], request['valueDate'], month, benefit))
            month += 1
        conn.commit()


    create_holmen_schema >> create_cashflow_table >> task_http_sensor_check >> get_cashflows()


dag = ProjectCashflows()
