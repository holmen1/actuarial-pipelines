import datetime
import json
import pendulum
import os
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor


@dag(
    dag_id="process-contracts",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessContracts():
    create_holmen_schema = PostgresOperator(
        task_id="create_holmen1_schema",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""CREATE SCHEMA IF NOT EXISTS holmen;""",
    )

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

    poke_contracts = FileSensor(
        task_id="poke_contracts",
        filepath="/opt/airflow/dags/files/contracts.csv",
        poke_interval=10,
        timeout=300,
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


    create_holmen_schema >> [create_contract_table, create_contract_temp_table] >> \
    poke_contracts >> get_data() >> merge_data()


dag = ProcessContracts()
