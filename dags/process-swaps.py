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
    dag_id="process-swaps",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessSwaps():
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


    create_holmen_schema >> [create_swap_table, create_swap_temp_table] >> \
    poke_swaps >> get_data() >> merge_data()


dag = ProcessSwaps()
