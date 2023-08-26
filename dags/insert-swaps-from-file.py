import datetime
import pendulum
import os
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash import BashOperator


@dag(
    dag_id="insert-swaps-from-file",
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

    create_swap_stage_table = PostgresOperator(
        task_id="create_swap_stage_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            DROP TABLE IF EXISTS holmen.swap_stage;
            CREATE TABLE holmen.swap_stage (
                "VALUE_DATE" DATE,
                "IDENTIFIER" TEXT,
                "PX_LAST" NUMERIC,
                PRIMARY KEY ("VALUE_DATE", "IDENTIFIER")
            );""",
    )

    create_swap_table = PostgresOperator(
        task_id="create_swap_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.swap (
                "ValueDate" DATE,
                "Id" TEXT,
                "Tenor" INTEGER,
                "SettlementFreq" INTEGER,
                "Value" NUMERIC,
                "LastUpdated" TIMESTAMP,
                PRIMARY KEY ("ValueDate", "Id", "Tenor")
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
                "COPY holmen.swap_stage FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def merge_data(**kwargs):
        ti = kwargs['ti']
        now = datetime.datetime.now()
        ti.xcom_push(key='merge_timestamp', value=now.strftime("%Y_%m_%d_%H%M%S"))
        sql = r"""
            INSERT INTO holmen.swap
            SELECT *
            FROM (
                SELECT DISTINCT 
                    "VALUE_DATE" AS "ValueDate",
                    substring("IDENTIFIER" from '[A-Z]+') AS "Id",
                    substring("IDENTIFIER" from '\d+')::integer AS "Tenor",
                    1 as "SettlementFreq",
                    "PX_LAST" AS "Value",
                    %s AS "LastUpdated"
                FROM holmen.swap_stage
            ) t
            ON CONFLICT ON CONSTRAINT swap_pkey DO UPDATE
            SET "SettlementFreq" = excluded."SettlementFreq",
                "Value" = excluded."Value",
                "LastUpdated" = excluded."LastUpdated";
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(sql, (now,))
            conn.commit()
            return 0
        except Exception as e:
            return 1

    archive_data = BashOperator(
        task_id='archive_data',
        bash_command='mv /opt/airflow/dags/files/swaps.csv /opt/airflow/dags/files/archive/swaps_{{ ti.xcom_pull(key="merge_timestamp", task_ids="merge_data") }}.csv'
    )

    create_holmen_schema >> [create_swap_table, create_swap_stage_table] >> \
    poke_swaps >> get_data() >> merge_data() >> archive_data


dag = ProcessSwaps()
