import datetime
import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator


def get_dataframe(data_path):
    """
    Some transformations to get the data from EIOPA into a dataframe
    """
    df = pd.read_csv(data_path, sep=";", skiprows=1)
    df.rename(columns={'Main menu': 'Index'}, inplace=True)
    df.loc[0, 'Index'] = 'Id'
    df.set_index('Index', inplace=True)
    return df


@dag(
    dag_id="insert-rates-from-eiopa",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def InsertRates():
    create_riskfreerate_data_table = PostgresOperator(
        task_id="create_riskfree_rate_data_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.rate_data (
                "ProjectionId" INTEGER,
                "Month" INTEGER,
                "Maturity" NUMERIC,
                "SpotValue" NUMERIC,
                "Price" NUMERIC,
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
                "SwapId" TEXT,
                "Alpha" NUMERIC,
                "RequestParameters" TEXT,
                "LastUpdated" DATE,
                "VerifiedBy" TEXT
            );""",
    )

    @task
    def get_data(**kwargs):
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/dags/files/eiopa.csv"

        df = get_dataframe(data_path)

        ti = kwargs['ti']
        ti.xcom_push(key='eiopa_rfr', value=df)

    @task
    def insert_projection(**kwargs):
        ti = kwargs['ti']
        eiopa_rates = ti.xcom_pull(key='eiopa_rfr', task_ids='get_data').Sweden

        # EUR_30_06_2023_SWP_LLP_20_EXT_40_UFR_3.45
        swap_id = "EIOPA_" + eiopa_rates.Id.split('_')[0]
        alpha = float(eiopa_rates["alpha"])
        parameters = eiopa_rates.Id[14:]
        date_string = '_'.join(eiopa_rates.Id.split('_')[1:4])
        value_date = datetime.datetime.strptime(date_string, '%d_%m_%Y').date()
        ti.xcom_push(key='value_date', value=value_date)

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
        SELECT "ProjectionId"
        FROM holmen.rate
        WHERE "ValueDate" = %s 
        AND "SwapId" = %s""", (value_date, swap_id,))
        try:
            eiopa_id = cur.fetchone()[0]
        except:
            cur.execute("""
                    select max("ProjectionId")
                    from holmen.rate""")
            eiopa_id = cur.fetchone()[0] + 1

            # RATE
            cur.execute(
                "INSERT INTO holmen.rate VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    eiopa_id,
                    value_date,
                    swap_id,
                    alpha,
                    parameters,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Copilot",
                ),
            )

            # RATE_DATA
            for year, value in eiopa_rates[8:158].items():
                cur.execute(
                    r"INSERT INTO holmen.rate_data VALUES (%s, %s, %s, %s, %s)",
                    (eiopa_id, 12 * int(year), int(year), float(value), 1.0),
                )
            conn.commit()
        else:
            # RATE
            cur.execute(
                """
                UPDATE holmen.rate
                SET "Alpha" = %s, "RequestParameters" = %s, "LastUpdated" = %s
                WHERE "ProjectionId" = %s""",
                (alpha, parameters, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), eiopa_id,))

            # RATE_DATA
            for year, value in eiopa_rates[8:158].items():
                cur.execute(r"""
                UPDATE holmen.rate_data
                SET "SpotValue" = %s
                WHERE "ProjectionId" = %s
                AND "Month" = %s""",
                            (float(value), eiopa_id, 12 * int(year),))
            conn.commit()
        cur.close()
        conn.close()

    @task
    def verify_projection(**kwargs):
        ti = kwargs['ti']
        value_date = ti.xcom_pull(key='value_date', task_ids='insert_projection')

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
        select max("ProjectionId")
        from holmen.rate
        where "SwapId" = 'SKSW'
        and "ValueDate" = %s""",
                    (value_date,))
        sw_id = cur.fetchone()[0]

        cur.execute("""
        select "ProjectionId"
        from holmen.rate
        where "SwapId" = 'EIOPA_SE'
        and "ValueDate" = %s""",
                    (value_date,))
        eiopa_id = cur.fetchone()[0]

        cur.execute("""
        select SW."Month", (SW."SpotValue" - E."SpotValue") * 1E4 "bps"
        from
        (select "Month", "SpotValue"
         from holmen.rate_data where "ProjectionId" = %s) SW,
        (select "Month", "SpotValue"
         from holmen.rate_data where "ProjectionId" = %s) E
        where SW."Month" = E."Month";""",
                    (sw_id, eiopa_id,))
        residual = cur.fetchall()

        residual_within_tolerance = True
        for month, value in residual:
            cur.execute(
                r"""UPDATE holmen.rate_data SET "Price" = %s WHERE "ProjectionId" = %s AND "Month" = %s""",
                (value, eiopa_id, month,),
            )
            if abs(value) > 1.2: # 1.2 bps
                residual_within_tolerance = False

        if residual_within_tolerance:
            cur.execute(
                r"""UPDATE holmen.rate SET "VerifiedBy" = 'Airflow' WHERE "ProjectionId" = %s""",
                (sw_id,),
            )
        else:
            cur.execute(
                r"""UPDATE holmen.rate SET "VerifiedBy" = 'FAILED' WHERE "ProjectionId" = %s""",
                (sw_id,),
            )

        conn.commit()
        cur.close()
        conn.close()

    archive_data = BashOperator(
        task_id='archive_data',
        bash_command='mv /opt/airflow/dags/files/eiopa.csv /opt/airflow/dags/files/archive/eiopa_{{ ti.xcom_pull(key="value_date", task_ids="insert_projection") }}.csv'
    )

    [create_riskfreerate_table, create_riskfreerate_data_table] >> \
    get_data() >> insert_projection() >> verify_projection() >> archive_data


dag = InsertRates()
