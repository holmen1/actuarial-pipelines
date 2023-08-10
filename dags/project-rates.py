import datetime
import json
import pendulum
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import XCom
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class SwapSensor(BaseSensorOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        pg_hook = PostgresHook(postgres_conn_id='tutorial_pg_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX("ValueDate")
            FROM holmen.swap
            WHERE \"ValueDate\" NOT IN (
                SELECT DISTINCT \"ValueDate\"
                FROM holmen.rate
            )
        """)
        max_date = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        ti = context['ti']
        ti.xcom_push(key='max_date', value=max_date)
        return bool(max_date)


def get_request_data(value_date):
    postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    cur.execute(r"""SELECT "Tenor", "Value" FROM holmen.swap
                    WHERE "ValueDate" = %s
                    AND "Tenor" IN (2, 3, 5, 10)
                    ORDER BY "Tenor";""", (value_date,))
    rows = cur.fetchall()
    tenors = [row[0] for row in rows]
    values = [float(row[1]) for row in rows]

    return {
        "value_date": value_date.strftime("%Y-%m-%d"),
        "data": {
            "par_rates": values,
            "par_maturities": tenors,
            "projection": [1, 151],
            "ufr": 0.0345,
            "convergence_maturity": 20,
            "tol": 1E-4,
            "credit_risk_adjustment": 0.001
        }
    }


@dag(
    dag_id="project-rates",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProjectRates():
    create_riskfreerate_data_table = PostgresOperator(
        task_id="create_riskfree_rate_data_table",
        postgres_conn_id="tutorial_pg_conn",
        sql=r"""
            CREATE TABLE IF NOT EXISTS holmen.rate_data (
                "ProjectionId" INTEGER,
                Month INTEGER,
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
                "Alpha" NUMERIC,
                "RequestParameters" TEXT,
                "LastUpdated" DATE,
                "VerifiedBy" TEXT
            );""",
    )

    poke_swaps = SwapSensor(
        task_id="poke_swaps",
        timeout=300,
    )

    task_http_sensor_check = HttpSensor(
        task_id="http_sensor_check",
        http_conn_id="smithwilson_api",
        endpoint="/",
    )

    @task
    def get_projection(**kwargs):
        ti = kwargs['ti']
        value_date = ti.xcom_pull(key='max_date', task_ids='poke_swaps')
        http_hook = HttpHook(method="POST", http_conn_id="smithwilson_api")
        request_data = get_request_data(value_date)
        request = request_data['data']
        response = http_hook.run(
            endpoint="/api/monthly",
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
        cur.execute("""SELECT MAX("ProjectionId") FROM holmen.rate""")
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

        end_year = request_parameters["data"]["projection"][1]  # not inclusive
        months = np.arange(0, (end_year - 1) * 12 + 1)
        maturities = months / 12.0
        for month, maturity, value, price in zip(months, maturities, projection_result["rfr"],
                                                 projection_result["price"]):
            cur.execute(
                r"INSERT INTO holmen.rate_data VALUES (%s, %s, %s, %s, %s)",
                (max_projection_id, int(month), maturity, value, price),
            )
        conn.commit()
        cur.close()
        conn.close()

    [create_riskfreerate_table, create_riskfreerate_data_table] >> \
    poke_swaps >> task_http_sensor_check >> \
    get_projection() >> insert_projection()


dag = ProjectRates()
