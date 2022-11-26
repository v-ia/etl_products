from airflow import DAG
from airflow.operators.python import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


# secondary function for extract_and_transform_run stage
async def _extract_and_transform(**kwargs):
    import asyncio
    import aiohttp

    async with aiohttp.ClientSession() as client:
        tasks = set()
        for barcode in kwargs['ti'].xcom_pull(key='return_value', task_ids=['get_barcodes_db']):
            task = asyncio.create_task(client.get(f'https://world.openfoodfacts.org/api/v0/product/{barcode}.json'))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        responses = await asyncio.gather(*tasks)
    nutriscore_grades = dict()
    for response in responses:
        item = response.json()
        nutriscore_grades[item['product']['code']] = item['product']['nutriscore_grade']
    return nutriscore_grades

with DAG(
    dag_id='etl_products',
    description='Adding info about diary products',
    start_date=datetime(2022, 10, 1),
    schedule=timedelta(seconds=1),
    max_active_runs=1
) as dag:

    @task(task_id='get_barcodes_db')
    def get_barcodes_db():
        import psycopg2

        connection = BaseHook.get_connection('postgres_db')
        with psycopg2.connect(
            host=connection.host,
            port=connection.port,
            user=connection.login,
            password=connection.password,
            dbname=connection.EXTRA_KEY
        ) as conn:
            with conn.cursor() as cur:
                records = cur.fetchall(
                    'SELECT barcode FROM items WHERE id > $1 AND nutriscore_grade IS NULL ORDER BY id LIMIT $2',
                    Variable.get("current_id"), Variable.get("n_items"))
        return records

    @task(task_id='extract_and_transform_run')
    async def extract_and_transform_run(**kwargs):
        import asyncio

        return asyncio.run(_extract_and_transform(**kwargs))

    @task(task_id='load_data_to_db', do_xcom_push=False)
    def load_data_to_db(**kwargs):
        import psycopg2

        nutriscore_grades = kwargs['ti'].xcom_pull(key='return_value', task_ids=['extract_and_transform_run'])
        connection = BaseHook.get_connection('postgres_db')
        with psycopg2.connect(
                host=connection.host,
                port=connection.port,
                user=connection.login,
                password=connection.password,
                dbname=connection.EXTRA_KEY
        ) as conn:
            ids = list()
            with conn.cursor() as cur:
                for barcode, nutriscore_grade in nutriscore_grades.items():
                    cur.execute('UPDATE items SET nutriscore_grade = $1 WHERE barcode = $2 RETURNING id',
                                nutriscore_grade, barcode)
                    ids.append(int(next(cur)))
        Variable.set("current_id", max(ids))

    get_barcodes_db() >> extract_and_transform_run() >> load_data_to_db()
