import datetime

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

import pandas as pd
from dependencies.spotify_charts_to_json import create_json
from dependencies.json_to_table import create_table

default_args = {
    'owner': 'Baruch',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 6, 2)
}

dag = airflow.DAG(
    'spotify_charts',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 9 * * * ' # run every day at 09:00 UTC
)

t1 = PythonOperator(
    task_id='create_json',
    python_callable=create_json,
    dag=dag,
    depends_on_past=False
)

t2 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
    depends_on_past=False
)

sql = """
    SELECT
        PARSE_DATE('%Y%m%d',
            SUBSTR(_FILE_NAME, -12, 8)) date,
        country,
        position,
        name,
        SUBSTR(artist, 4) artist,
        streams
    FROM
        `valiant-nucleus-162210.spotify.music_chart`
    WHERE
        SUBSTR(_FILE_NAME, -12, 8) = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  """
t3 = BigQueryOperator(
    sql=sql,
    destination_dataset_table= 'valiant-nucleus-162210.spotify.fact_music_chart',
    task_id='insert_to_fact',
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    dag=dag
)

t1 >> t2 >> t3