import datetime

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import BranchPythonOperator
# from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

import pandas as pd
from dependencies.spotify_charts_to_json import create_json
from dependencies.json_to_table import create_table
import pathlib

default_args = {
    'owner': 'Baruch',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 6, 8)
}

dag = airflow.DAG(
    'spotify_charts',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 9 * * * ' # run every day at 09:00 UTC
)

sql_check_if_date_exists = """
    SELECT
    CASE
        WHEN
            EXISTS ( SELECT date FROM `valiant-nucleus-162210.spotify.fact_music_chart` WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
        THEN 
            true
        ELSE
            false
    END
        doesDateExist
    """ # true if there are records, false if there are no records

# this method didnt work for me, got 404 from BigQuery:
"""check_if_need_to_run = BigQueryCheckOperator( 
    task_id='check_if_date_exists',
    sql=sql_check_if_date_exists,
    use_legacy_sql=False,
    dag=dag,
    depends_on_past=False
)"""

# so I used pandas and PythonOperator for this task
def check_if_date_exists():
    if pd.read_gbq(query = sql_check_if_date_exists, dialect = 'standard').bool(): # return bool value from the query
        return 'do_nothing'
    else:
        return 'create_json'
        #raise Exception("date already exists")

check_if_need_to_run = BranchPythonOperator(
    task_id='check_if_date_exists',
    python_callable=check_if_date_exists,
    dag=dag,
    depends_on_past=False
)

def call_for_raw():
    create_json(pathlib.Path('/spotify_project/output_raw'))

t1 = PythonOperator(
    task_id='create_json',
    python_callable=call_for_raw,
    dag=dag,
    depends_on_past=False
)

def call_for_table():
    create_table(pathlib.Path('/spotify_project/output_raw'), pathlib.Path('/spotify_project/output_tables'))

t2 = PythonOperator(
    task_id='create_table',
    python_callable=call_for_table,
    dag=dag,
    depends_on_past=False
)

insert_to_fact = """
        SELECT
            PARSE_DATE('%Y%m%d',
                SUBSTR(m._FILE_NAME, -12, 8)) date,
            c.CountryFullName country,
            position,
            name,
            SUBSTR(artist, 4) artist,
            streams
        FROM
            `valiant-nucleus-162210.spotify.music_chart` m
        JOIN
            `valiant-nucleus-162210.spotify.countries` c
        ON
            m.country = c.country
        WHERE
            SUBSTR(m._FILE_NAME, -12, 8) = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  """
t3 = BigQueryOperator(
    sql=insert_to_fact,
    destination_dataset_table= 'valiant-nucleus-162210.spotify.fact_music_chart',
    task_id='insert_to_fact',
    write_disposition='WRITE_APPEND',
    use_legacy_sql=False,
    dag=dag
)

def date_already_exists():
    print('date already exists in BigQuery')


do_nothing = PythonOperator(
    task_id='do_nothing',
    python_callable=date_already_exists,
    dag=dag
)

check_if_need_to_run >> t1 >> t2 >> t3 # the process should run
check_if_need_to_run >> do_nothing # date already exists, process should not run