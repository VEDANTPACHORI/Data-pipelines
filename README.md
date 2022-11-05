# Data-pipelines
Work and projects done for data pipelines.
#code for the data retrieving from the MySQL database to the apache airflow.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
import sqlite3
import os

dag_path= os.getcwd()

def get_data():
    sql_stmt = "select * from deals_data"
    mysql_hook = MySqlHook( mysql_conn_id='MySQL_db', schema='dealsnaks')                             
    mysql_conn = mysql_hook.get_conn()
    Cursor = mysql_conn.cursor()
    Cursor.execute (sql_stmt)
    return Cursor.fetchall()

with DAG(
    dag_id= 'mysql_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=10, day=29),
    catchup=False
) as dag:
    task_get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        do_xcom_push= True
    )

task_get_data
