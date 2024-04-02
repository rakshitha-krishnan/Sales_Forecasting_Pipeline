from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator



import sys

add_path_to_sys = "C:/Users/Rakshitha Krishnan/Downloads/learnairflow.py/project/dags"
sys.path.append(add_path_to_sys)

from modular_scratch import *


default_args = {"owner":"airflow", "start_date" : datetime(2023,5,11)}

with DAG(dag_id="workflow_scratch", default_args = default_args, schedule_interval= '@daily', catchup = False) as dag:
    start = DummyOperator(
        task_id = "start"
    )
    create_table = MySqlOperator(
       task_id = "create_table",
       mysql_conn_id = "sql_connect",
       sql = "CREATE table IF NOT EXISTS store_db (location_id int NULL, belongs_to text NULL, totala int NULL)"
    )
    insert_db = MySqlOperator(
        task_id = "insert_db",
        mysql_conn_id = "sql_connect",
        sql = "LOAD DATA INFILE '/var/lib/mysql-files/store_sales.csv' INTO TABLE store_db FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
    )
    ETL = PythonOperator(
        task_id='ETL', 
        python_callable = allcombo,
        dag=dag
        )
    
    end = DummyOperator(
        task_id='end'
        )

    #start >> ETL >> end
    start >> create_table >> insert_db >>  ETL >> end

