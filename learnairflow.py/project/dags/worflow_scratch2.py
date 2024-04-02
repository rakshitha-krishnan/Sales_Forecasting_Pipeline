from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator


import sys

add_path_to_sys = "C:/Users/Rakshitha Krishnan/Downloads/learnairflow.py/project/dags"
sys.path.append(add_path_to_sys)

from modular_scratch2 import *


default_args = {"owner":"airflow", "start_date" : datetime(2023,5,11)}

with DAG(dag_id="workflow_scratch2", default_args = default_args, schedule_interval= '@daily', catchup = False) as dag:
    start = DummyOperator(
        task_id = "start"
    )
    create_table = MySqlOperator(
       task_id = "create_table",
       mysql_conn_id = "sql_connect",
       sql='''
            CREATE table IF NOT EXISTS store_fore(
                ActivityLevel int NULL, 
                CourseName text NULL, 
                CourseNumber int NULL, 
                CoursePaidTime int NULL, 
                CourseParkTime int NULL, 
                CoursePreparationTime int NULL, 
                CourseStartTimeLocal text NULL, 
                CourseStartTimeUTC text NULL, 
                CourseType text NULL, 
                CourseUid text NULL, 
                Destination int NULL, 
                DestinationName text NULL, 
                DisplayGroupId int NULL, 
                FastTrackedTime int NULL, 
                FirstDisplayTime int NULL, 
                FirstStoreTime int NULL, 
                FirstTenderTime int NULL, 
                FirstViewedTime int NULL, 
                FoodDeliveredTime int NULL, 
                InsertDate text NULL, 
                ItemCategory int NULL, 
                ItemCookStartTime int NULL, 
                ItemCookTime int NULL, 
                ItemDescription text NULL, 
                ItemId int NULL, 
                ItemNumber int NULL, 
                ItemQuantity int NULL, 
                ItemTagTime int NULL, 
                ItemUID text NULL, 
                LastBumpTime int NULL, 
                LastPreparedTime int NULL, 
                LastRecallTime int NULL, 
                LastTotalTime int NULL, 
                LastUnbumpTime int NULL, 
                Modifier1Id int NULL, 
                Modifier2Id int NULL, 
                Modifier3Id int NULL, 
                ParentItemNumber int NULL, 
                PriorityStatusReached text NULL, 
                PriorityTime int NULL, 
                RemakeTime int NULL, 
                RushStatusReached text NULL, 
                RushTime int NULL, 
                ServerId int NULL, 
                ServerName text NULL, 
                SoSTag text NULL, 
                StationType text NULL, 
                seat int NULL, 
                TerminalNumber int NULL, 
                TimeStampLocal text NULL, 
                TimeStampUTC text NULL, 
                TopLevelItem text NULL, 
                TransactionNumber int NULL, 
                TransactionStartTimeLocal text NULL, 
                TransactionStartTimeUIC text NULL, 
                TransactionUID text NULL, UID text NULL, 
                ViewId int NULL, 
                ViewName text NULL, 
                Token text NULL
            );
       '''
    )
    insert_db = MySqlOperator(
        task_id = "insert_db",
        mysql_conn_id = "sql_connect",
        sql = "LOAD DATA INFILE '/var/lib/mysql-files/transformed_data.csv' INTO TABLE store_fore FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"
    )
    create_result_table = MySqlOperator(
        task_id = "create_result_table",
        mysql_conn_id = "sql_connect",
        sql = "CREATE table IF NOT EXISTS result_store(cook_time_predict int NULL);"
    )
    ETL = PythonOperator(
        task_id='ETL', 
        python_callable = allcombo,
        dag=dag
        )
    end = DummyOperator(
        task_id='end'
        )

    start >> create_table >> insert_db >> create_result_table >> ETL >> end