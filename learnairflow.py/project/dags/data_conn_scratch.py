from sqlalchemy import create_engine
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd


def get_mysql_hook_conn():
    con = MySqlHook(mysql_conn_id='sql_connect', schema = 'storenew')
    return con