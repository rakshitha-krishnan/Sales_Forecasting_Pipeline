import pandas as pd
import numpy as np
import warnings
from pmdarima.arima import auto_arima
from pmdarima.arima import ADFTest
from statsmodels.tsa.statespace.sarimax import SARIMAX
import logging
from airflow.providers.mysql.operators.mysql import MySqlOperator
import csv

import sys

add_path_to_sys = "C:/Users/Rakshitha Krishnan/Downloads/learnairflow.py/project/dags"
sys.path.append(add_path_to_sys)
from data_conn_scratch import *


def allcombo():
     con_obj = get_mysql_hook_conn()
     createtabledf_obj = createtabledf(con_obj)
     analyzed_data_object = analyzedata(createtabledf_obj)
    #  stationary_test_obj = stationaritytest(analyzed_data_object)
    # training_model_obj = trainingmodel(analyzed_data_object)
# #     forecast_values_obj = forecastvalues(analyzed_data_object, training_model_obj)
     return None
     
def createtabledf(con_obj):
    select_qry ="SELECT * FROM store_db"
    connection = con_obj.get_conn()
    cur_dev = connection.cursor()
    h = cur_dev.execute(select_qry)
    h = cur_dev.fetchall()
    df = con_obj.get_pandas_df(select_qry)
    print(df)
    return df


def analyzedata(createtabledf_obj):
    new_sales_data = createtabledf_obj 
    print(f"The datatype of each column is: {new_sales_data.dtypes}")
    new_sales_data['belongs_to'] = pd.to_datetime(new_sales_data['belongs_to'])
    print(f"the datatype of each column after conversion is: {new_sales_data.dtypes}")
    return new_sales_data

def stationaritytest(analyzed_data_object):
    sales_data = analyzed_data_object
    adf_test = ADFTest(alpha=0.05)                         
    result = adf_test.should_diff(sales_data['totala'])
    print(f"From this we determine if the data is stationary or not:{result}")
    return sales_data

# def trainingmodel(analyzed_data_object):
#     new_sales_data = analyzed_data_object
#     train = new_sales_data[:8613]
#     test = new_sales_data[-2870:]
#     ind_features =['location_id', 'belongs_to']
#     trained_model = auto_arima(y=train['totala'], exogenous=train[ind_features], trace=False)
#     trained_model_summary = trained_model.summary()
#     print(f"The Best ARIMA model is :{trained_model_summary}")
#     trained_model.fit(train['totala'])
#     return trained_model