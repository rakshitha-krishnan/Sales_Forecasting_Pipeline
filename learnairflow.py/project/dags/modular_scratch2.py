import warnings
from pmdarima.arima import auto_arima
from pmdarima.arima import ADFTest
from statsmodels.tsa.statespace.sarimax import SARIMAX
import logging
from airflow.providers.mysql.operators.mysql import MySqlOperator
import sqlalchemy

import sys

add_path_to_sys = "C:/Users/Rakshitha Krishnan/Downloads/learnairflow.py/project/dags"
sys.path.append(add_path_to_sys)
from data_conn_scratch import *

def allcombo():
     con_obj = get_mysql_hook_conn()
     createtabledf_obj = createtabledf(con_obj)
     stationaritytest_obj = stationaritytest(createtabledf_obj)
     transform_and_train_data_obj = transform_and_train_data(createtabledf_obj)
     forecasted_sales_obj = forecasted_sales(createtabledf_obj, transform_and_train_data_obj )
     thirty_day_forecast_obj = thirty_day_prediction(createtabledf_obj)
     #store_the_results_obj = store_the_results(con_obj, thirty_day_forecast_obj)

def createtabledf(con_obj):
    select_qry ="SELECT * FROM store_fore"
    connection = con_obj.get_conn()
    df = con_obj.get_pandas_df(select_qry)
    print(df)
    return df

def stationaritytest(createtabledf_obj):
    new_sales_data = createtabledf_obj
    adf_test = ADFTest(alpha=0.05)                         
    result = adf_test.should_diff(new_sales_data['ItemCookTime'])
    print(result)

def transform_and_train_data(createtabledf_obj):
    new_sales_data = createtabledf_obj 
    print(f"The datatype of each column is: {new_sales_data.dtypes}")
    train = new_sales_data[:160]
    test = new_sales_data[-40:]
    ind_features = ['CourseName', 'CourseNumber', 'CoursePreparationTime', 'CourseStartTimeLocal', 'CourseType', 'ItemCookStartTime', 'ItemQuantity']
    warnings.filterwarnings('ignore')
    model=auto_arima(y=train['ItemCookTime'], exogenous=train[ind_features], trace=True)
    print(model)
    return model

def forecasted_sales(createtabledf_obj, transform_and_train_data_obj):
    new_sales_data = createtabledf_obj
    model = transform_and_train_data_obj
    test = new_sales_data[-40:]
    ind_features = ['CourseName', 'CourseNumber', 'CoursePreparationTime', 'CourseStartTimeLocal', 'CourseType', 'ItemCookStartTime', 'ItemQuantity']
    forecast = model.predict(n_periods=len(test), exogenous=test[ind_features])
    print(forecast)

def thirty_day_prediction(createtabledf_obj):
    new_sales_data = createtabledf_obj
    forecast_model = SARIMAX(new_sales_data['ItemCookTime'], order=(1,0,0))
    result = forecast_model.fit()
    fcast = result.predict(len(new_sales_data), len(new_sales_data)+30, type='levels').rename('Column1.ItemCookTime')
    print(fcast)
    return fcast

# def store_the_results(thirty_day_forecast_obj):
#     fcast = thirty_day_forecast_obj
#     mysql_hook = MySqlHook(mysql_conn_id='sql_connect')
#     fcast.to_sql('result_store', mysql_hook.sqlalchemy.create_engine(), if_exists='replace', chunksize= 1000)

# def store_the_results(con_obj, thirty_day_forecast_obj):
#     fcast = thirty_day_forecast_obj
#     col = list(fcast.columns)
#     rows = list(fcast.itertuples(index=False, name=None))
#     con_obj.insert_rows(table="result_store", rows=rows, target_fields=col)

#     return 1











