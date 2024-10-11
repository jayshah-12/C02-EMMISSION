import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import time
import os
import hvac
 # Make sure you have your secret module with the API key

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'eia_data_pipeline',
    default_args=default_args,
    description='Pipeline for fetching and inserting data into MySQL from EIA API',
    schedule_interval=timedelta(days=1),  # Run daily
)


def fetch_data(api_url, params, no_of_records=None):
    """
    Fetch data from EIA API
    """
    params['offset'] = 0
    complete_data = pd.DataFrame()
    total_records_fetched = 0  

    while True:
        response = requests.get(api_url, params=params)
        data = response.json()

        records = data['response']['data']

        if not records:
            break

        df = pd.DataFrame(records)
        complete_data = pd.concat([complete_data, df], ignore_index=True)
        total_records_fetched += len(records)
        params['offset'] += len(records)

        if no_of_records is not None and total_records_fetched >= no_of_records:
            return complete_data.iloc[:no_of_records]

    return complete_data


def mysql_connect(dataframe, table_name):
    """
    Insert DataFrame into MySQL.
    """
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
    engine = create_engine(mysql_connection_string)

    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)

    print(f"{dataframe} stored in MySQL")


def call_stored_procedure():
    """
    Call stored procedure in MySQL.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='eia',
            user='root',
            password='root'
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.callproc('calculate_co2_reduction')
            connection.commit()
            print("Stored procedure executed successfully.")
    
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")






# Task 1: Fetch data from the first API
def fetch_api1_data(**kwargs):
    api1_url = "https://api.eia.gov/v2/co2-emissions/co2-emissions-aggregates/data/"
    api1_params = {
        "frequency": "annual",
        "data[0]": "value",
        "api_key": "ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
    }
    data_api1 = fetch_data(api1_url, api1_params, no_of_records=20000)
    mysql_connect(data_api1, "co2_emission")


# Task 2: Fetch data from the second API
def fetch_api2_data(**kwargs):
    api2_url = "https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
    api2_params = {
        "frequency": "daily",
        "data[0]": "value",
        "api_key": "ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
    }
    data_api2 = fetch_data(api2_url, api2_params, no_of_records=50000)
    df2 = data_api2[['period', 'respondent-name', 'type-name', 'value', 'value-units']]
    mysql_connect(df2, "daily_electricity")


# Task 3: Fetch data from the third API
def fetch_api3_data(**kwargs):
    api3_url = "https://api.eia.gov/v2/international/data/"
    api3_params = {
        "frequency": "annual",
        "data[0]": "value",
        "facets[productId][]": [116, 33, 37],
        "facets[countryRegionId][]": "USA",
        "api_key": "ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
    }
    data_api3 = fetch_data(api3_url, api3_params, no_of_records=20000)
    mysql_connect(data_api3, "renewable_generation")


# Task 4: Call the stored procedure
def execute_stored_procedure(**kwargs):
    time.sleep(5)  # Wait for data insertion to complete
    call_stored_procedure()


# Define the tasks
fetch_api1_task = PythonOperator(
    task_id='fetch_api1_data',
    python_callable=fetch_api1_data,
    provide_context=True,
    dag=dag,
)

fetch_api2_task = PythonOperator(
    task_id='fetch_api2_data',
    python_callable=fetch_api2_data,
    provide_context=True,
    dag=dag,
)

fetch_api3_task = PythonOperator(
    task_id='fetch_api3_data',
    python_callable=fetch_api3_data,
    provide_context=True,
    dag=dag,
)

call_stored_proc_task = PythonOperator(
    task_id='call_stored_procedure',
    python_callable=execute_stored_procedure,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
[fetch_api1_task, fetch_api2_task, fetch_api3_task] >> call_stored_proc_task
