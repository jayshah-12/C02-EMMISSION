import requests
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# import secret as secret

def fetch_data(api_url, params, no_of_records=None):
    params['offset'] = 0  
    complete_data = pd.DataFrame()
    total_records_fetched = 0  

    while True:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Ensure we raise an error for bad responses
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
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
    engine = create_engine(mysql_connection_string)
    dataframe.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"{table_name} stored in MySQL")

def fetch_and_store_data(api_url, params, table_name):
    data = fetch_data(api_url, params, no_of_records=20000)
    mysql_connect(data, table_name)

# Airflow DAG definition
with DAG(
    'eia_data_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 10, 1),
        'retries': 1,
    },
    schedule_interval='@daily',  # Adjust as needed
    catchup=False,
) as dag:

    api1_params = {
        "frequency": "annual",
        "data[0]": "value",
        "api_key": secret.api_key
    }
    
    api2_params = {
        "frequency": "daily",
        "data[0]": "value",
        "api_key": secret.api_key
    }
    
    api3_params = {
        "frequency": "annual",
        "data[0]": "value",
        "facets[productId][]": [116, 33, 37],
        "facets[countryRegionId][]": "USA",
        "api_key": secret.api_key
    }

    fetch_api1_data = PythonOperator(
        task_id='fetch_api1_data',
        python_callable=fetch_and_store_data,
        op_kwargs={'api_url': f"https://api.eia.gov/v2/co2-emissions/co2-emissions-aggregates/data/", 'params': api1_params, 'table_name': "df1"}
    )

    fetch_api2_data = PythonOperator(
        task_id='fetch_api2_data',
        python_callable=fetch_and_store_data,
        op_kwargs={'api_url': f"https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/", 'params': api2_params, 'table_name': "df2"}
    )

    fetch_api3_data = PythonOperator(
        task_id='fetch_api3_data',
        python_callable=fetch_and_store_data,
        op_kwargs={'api_url': f"https://api.eia.gov/v2/international/data/", 'params': api3_params, 'table_name': "df3"}
    )

    fetch_api1_data >> fetch_api2_data >> fetch_api3_data  # Set task dependencies
