
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
import time
# import secret


def fetch_data(api_url, params, no_of_records=None):
    """
    Fetch data from EIA API

    :param api_url: Provide the base API URL.
    :param params: Provide parameters for the data, e.g., your API key, frequency.
    :param no_of_records: Maximum number of records to fetch (default is None, meaning fetch all records).
    :return: A pandas DataFrame containing the fetched data.
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

        if no_of_records is  not None:
            if total_records_fetched>=no_of_records:
                return complete_data.iloc[:no_of_records]
        print(len(complete_data))
        
    
    return complete_data



def mysql_connect(dataframe, table_name):
    """
    Insert DataFrame into MySQL.
    :param dataframe: DataFrame name
    :param table_name: Table name
    """
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
    engine = create_engine(mysql_connection_string)
    

    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)

    print(f"{dataframe} stored in MySQL")


def call_stored_procedure():
    try:
        # Establishing the connection
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



base_url = "https://api.eia.gov/v2/" 


api1_url = f"{base_url}co2-emissions/co2-emissions-aggregates/data/"
api2_url = f"{base_url}electricity/rto/daily-fuel-type-data/data/"
api3_url = f"{base_url}international/data/"


api3_params = {
    "frequency": "annual",
    "data[0]": "value",
    "facets[productId][]": [116, 33, 37],
    "facets[countryRegionId][]": "USA",
    "api_key":"ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"

}
api1_params = {
    "frequency": "annual",
    "data[0]": "value",
    "api_key": "ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
}


api2_params = {
    "frequency": "daily",
    "data[0]": "value",
    "api_key":"ixxID9vFalaJnrWYcqNbAPMFRkmKIiC4OJlAGoae"
}


# data_api1 = fetch_data(api1_url, api1_params)
# data_api2 = fetch_data(api2_url, api2_params, no_of_records=200000)  
data_api3 = fetch_data(api3_url, api3_params)
print(data_api3)
# df1 = data_api1[['period','fuel-name', 'state-name','value','value-units']]
# df2=data_api2[['period','respondent-name','type-name','value','value-units']]
df3 = data_api3[['period','productName','activityName','unitName','value']]
df3 = df3[((df3['activityName'] == 'Generation') & (df3['unitName'] == 'billion kilowatthours')) | (df3['activityName'] == 'Capacity')]
# print(df2)

# mysql_connect(df1,"emission_co2_source")
# mysql_connect(df2,"daily_electricity_source")
mysql_connect(df3,"renewable_generation_source")
# print(data_api1)
time.sleep(5)

call_stored_procedure()
