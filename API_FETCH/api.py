
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
import time



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
        # response.raise_for_status()
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
        

    return complete_data



def mysql_connect(dataframe, table_name):
    """
    Insert DataFrame into MySQL.
    :param dataframe: DataFrame name
    :param table_name: Table name
    """
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
    engine = create_engine(mysql_connection_string)
    
    # Use append instead of replace to avoid dropping the table (which can disable the trigger)
    with engine.begin() as connection:
        dataframe.to_sql(table_name, con=connection, if_exists='replace', index=False)

    print(f"{dataframe} stored in MySQL")

base_url = "https://api.eia.gov/v2/" 


api1_url = f"{base_url}co2-emissions/co2-emissions-aggregates/data/"
api2_url = f"{base_url}electricity/rto/daily-fuel-type-data/data/"
api3_url = f"{base_url}international/data/"

# print(api1_url)
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

# print(api_key)
data_api1 = fetch_data(api1_url, api1_params, no_of_records=20000)
data_api2 = fetch_data(api2_url, api2_params, no_of_records=50000)  
data_api3 = fetch_data(api3_url, api3_params, no_of_records=20000)
df2=data_api2[['period','respondent-name','type-name','value','value-units']]
# print(data_api2)
# print()

mysql_connect(data_api1,"co2_emission")
mysql_connect(df2,"daily_electricity")
mysql_connect(data_api3,"renewable_generation")

# print(secret.api_key)



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
            # Calling the stored procedure
            cursor.callproc('calculate_co2_reduction')
            
            # Commit the transaction if the procedure modifies data
            connection.commit()

            print("Stored procedure executed successfully.")
    
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

time.sleep(5)
    # Call the procedure without parameters
call_stored_procedure()