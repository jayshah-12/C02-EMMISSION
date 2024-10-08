import requests
import pandas as pd
from sqlalchemy import create_engine 



def fetch_data(api_url, params, no_of_records=None):
    """
    Fetch data from EIA API

    :param api_url: Provide the base API URL.
    :param params: Provide parameters for the data, e.g., your API key, frequency.
    :param no_of_records: Maximum number of records to fetch (default is None, meaning fetch all records).
    :return: A pandas DataFrame containing the fetched data.
    """
    params['offset'] = 1000000  
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
 
    dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"{dataframe} stored in mysql")




def fetch_data(table_name):
    """
    Fetch data from MySQL.

    :param table_name: The name of the table to fetch data from.
    :return: A pandas DataFrame containing the fetched data.
    """
    mysql_connection_string = 'mysql+pymysql://root:root@localhost:3306/eia'
    engine = create_engine(mysql_connection_string)

    # Read the data from the table into a DataFrame
    query = f"SELECT * FROM {table_name}"
    dataframe = pd.read_sql(query, con=engine)

    return dataframe