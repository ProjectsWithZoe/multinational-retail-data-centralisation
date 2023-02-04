
from sqlalchemy import inspect
import psycopg2
import pandas as pd
import json, requests

header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
url_one = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


class DataExtractor:
    def __init__(self) -> None:
        pass

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        #print(table_names)
        return table_names

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine('db_creds.yaml')
        query = (f'SELECT * FROM {table_name}')
        df = pd.read_sql_query(query, engine )
        print(df)
        return df

    def list_number_of_stores(self,url,header):
       
        response = requests.get(url=url_one, headers=header)
        r = response.raise_for_status()
        #print(r)
        store_number = response.json()
        print(store_number)
        return store_number

    def retrieve_stores_data(self, url, header, store_number):
        df_list = []
        while store_number >=0:
            url_two= f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            response = requests.get(url=url_two,headers=header)
            data = response.json()
            #print(data)
            df_list.append(data)
            store_number -=1
        #print(df_list)
        df = pd.DataFrame(df_list)
        print(df)


database_extractor = DataExtractor()



    