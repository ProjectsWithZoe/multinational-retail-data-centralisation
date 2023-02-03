
from sqlalchemy import inspect
import psycopg2
import pandas as pd
import json, requests



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
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        
        response = requests.get(url, headers=header)
        r = response.raise_for_status()
        #print(r)
        store_number = response.json()
        print(store_number)
        return store_number

    def retrieve_stores_data(self, url, header):

        response = requests.get(url,headers=header)
        data = response.json()
        print(data)
        #data = response.json()
        #print(data)








database_extractor = DataExtractor()



    