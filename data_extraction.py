
from sqlalchemy import inspect
import psycopg2
import pandas as pd
import json, requests
import boto3
import config

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
        return df_list
        #df = pd.DataFrame(df_list)

    def extract_from_s3(self,url):
        #url = 's3://data-handling-public/products.csv'
        session = boto3.Session(aws_access_key_id =config.aws_access_key_id,
                                aws_secret_access_key = config.aws_secret_access_key)
        s3_client = session.client('s3')
        bucket,key = url.split('/')[2], '/'.join(url.split('/')[3:])
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        #print(df)
        return df
    
    def extract_s3_datetime(self,url):
        url='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json.'
        session =boto3.Session(aws_access_key_id =config.aws_access_key_id,
                                aws_secret_access_key = config.aws_secret_access_key)
        s3_client = session.client('s3')
        bucket,key = 'data-handling-public', 'date_details.json'
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_json(response['Body'])
        #print(df)
        return df
        
        


database_extractor = DataExtractor()



    