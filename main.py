import database_utils, data_cleaning, data_extraction
import pandas as pd


db_connector = database_utils.DatabaseConnector()
data_cleaner = data_cleaning.DataCleaning()
database_extractor = data_extraction.DataExtractor()

header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
store_number = 450
url_two= f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

df = database_extractor.retrieve_stores_data(url_two,header,450)
#print(df)

cleaned = data_cleaner.clean_store_data(data_extraction.database_extractor)

db_connector.upload_to_db(cleaned, 'dim_store_details')



