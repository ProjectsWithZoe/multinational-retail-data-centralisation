import database_utils, data_cleaning, data_extraction


db_connector = database_utils.DatabaseConnector()
data_cleaner = data_cleaning.DataCleaning()
database_extractor = data_extraction.DataExtractor()

url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

database_extractor.list_number_of_stores(url,header=header)