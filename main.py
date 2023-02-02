import database_utils, data_cleaning, data_extraction


db_connector = database_utils.DatabaseConnector()
data_cleaner = data_cleaning.DataCleaning()
database_extractor = data_extraction.DataExtractor()

df = data_cleaner.clean_user_data(database_extractor)
db_connector.upload_to_db(df,'dim_card_details')