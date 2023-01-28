import data_extraction, database_utils


class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self,database_extractor):
        user_df=database_extractor.read_rds_table(database_utils.db_connector,'legacy_users' )
        #look out for null values
        print(user_df.duplicated().sum())


        

data_cleaner = DataCleaning()
data_cleaner.clean_user_data(data_extraction.database_extractor)