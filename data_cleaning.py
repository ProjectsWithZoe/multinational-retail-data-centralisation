
import pandas as pd
import database_utils

class DataCleaning:
    def __init__(self) -> None:
        pass

    def clean_user_data(self,database_extractor):
        user_df=database_extractor.read_rds_table(database_utils.db_connector,'legacy_users' )
        #print(user_df)
        #country_code to category
        user_df['country_code'] = user_df['country_code'].astype('category')
        #look out for duplicated values
        user_df.duplicated().sum()
        user_df.isnull().sum()
        #look for unknown values
        #print(user_df.sort_values(by=['company']))

        #values where country code is not 2 letters
        not_2 = user_df['country_code'].apply(lambda x: len(x) !=2)
        new_df = user_df.drop(user_df[not_2].index, inplace=False)
        #print(new_df)

        #date values fixed
        new_df['join_date'] = pd.to_datetime(new_df['join_date'], infer_datetime_format=True)
        new_df['date_of_birth'] = pd.to_datetime(new_df['date_of_birth'], infer_datetime_format=True)
        
        print(new_df)
        return new_df
    #to be updated once link is fixed
    #def clean_store_data(self, database_extractor):
        
        #store_df = database_extractor.retrieve_stores_data(url,header)

    def clean_orders_data(self,database_extractor):
        orders_df = database_extractor.read_rds_table(database_utils.db_connector, 'orders_table')
        orders_df_copy = orders_df

        new_df = orders_df_copy.drop(columns=['1','first_name', 'last_name'])
        print(new_df)
        return new_df



data_cleaner = DataCleaning()

