
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
        
        #print(new_df)
        return new_df
    #to be updated once link is fixed
    def clean_store_data(self, database_extractor):
        store_number = 450
        url_two= f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
        header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        
        df_list = database_extractor.retrieve_stores_data(url_two, header=header, store_number=store_number)
        df = pd.DataFrame(df_list)
        return df 
        
        #store_df = database_extractor.retrieve_stores_data(url,header)

    def clean_orders_data(self,database_extractor):
        orders_df = database_extractor.read_rds_table(database_utils.db_connector, 'orders_table')
        orders_df_copy = orders_df

        new_df = orders_df_copy.drop(columns=['1','first_name', 'last_name', 'level_0'])
        #print(new_df)
        return new_df
    
    def convert_product_weights(self,database_extractor):
        url='s3://data-handling-public/products.csv'
        products_df = database_extractor.extract_from_s3(url)
        products_df['weight'] = pd.to_numeric(products_df['weight'].str.replace('[a-zA-Z]',''), errors='coerce')
        print(products_df)
        #weight_column = products_df['weight']
        #print(weight_column)
        #return weight_column


data_cleaner = DataCleaning()

