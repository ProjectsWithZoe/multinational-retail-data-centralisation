import database_utils 
from sqlalchemy import create_engine,inspect
import psycopg2
import pandas as pd

class DataExtractor:
    def __init__(self) -> None:
        pass

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print(table_names)
        return table_names

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine('db_creds.yaml')
        table_names = self.list_db_tables(engine)
        query = (f'SELECT * FROM {table_name}')
        df = pd.read_sql_query(query, engine )
        print(df)
        return df

database_extractor = DataExtractor()
database_extractor.read_rds_table(database_utils.db_connector,'legacy_users' )

    