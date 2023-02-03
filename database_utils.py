
import yaml
from sqlalchemy import create_engine,MetaData
import psycopg2
import config

DBAPI = 'psycopg2'

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds(self,creds_file):
        creds_file = 'db_creds.yaml'
        with open(creds_file) as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        #print (data)
        return data

    def init_db_engine(self,creds_file):
        data = self.read_db_creds(creds_file)
        
        url =f"postgresql+{DBAPI}://{data['RDS_USER']}:{data['RDS_PASSWORD']}@{data['RDS_HOST']}:{data['RDS_PORT']}/{data['RDS_DATABASE']}" 
        engine = create_engine(url, echo=False)
        metadata = MetaData(bind=engine)
        db_engine = engine
        #print(engine)
        return db_engine  

    def upload_to_db(self,df,table_name):
        user = config.user
        password= config.password
        sql_engine = create_engine(f'postgresql://{user}:{password}@localhost:5432/Sales_Data')
        df.to_sql(table_name, sql_engine)

db_connector = DatabaseConnector()






