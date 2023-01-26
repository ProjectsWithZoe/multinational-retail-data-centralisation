import database_utils as du
from sqlalchemy import create_engine,inspect
import psycopg2

class DataExtractor:
    def __init__(self) -> None:
        pass

    def list_db_tables(self,engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print(table_names)