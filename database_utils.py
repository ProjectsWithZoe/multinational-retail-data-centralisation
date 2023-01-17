
import yaml

class DatabaseConnector:
    def __init__(self) -> None:
        pass

    def read_db_creds():
        with open('db_creds.yaml') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
            print(data)

db1 = DatabaseConnector
db1.read_db_creds()



