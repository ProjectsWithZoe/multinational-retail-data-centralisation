import yaml

def read_db_creds():
    with open('db_creds.yaml') as file:
        data = yaml.load(file, Loader=yaml.FullLoader)
        return data

data = read_db_creds()
print(data)