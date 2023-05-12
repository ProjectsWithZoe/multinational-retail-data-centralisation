# multinational-retail-data-centralisation
This project is the Multinational Data Centralisation project, which is a comprehensive project aimed at transforming and analysing large datasets from multiple data sources. By utilising the power of Pandas, the project cleans the data, and produces a STAR based database schema for optimised data storage and access. The project also builds complex SQL-based data queries, allowing the user to extract valuable insights and make informed decisions. This project provided me with the experience of building a real-life complete data solution, from data acquisition to analysis, all in one place. 

<h1> Database_utils.py </h1>

The code allows for easy connection to a PostgreSQL database, retrieving database credentials from a YAML file, initializing a database engine, and uploading pandas DataFrame data to the database with the help of SQLAlchemy and psycopg2 libraries.

Reading Database Credentials: The code allows reading the credentials required to connect to a PostgreSQL database from a YAML file. The read_db_creds method opens the YAML file, loads its contents, and returns the credentials as a dictionary.

Initializing a Database Engine: The code provides a method called init_db_engine that initializes a database engine using the credentials obtained from the YAML file. It constructs a database URL based on the credentials and uses the create_engine function from SQLAlchemy to create the engine. The initialized database engine can be used to establish a connection to the PostgreSQL database.

Uploading Data to the Database: The code includes a method named upload_to_db that enables uploading a pandas DataFrame to a PostgreSQL database. It takes a DataFrame and a table name as inputs. Using the SQLAlchemy engine created earlier, the method establishes a connection to the database and utilizes the to_sql method of the DataFrame to upload the data to the specified table in the database.

DatabaseConnector Class: The DatabaseConnector class encapsulates the above functionalities. It provides an interface for interacting with a PostgreSQL database by reading credentials, initializing the database engine, and uploading data. An instance of this class can be created to utilize these functionalities conveniently.

<h1> Data_extraction.py </h1>

The code provides a DataExtractor class that encapsulates various functions for retrieving and extracting data from different sources.
retrieve_pdf_data(url): This function takes a URL pointing to a PDF file as input. It uses the tabula library to read the PDF file and extract the data from all its pages. The extracted data is concatenated into a single pandas DataFrame and returned.

list_db_tables(engine): Given a SQLAlchemy engine object, this function lists the names of all tables in the connected database. It uses the inspect function from SQLAlchemy to retrieve the table names.

read_rds_table(db_connector, table_name): This function takes a db_connector object (an instance of DatabaseConnector class) and a table name as input. It initializes a database engine using the db_connector and executes a SELECT query to fetch all data from the specified table. The query result is returned as a pandas DataFrame.

list_number_of_stores(url, header): It sends an HTTP GET request to the specified URL with the provided header. The response is expected to contain a JSON object representing the number of stores. The function retrieves the JSON data, prints it, and returns the JSON object.

retrieve_stores_data(url, header, store_number): This function retrieves store details from an API by making multiple HTTP GET requests. It starts from a given store_number and iterates down until it reaches 0. For each store, it constructs a specific URL and sends an HTTP GET request with the provided header. The response data is collected into a list, and the final list of store details is returned as a pandas DataFrame.

extract_from_s3(url): Given an S3 bucket URL, this function uses the boto3 library to connect to AWS S3 using the provided AWS credentials. It retrieves the specified file from S3, reads its contents as a pandas DataFrame, and returns the DataFrame.

extract_s3_datetime(url): This function retrieves a JSON file from an S3 bucket and reads its contents as a pandas DataFrame. The URL, bucket, and key are pre-defined within the function.

These functions collectively provide data extraction capabilities from PDF files, SQL databases, APIs, and AWS S3 storage. The extracted data is returned as pandas DataFrames, which can be further processed and analyzed as needed.

<h1> Data_cleaning.py </h1>

The provided code defines a DataCleaning class with several methods that perform data cleaning operations on different datasets.
clean_user_data: This method takes a database_extractor object as input and performs cleaning operations on the user data. It retrieves the user data from the 'legacy_users' table using the read_rds_table method of the database_extractor object. It then performs the following cleaning tasks:

Converts the 'country_code' column to the category data type.
Checks for duplicated values in the DataFrame.
Checks for null values in the DataFrame.
Filters out rows where the 'country_code' column does not contain exactly 2 letters.
Converts the 'join_date' and 'date_of_birth' columns to datetime data type.
Finally, it returns the cleaned DataFrame.
clean_card_data: This method takes a DataFrame (df) as input and performs cleaning operations on card data. It drops duplicate rows from the DataFrame and returns the cleaned DataFrame.

clean_store_data: This method takes a database_extractor object as input and performs cleaning operations on store data. It retrieves store data from an API endpoint using the retrieve_stores_data method of the database_extractor object. The retrieved data is converted into a DataFrame, and then returned.

clean_orders_data: This method takes a database_extractor object as input and performs cleaning operations on orders data. It retrieves the orders data from the 'orders_table' using the read_rds_table method of the database_extractor object. It creates a copy of the DataFrame and drops the '1', 'first_name', 'last_name', and 'level_0' columns. 

convert_product_weights: This method takes a database_extractor object as input and performs cleaning operations on product weights. It extracts product data from an S3 bucket using the extract_from_s3 method of the database_extractor object. It drops rows with NaN values in the 'weight' column and creates a copy of the DataFrame. It converts weights expressed in milliliters ('ml') to kilograms ('kg') and weights expressed in grams ('g') to kilograms ('kg'). 

clean_products_data: This method takes a DataFrame (df) as input and performs cleaning operations on products data. It checks for duplicated rows in the DataFrame, checks for null values in the DataFrame, and removes rows where the 'weight' column has a length greater than 9 characters. 

clean_datetime: This method takes a DataFrame (df) as input and performs cleaning operations on datetime data. It checks for duplicated rows in the DataFrame, checks for null values in the DataFrame, removes duplicate rows, and removes rows where the 'month' column has a length greater than 2 characters. 

<h1> Code functionality </h1>
In the main.py file, we demonstrate the usage of the DatabaseConnector, DataCleaning, and DataExtractor classes to retrieve store details, perform data cleaning, and upload the cleaned data to a database. 
The necessary modules and classes are imported: database_utils, data_cleaning, and data_extraction.

Instances of the DatabaseConnector, DataCleaning, and DataExtractor classes are created: db_connector, data_cleaner, and database_extractor.

The header dictionary is defined, which contains the required API header information.

A store number and the corresponding URL for retrieving store details are defined.

The retrieve_stores_data method of the DataExtractor class is called, passing the URL, header, and store number as arguments. This retrieves the store details as a DataFrame and assigns it to the variable df.

The clean_store_data method of the DataCleaning class is called, passing the database_extractor instance as an argument. This performs data cleaning operations on the df DataFrame, and the cleaned DataFrame is assigned to the variable cleaned.

The upload_to_db method of the DatabaseConnector class is called, passing the cleaned DataFrame and the table name ('dim_store_details') as arguments. This uploads the cleaned DataFrame to the specified database table.
