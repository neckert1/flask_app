from sqlalchemy import create_engine, select
import pandas as pd

# server information
SERVER = 'ito096222.hosts.cloud.ford.com'
DATABASE = 'APTAutomatedData'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = 'APTAutomatedData_appl'
PASSWORD = 'Serverpa$$word1'
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'

# establish a connection with the server
engine = create_engine(DATABASE_CONNECTION)
connection = engine.raw_connection()
# read and print the data table and data types
check_connection = pd.read_sql_query("SELECT * FROM [dbo].[Connection_Table]", connection)
print(check_connection)