# // READ ME //
# this script checks a connection with the server via a dummy table created in the server
# after running this script, the terminal should read the result of an Empty DataFrame
# this demonstrates that this script is connected to the server and is pulling the result from the dummy table

# // IMPORT PYTHON LIBRARIES //
# sqlalchemy lets us interface with our server
# create_engine lets us create an database connection
from sqlalchemy import create_engine
# pandas allows us to mainipulate data
import pandas as pd

# // CONNECT TO SERVER //
def server_info():
    # // this function allows us to connect to the server and send an query to a dummy table within the server //
   
    # server information
    SERVER = 'ito096222.hosts.cloud.ford.com'
    DATABASE = 'APTAutomatedData' # this will change depending on what we name the Instron DB
    DRIVER = 'SQL Server Native Client 11.0'
    USERNAME = 'APTAutomatedData_appl' 
    PASSWORD = 'Serverpa$$word1'
    # initiate a database connection function with all the server information
    DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'

    # establish a connection with the server
    engine = create_engine(DATABASE_CONNECTION)
    connection = engine.raw_connection()
    # check connection by querying information from a dummy table in the server
    # dummy table is called "Connection_Table"
    check_connection = pd.read_sql_query("SELECT * FROM [dbo].[Connection_Table]", connection)
    # print the result of the query - this should return an empty dataframe 
    # this is only a test, once we have data in the server, we can use this function to query real data
    print(check_connection)

    # return the engine and connection so we can use these later on
    return engine, connection

server_info()