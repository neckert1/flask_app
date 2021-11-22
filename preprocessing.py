# // READ ME //
# this script pre-processes the data from the TrendTracker Instron DB
# the goal of this script is to pull the data from tables in the DB and put all the data into one master table

# // IMPORT PYTHON LIBRARIES //
# pandas allows us to mainipulate data
import pandas as pd
# sqlalchemy lets us interface with our server
# create_engine lets us create an database connection
from sqlalchemy import create_engine
# metadata, table, and column lets us work with the data from the server 
from sqlalchemy import MetaData, Table, Column
# float, integer, and string lets us work with different data types
from sqlalchemy.sql.sqltypes import Float, Integer, String

# // CONNECT TO SERVER //
def server_info():
    # // this function allows us to connect to the server so we can work with the data from the server //
   
    # server information
    SERVER = 'ito096222.hosts.cloud.ford.com'
    DATABASE = 'APTAutomatedData' # this will change depending on what we name the Instron DB
    DRIVER = 'SQL Server Native Client 11.0'
    USERNAME = 'APTAutomatedData_appl' 
    PASSWORD = 'Serverpa$$word1'
    # initiate a database connection function with all the server information
    DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
    # establish a database engine so that we can connect to the database
    engine = create_engine(DATABASE_CONNECTION)
    # finally, connect to the server!
    connection = engine.connect()

    # return the engine and connection so we can use these later on
    return engine, connection

# // PULL DATA FROM SERVER //
def pull_data():
    # // this function allows us to pull data from the server //
    # // the data is stored in tables so we need to pull all tables //
    
    # we will need to make sure we claim that we are connecting to the server
    connection = server_info()
    
    # for each table that we pull, we need to create a dataframe (df) for the data to be stored in
    # pull the specimen data table
    df_1 = pd.read_sql_query("SELECT Specimens.Guid, Specimens.SampleId FROM Ids.Specimens", connection)
    # pull the test data table
    df_2 = pd.read_sql_query("SELECT Tests.Guid, Tests.SpecimenId FROM Ids.Tests", connection)
    # pull the data category data table
    df_3 = pd.read_sql_query("SELECT DataCategories.DataCategoryId FROM Ids.DataCategories", connection)
    # pull the data tag data table
    df_4 = pd.read_sql_query("SELECT DataTags.DataTagId, DataTags.DataCategoryId FROM Ids.DataTags", connection)
    # pull the user label data table
    df_5 = pd.read_sql_query("SELECT UserLabels.UserLabelId, Userlabels.Label FROM Ids.UserLabels", connection)
    # pull the data column data table
    df_6 = pd.read_sql_query("SELECT DataColumns.DatumColumnId, DataColumns.UserLabelId, DataColumns.DataTagId FROM Ids.DataColumns", connection)
    # pull the data from the data table
    df_7 = pd.read_sql_query("SELECT Data.ParentId, Data.ParentType, Data.DataFloat, Data.DataInt, Data.DataText, Data.DatumColumnId, Data.RelatedItemId FROM Ids.Data", connection)
    # pull the sample data table
    df_8 = pd.read_sql_query("SELECT Samples.Guid, Samples.Name FROM Ids.Samples", connection)

    # return these dataframes so we can use them later on
    return df_1, df_8, df_2, df_7

# // SET INDICIES FOR DATA //
def set_indicies():
    # // now that we've pulled data, we need to join and merge tables together //
    # // we can't merge or join tables until we define an index //
    # // the index allows us to match records to each other so we preserve record integrity //
    
    # define the dataframes that we're going to index
    df_1 = pull_data()
    df_8 = pull_data()
    df_2 = pull_data()
    df_7 = pull_data()

    # index the dataframe for specimen table
    specimen_index = df_1.set_index("Guid")
    # index the dataframe for sample table
    sample_index = df_8.set_index("Guid")
    # index the dataframe for test table
    test_index = df_2.set_index("Guid")
    # index the dataframe for the data table
    data_index = df_7.set_index("ParentId")

    # return these indices so we can use them later on
    return specimen_index, sample_index, test_index, data_index

# // JOIN TABLES BASED ON INDICIES //
def join_indicies():
    # // now that we've created indicies, we can begin to join tables together

    # define the indicies that we want to use
    data_index = set_indicies()
    test_index = set_indicies()
    specimen_index = set_indicies()
    sample_index = set_indicies()
    
    # join the data table and test table with "join"
    join_1 = data_index.join(test_index, how="inner")
    # join the data table and specimen table with "join"
    join_2 = data_index.join(specimen_index, how="inner")
    # join the data table and sample table with "join"
    join_3 = data_index.join(sample_index, how="inner")

    # return these joined tables so we can use them later on
    return join_1, join_2, join_3

# // MERGE TABLES BASED ON INDICIES //
def merge_tables():
    # // we also have to merge tables together, based on the indicies we set before

    # define the tables we want to join and the indicies we want to use
    df_5 = pull_data()
    df_6 = pull_data()
    join_1 = join_indicies()
    join_2 = join_indicies()
    join_3 = join_indicies()

    # merge two dataframes (df_5 and df_6) to get the userlabel information
    # merge on "UserLabelId"
    merge_0 = pd.merge(df_5, df_6, how="right", on=["UserLabelId", "UserLabelId"])
    # join all three tables that we joined before
    final_df = [join_1, join_2, join_3]
    # concatenate the final dataframe to align all records (just cleaning it up)
    final = pd.concat(final_df)
    # merge the merged table from above and the final table to get one final data table
    # merge on "DatumColumnId"
    final_table = pd.merge(merge_0, final, how="right", on=["DatumColumnId", "DatumColumnId"])
    # print the final table to double check that it looks good
    print(final_table)
    # export this table to a csv for a quality check
    exported_file = final_table.to_csv("final_raw_data.csv", index=False)

    # return this final table so we can use it later on
    return final_table

# // CREATE NEW TABLE IN DATABASE //
def new_table():
    # in order to use the final table from above, we need to put it into the server
    # thus, we need to create new table space
    # this function creates new table space for our final table

    # we need to connect to the server to create this table
    engine = server_info()
    # we will need to create a metadata structure to create this table
    meta = MetaData()
    
    # define the new table
    # define each column name and the data type
    test_data_results = Table("Complete_Dataset", meta,
                            Column('id', Integer, primary_key=True),
                            Column('user_label_id', Integer),
                            Column('label', String),
                            Column('datum_column_id', Integer),
                            Column('data_tag_id', Integer),
                            Column('parent_type', Integer),
                            Column('data_float', Float),
                            Column('data_int', Integer),
                            Column('data_text', String),
                            Column('related_item_id', String),
                            Column('specimen_id', String),
                            Column('sample_id', String),
                            Column('sample_name', String))
    # create the meta data table
    meta.create_all(engine)

    # return the new table so we can use it later on
    return test_data_results

# // UPDATE SQL TABLE //
def update_table():
    # we have our final table all ready 
    # we also have our new table in the server
    # now we need to put the final table into the new table in the server

    # make sure we're connected to the server
    engine = server_info()
    # call up the final table that we want to put into the server
    final_table = merge_tables()

    # export the final table to the new table that we created in the server
    final_table.to_sql("Complete_Dataset", engine, if_exists="replace")
    engine.execute("SELECT * FROM Complete_Dataset").fetchall()

    # return the final table
    return final_table

# run this code
update_table()