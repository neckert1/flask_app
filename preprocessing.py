from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import Float, Integer, String
from werkzeug.utils import header_property

# ----- connecting to the server -----
# server information
SERVER = 'ito096222.hosts.cloud.ford.com'
DATABASE = 'APTAutomatedData'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = 'APTAutomatedData_appl'
PASSWORD = 'Serverpa$$word1'
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
# establish a connection with the server
engine = create_engine(DATABASE_CONNECTION)
connection = engine.connect()

# ----- pull the data we want from the server -----
def pull_data():
    # specimen data
    df_1 = pd.read_sql_query("SELECT Specimens.Guid, Specimens.SampleId FROM Ids.Specimens", connection)
    # test data
    df_2 = pd.read_sql_query("SELECT Tests.Guid, Tests.SpecimenId FROM Ids.Tests", connection)
    # data category data
    df_3 = pd.read_sql_query("SELECT DataCategories.DataCategoryId FROM Ids.DataCategories", connection)
    # data tag data
    df_4 = pd.read_sql_query("SELECT DataTags.DataTagId, DataTags.DataCategoryId FROM Ids.DataTags", connection)
    # user label data
    df_5 = pd.read_sql_query("SELECT UserLabels.UserLabelId, Userlabels.Label FROM Ids.UserLabels", connection)
    # data column data
    df_6 = pd.read_sql_query("SELECT DataColumns.DatumColumnId, DataColumns.UserLabelId, DataColumns.DataTagId FROM Ids.DataColumns", connection)
    # data data :)
    df_7 = pd.read_sql_query("SELECT Data.ParentId, Data.ParentType, Data.DataFloat, Data.DataInt, Data.DataText, Data.DatumColumnId, Data.RelatedItemId FROM Ids.Data", connection)
    # sample data
    df_8 = pd.read_sql_query("SELECT Samples.Guid, Samples.Name FROM Ids.Samples", connection)

    return df_1, df_8, df_2, df_7

# ----- set indicies for this data -----
def set_indicies():
    df_1 = pull_data()
    df_8 = pull_data()
    df_2 = pull_data()
    df_7 = pull_data()
    # specimen index
    specimen_index = df_1.set_index("Guid")
    # sample index
    sample_index = df_8.set_index("Guid")
    # test index
    test_index = df_2.set_index("Guid")
    # data index for joining tables
    data_index = df_7.set_index("ParentId")

    return specimen_index, sample_index, test_index, data_index

# ----- joining tables to ParentId index -----
def join_indicies():
    data_index = set_indicies()
    test_index = set_indicies()
    specimen_index = set_indicies()
    sample_index = set_indicies()
    # data -> tests
    join_1 = data_index.join(test_index, how="inner")
    # data -> specimens
    join_2 = data_index.join(specimen_index, how="inner")
    # data -> samples
    join_3 = data_index.join(sample_index, how="inner")

    return join_1, join_2, join_3

# ----- merging information together -----
def merge_tables():
    df_5 = pull_data()
    df_6 = pull_data()
    join_1 = join_indicies()
    join_2 = join_indicies()
    join_3 = join_indicies()
    # merge to get Label into table
    merge_0 = pd.merge(df_5, df_6, how="right", on=["UserLabelId", "UserLabelId"])
    # joining tables to DatumColumnId index
    final_df = [join_1, join_2, join_3]
    final = pd.concat(final_df)
    # merge the Label to the Final Table
    final_table = pd.merge(merge_0, final, how="right", on=["DatumColumnId", "DatumColumnId"])
    print(final_table)
    exported_file = final_table.to_csv("final_raw_data.csv", index=False)

    return final_table

# ----- create a new table in the database -----
def new_table():
    meta = MetaData()

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

    meta.create_all(engine)

    return test_data_results

# ----- update sql table -----
def update_table():
    final_table = merge_tables()
    final_table.to_sql("Complete_Dataset", engine, if_exists="replace")
    engine.execute("SELECT * FROM Complete_Dataset").fetchall()

    return final_table

update_table()