from flask import Flask, request, render_template, redirect, url_for, Response
from sqlalchemy import create_engine
import pandas as pd
import time

# connect to SQL server
SERVER = 'ito096222.hosts.cloud.ford.com'
DATABASE = 'APTAutomatedData'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = 'APTAutomatedData_appl'
PASSWORD = 'Serverpa$$word1'
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'

engine = create_engine(DATABASE_CONNECTION)
connection = engine.raw_connection()
cursor = connection.cursor()

# create flask app
app = Flask(__name__)
# app.static_folder = 'static'

@app.route("/")
# define homepage
def home_page():
    return render_template("index.html")

@app.route("/database_retrieve", methods=["POST", "GET"])
# define data entry table
def database_retrieve():
    sample_name = request.form.get("sample_name")
    print(sample_name) 
    return render_template("retrieve_name.html")

@app.route("/return_data", methods=["POST", "GET"])
#define return data table
def database_display():
    cursor = connection.cursor()
    #cursor.execute(f"SELECT Label, DataFloat, DataInt, DataText, SpecimenId, SampleId, Name FROM Complete_Dataset WHERE Name = {sample_name}")
    cursor.execute("SELECT Label, DataFloat, DataInt, DataText, RelatedItemId, SpecimenId, SampleId, Name FROM Complete_Dataset")
    data = cursor.fetchall()
    cursor.close()
    return render_template("display_data.html", samples = data)

@app.route("/datavisual")
#define import page
def importing():
    return render_template("import.html")

# run flask app
if __name__ == "__main__":
    app.run(debug=True)