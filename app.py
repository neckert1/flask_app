# // READ ME //
# the scripts runs the back-end Flask server 
# it creates a localhost URL that you can access from the Python terminal
# localhost: http://127.0.0.1:5000/
# this script links returns all html and css scripts to create a friendly front-end UI

# // IMPORT PYTHON LIBRARIES //
# flask allows us to create a localhost server that renders a web page 
# request allows us to get user input from the web app
# render_template lets us return an html file for the web app
from flask import Flask, request, render_template
# sqlalchemy lets us interface with our server
# create_engine lets us create an database connection
from sqlalchemy import create_engine

# // CONNECT TO SERVER //    
# connect to SQL server
SERVER = 'ito096222.hosts.cloud.ford.com'
DATABASE = 'APTAutomatedData'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = 'APTAutomatedData_appl'
PASSWORD = 'Serverpa$$word1'
# initiate a database connection function with all the server information
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
# establish a database engine so that we can connect to the database
engine = create_engine(DATABASE_CONNECTION)
# finally, connect to the server
connection = engine.raw_connection()
# and set up a cursor so that we can easily query data with our connection
cursor = connection.cursor()

# // CREATE FLASK APP //
# initiate an instance of the Flask app
app = Flask(__name__)
# define static folder so we can use CSS style sheets
app.static_folder = 'static'

# // CREATE APP HOMEPAGE //
# create a homepage for the web app
@app.route("/")
# define homepage the homepage
def home_page():
    # render the homepage html file
    return render_template("index.html")

# // CREATE SAMPLE PAGE //
# create a page to retrieve sample_name that users enter for the web app
# once we get the sample_name, we will return a table with the associated data
@app.route("/database_retrieve", methods=["POST", "GET"])
# define how users enter in information
def database_retrieve():
    # ask users to enter a sample_name
    # use the GET method to retrieve a sample_name
    sample_name = request.form.get("sample_name")
    # return the sample_name in Flask (check the Python terminal)
    print(sample_name)
    # render the database html file 
    return render_template("retrieve_name.html")

# // CREATE DATA PAGE //
# this returns a table with all the data associated with the sample_name from user input
@app.route("/return_data", methods=["POST", "GET"])
#define how data is displayed 
def database_display():
    # initiate a cursor so that we can display data from the server
    cursor = connection.cursor()
    # uncomment the following line of code to pull data that matches the sample_name entered by the user 
    #cursor.execute(f"SELECT Label, DataFloat, DataInt, DataText, SpecimenId, SampleId, Name FROM Complete_Dataset WHERE Name = {sample_name}")
    # the following line of code returns all data from the server (not based on sample_name)
    cursor.execute("SELECT Label, DataFloat, DataInt, DataText, RelatedItemId, SpecimenId, SampleId, Name FROM Complete_Dataset")
    # define the data that you want to fetch from the server
    data = cursor.fetchall()
    # close the cursor for security purposes
    cursor.close()
    # return the html file that displays the data from the server 
    return render_template("display_data.html", samples = data)

# // CREATE DATA VISUALIZATION AND IMPORT PAGE //
# this creates a page to import and visualize data on the web app
@app.route("/datavisual")
#define the page to import data and visualize it later
def importing():
    # return the html file that displays where users can upload data and visualize it
    return render_template("import.html")

# // RUN FLASK APP //
# this code runs the flask app, if you don't include it, the flask app won't work
if __name__ == "__main__":
    # run the app, allow debug = True to continually update the web app throughout coding
    app.run(debug=True)