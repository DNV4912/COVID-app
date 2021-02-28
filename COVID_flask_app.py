import flask
from flask import request, jsonify
import requests
import json
import pandas as pd
import data_pull_and_store as t
import atexit

t.init()

def OnExitApp(table_name):
    t.deleteRecord(table_name)

atexit.register(OnExitApp,"continents")
atexit.register(OnExitApp,"countries")     # deletes the table when the server closes

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    
    return '''<h1>COVID DATA </h1>
<p>A prototype API for fetching COVID data at a Continent and Country level.</p>'''

@app.route('/api/continents/all', methods=['GET'])
def api_all_continents():
     
    recs = t.select_all_continents()

    return (jsonify(recs))

@app.route('/api/continents/<name>', methods=['GET'])
def api_continents_specific(name):
   
    name =name[0].upper()+name[1:].lower()   #Converts the continent name to the required format
    print(name)
    recs = t.select_sp_continents( name )
  

    return (jsonify(recs))




app.run()