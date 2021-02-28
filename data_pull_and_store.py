import pandas as pd
import requests
import json
import datetime
import sqlite3

db = sqlite3.connect('continents')
cursor = db.cursor()

def create_continents():                                        #Creates a table with the information pertaining to each continenet
    try:
    # db = sqlite3.connect('continents')
    # cursor = db.cursor()
        cursor.execute("""
 CREATE TABLE continents (
     cases integer NOT NULL,
     tests integer NOT NULL,
     deaths integer NOT NULL,
     recovered integer NOT NULL,
     population integer NOT NULL,
     continent text NOT NULL)""")

    except Exception as E:
        print('Error :', E)
    else:
        print('Continents table created')

def create_continents_countries():                             #Creates a table that stores the list of countries that were considered for each continent
    try:
        cursor.execute("""
 CREATE TABLE countries (continent text NOT NULL,
 countries text NOT NULL)""")

    except Exception as E:
        print('Error :', E)
    else:
        print('Continents countries table created')



def insert_values(table_name,no_of_columns,dataframe):         #Inserts values into the created tables
    
        
    query = "insert into "+ table_name +" values(" 
    for i in range(no_of_columns):
        query =query + "?"
        if i<no_of_columns-1:
            query = query + ","
    query = query + ")"
    
    try:
        cursor.executemany(query, dataframe)
    except Exception as E:
        print('Error : ', E)
    else:
        db.commit()
        print('data inserted')




def select_sp_continents(name,verbose=True):                                      #This function pulls the data from the DB based on what continent information the user wants
    db = sqlite3.connect('continents')
    cursor = db.cursor()
    query1 = "select * from continents where continent ='"+ name +"'"          
    query2 ="select countries from countries where continent ='"+ name +"'"
    recs = cursor.execute(query1)
    
    data =[]
    data_country =[]
    metric_names = [description[0] for description in cursor.description]
    

    if verbose:
        for row in recs:
            data.append(dict( zip(metric_names,row )))

    recs_country = cursor.execute(query2)
    

    if verbose:
        for row in recs_country:
            data_country.append(row[0])
            

    data.append({'Countries':data_country})
    return data

def select_all_continents(verbose=True):                                      #This function pulls the data from the DB based on what continent information the user wants
    db = sqlite3.connect('continents')
    cursor = db.cursor()
    query = "select * from continents"          
    recs = cursor.execute(query)
    
    data =[]
    data_country =[]
    metric_names = [description[0] for description in cursor.description]
    

    if verbose:
        for row in recs:
            data.append(dict( zip(metric_names,row )))          


    return data

def deleteRecord(table_name):
    try:
        db = sqlite3.connect('continents')
        cursor = db.cursor()   
        
        # Deleting table
        sql_delete_query = "DROP table " + table_name
        cursor.execute(sql_delete_query)
        db.commit()
        print("Record deleted successfully ")
        cursor.close()

    except Exception as E:
        print('Error : ', E)
    finally:
        if (db):
            db.close()
            print("the sqlite connection is closed")



def init() :
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name= 'continents' ''')
    #if the count is 1, then table exists
    if cursor.fetchone()[0]==1 : 
        print('Table exists.')
    else :
        print('Table does not exist.')
        url = "https://corona.lmao.ninja/v2/continents?yesterday=true&sort"
        content = requests.get(url).content
        dataset = json.loads(content)
        #create a list to append data
        dataframe=list()
        dataframe2=list()
        

        for continent in dataset:
            keys =['cases','tests','deaths','recovered','population','continent']
            data =[continent.get(key) for key in keys]
            dataframe.append(data)
            for country in continent['countries']:
                temp = [continent['continent'],country]
                dataframe2.append(temp)

        create_continents()
        create_continents_countries()
        insert_values('continents',6,dataframe)
        insert_values('countries',2,dataframe2)
        

if __name__ == "__main__":
    init()

