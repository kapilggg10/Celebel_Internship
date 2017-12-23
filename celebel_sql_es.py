import sqlite3
from elasticsearch import Elasticsearch

#create connection with sql database
def create_sql_cursor(database):
    connection = sqlite3.connect(database)
    return connection.cursor()

#elasticsearch connection
def create_es_index(index_es):
    #assuming that elasticsearch is running on localhost
    es = Elasticsearch()
    es.indices.delete(index = index_es,ignore=400)
    es.indices.create(index = index_es,ignore=400)
    return es

def insert_in_es(database,index_es,table):
    
    #create initial connections
    cur = create_sql_cursor(database)
    es = create_es_index(index_es)

    #select all data from the database
    data = cur.execute("select * from {}".format(table))
    
    #collect column names from database
    field_names = [i[0] for i in cur.description]
    
    i=0
    #iterate the database
    for each in data:
        each = {field_names[i]:each[i] for i in range(len(each))}
        es.index(index = index_es,doc_type="data",id=i+1,body=each)
        i+=1
    print("Status: Data is successfully inserted into Elasticsearch")
    cur.close()

insert_in_es('database_cel.db','tweets','tweets')
