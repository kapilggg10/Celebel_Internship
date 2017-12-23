import csv
from elasticsearch import Elasticsearch

#connect to localhost:9200
es = Elasticsearch()
i = 0
es.indices.delete(index = 'tweets',ignore=400)
es.indices.create(index = "tweets",ignore=400)
with open("tweets.csv",'r',encoding="UTF8") as file_:
    reader = csv.DictReader((line.replace('\0','') for line in file_))
    for each in reader:
        es.index(index="tweets",doc_type = "tweet",id = i,body=each)
        i+=1
print("data is stored into Elasticsearch")
