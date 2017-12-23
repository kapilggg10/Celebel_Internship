import sqlite3
import csv
#creating a connection with database
connection = sqlite3.connect('database_cel.db')

#drop the table named tweets if already exists
connection.execute('drop table if exists tweets')

#table creation with all column specified in csv file
connection.execute('create table tweets(tweet_id int,\
                    in_reply_to_status_id int,\
                    in_reply_to_user_id int,\
                    retweeted_status_id int,\
                    retweeted_status_user_id int,\
                    timestamp STR,\
                    source STR,\
                    text STR,\
                    expanded_urls STR)')

#file open with encoding utf-8 and newline is considered as none
file = open('tweets.csv',encoding = "UTF8",newline = '')

#using csv reader to read line by line and null bytes are stored in none
reader = csv.reader((line.replace('\0','') for line in file), delimiter=",")

#header of the csv file is stored in header
header = next(reader)
data = []

#blank is considered as Null
#looping around file and storing values in variables
for row in reader:
    tweet_id = int(float(row[0] or False))
    in_reply_to_status_id = int(float(row[1]  or False)) or None
    in_reply_to_user_id = int(float(row[2] or False)) or None
    retweeted_status_id = int(float(row[3] or False)) or None
    retweeted_status_user_id = int(float(row[4] or False)) or None
    timestamp = str(row[5])
    source = str(row[6])
    statement = str(row[7])
    expanded_urls = ','.join(row[8:])
    data.append([tweet_id, in_reply_to_status_id, in_reply_to_user_id, retweeted_status_id, retweeted_status_user_id, timestamp, source, statement, expanded_urls])
    connection.execute("insert into tweets(tweet_id, \
        in_reply_to_status_id, in_reply_to_user_id,\
        retweeted_status_id, retweeted_status_user_id,\
        timestamp, source, text, expanded_urls)\
         values(?,?,?,?,?,?,?,?,?)",[tweet_id\
                                     ,in_reply_to_status_id\
                                     ,in_reply_to_user_id\
                                     ,retweeted_status_id\
                                     ,retweeted_status_user_id\
                                     ,timestamp,source,statement,expanded_urls])
connection.commit()
connection.close()
file.close()
