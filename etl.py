import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

# Creating list of filepaths to process original event csv data files
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# Processing the files to create the data file csv that will be used for Apache Casssandra tables
full_data_rows_list = [] 
    
for f in file_path_list:

    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
                
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
print(len(full_data_rows_list))

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the 
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

# Creating a Cluster
cluster = Cluster()
session = cluster.connect()

# Create Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# Set Keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)

# Query 1: This query selects the artist, song, and length based on the sessionid and iteminsession. 
# The sessionid and iteminsession are used to make up the primary key.
session_table_drop = "DROP TABLE IF EXISTS session_library"

try:
    session.execute(session_table_drop)
except Exception as e:
    print(e)
    
query1 = "CREATE TABLE IF NOT EXISTS session_library "
query1 = query1 + "(sessionid int, iteminsession int, artist_name text, song_title text, song_length float, PRIMARY KEY(sessionid, iteminsession))"

try:
    session.execute(query1)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO session_library (sessionid, iteminsession, artist_name, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

select_query1 = "SELECT * FROM session_library WHERE sessionid = 338 AND iteminsession = 4"
try:
    rows = session.execute(select_query1)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, row.song_title, row.song_length)

# Query 2: This query selects the artist, song, and user based on the userid and sessionid. Song is also sorted by iteminsession.
# The primary key is made unique by partitioning on the userid and sessionid, and clustered by iteminsession.
user_table_drop = "DROP TABLE IF EXISTS user_library"

try:
    session.execute(user_table_drop)
except Exception as e:
    print(e)
    
query2 = "CREATE TABLE IF NOT EXISTS user_library "
query2 = query2 + "(userid int, sessionid int, iteminsession int, artist_name text, firstName text, lastName text, song_title text, song_length float, PRIMARY KEY((userid, sessionid), iteminsession))"

try:
    session.execute(query2)
except Exception as e:
    print(e)        

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_library (userid, sessionid, iteminsession, artist_name, firstName, lastName, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[-1]), int(line[8]), int(line[3]), line[0], line[1], line[4], line[9], float(line[5])))

select_query2 = "SELECT * FROM user_library WHERE userid = 10 AND sessionid = 182"

try:
    rows = session.execute(select_query2)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, row.song_title, row.firstname, row.lastname)

# Query 3: This query selects the first and last names of all users that listened to a particular song. 
# The primary key is made unique by partitioning on the userid and sessionid, and clustering on iteminsession.
song_table_drop = "DROP TABLE IF EXISTS song_library"

try:
    session.execute(song_table_drop)
except Exception as e:
    print(e)
    
query3 = "CREATE TABLE IF NOT EXISTS song_library "
query3 = query3 + "(song_title text, userId int, firstName text, lastName text, PRIMARY KEY(song_title, userId))"

try:
    session.execute(query3)
except Exception as e:
    print(e)        


with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO song_library (song_title, userId, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


select_query3 = "SELECT firstName, lastName FROM song_library WHERE song_title='All Hands Against His Own'"

try:
    rows = session.execute(select_query3)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.firstname, row.lastname)

# Drop the tables before closing out the sessions
try:
    rows = session.execute("DROP TABLE session_library")
except Exception as e:
    print(e)

try:
    rows = session.execute("DROP TABLE user_library")
except Exception as e:
    print(e)

try:
    rows = session.execute("DROP TABLE song_library")
except Exception as e:
    print(e)

# Close the session and cluster connection¶
session.shutdown()
cluster.shutdown()