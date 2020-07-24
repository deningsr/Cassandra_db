
# Part I. ETL Pipeline for Pre-Processing the Files

## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

#### Import Python packages 


```python
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

#### Creating list of filepaths to process original event csv data files


```python
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
```

    /home/workspace


#### Processing the files to create the data file csv that will be used for Apache Casssandra tables


```python
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

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
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
```

    8056



```python
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821


# Part II. Complete the Apache Cassandra coding portion of your project. 

## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">

## Begin writing your Apache Cassandra code in the cells below

#### Creating a Cluster


```python
from cassandra.cluster import Cluster
cluster = Cluster()

session = cluster.connect()
```

#### Create Keyspace


```python
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

#### Set Keyspace


```python
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)
```

### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

## Create queries to ask the following three questions of the data

### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4


### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    

### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'



#### Query 1: This query selects the artist, song, and length based on the sessionid and iteminsession. The sessionid and iteminsession are used to make up the primary key.


```python
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
```


```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO session_library (sessionid, iteminsession, artist_name, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```


```python
select_query1 = "SELECT * FROM session_library WHERE sessionid = 338 AND iteminsession = 4"
try:
    rows = session.execute(select_query1)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, row.song_title, row.song_length)
```

    Faithless Music Matters (Mark Knight Dub) 495.30731201171875


#### Query 2: This query selects the artist, song, and user based on the userid and sessionid. Song is also sorted by iteminsession. The primary key is made unique by partitioning on the userid and sessionid, and clustered by iteminsession.


```python
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
```


```python
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_library (userid, sessionid, iteminsession, artist_name, firstName, lastName, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[-1]), int(line[8]), int(line[3]), line[0], line[1], line[4], line[9], float(line[5])))
```


```python
select_query2 = "SELECT * FROM user_library WHERE userid = 10 AND sessionid = 182"

try:
    rows = session.execute(select_query2)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, row.song_title, row.firstname, row.lastname)
```

    Down To The Bone Keep On Keepin' On Sylvie Cruz
    Three Drives Greece 2000 Sylvie Cruz
    Sebastien Tellier Kilometer Sylvie Cruz
    Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit) Sylvie Cruz


#### Query 3: This query selects the first and last names of all users that listened to a particular song. The primary key is made unique by partitioning on the userid and sessionid, and clustering on iteminsession.


```python
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
```


```python
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO song_library (song_title, userId, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))
```


```python
select_query3 = "SELECT firstName, lastName FROM song_library WHERE song_title='All Hands Against His Own'"

try:
    rows = session.execute(select_query3)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.firstname, row.lastname)
```

    Jacqueline Lynch
    Tegan Levine
    Sara Johnson


### Drop the tables before closing out the sessions


```python
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
```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```
