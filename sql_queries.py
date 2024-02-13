#!/usr/bin/env python
# coding: utf-8

# In[8]:


import mysql.connector
import os
import glob
import pandas as pd

DATABASE = 'sparkify'
conn = None
global cur

def create_database():
    global cur
    try:
        with mysql.connector.connect(host='localhost', user='root', password='password') as conn:
            with conn.cursor() as cur:

            # Use the correct syntax for dropping a database
                cur.execute(f"SHOW DATABASES")
                databases = cur.fetchall()
                for db in databases:
                    cur.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
                    # creating a database
                    cur.execute(f"CREATE DATABASE {DATABASE} DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_unicode_ci")
        # Connect to the newly created database
        conn = mysql.connector.connect(host='localhost', user='root', password='password', database=DATABASE)
        cur = conn.cursor()

        return cur, conn

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    
def drop_tables(cur, conn):
    for query in drop_table_query:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_query:
        cur.execute(query)
        conn.commit()
    
    

factsongplay_drop = "Drop table if exists factsongplay"
dimArtist_drop = "Drop table if exists dimArtist"
dimUser_drop = "Drop table if exists dimUser"
dimTime_drop = "Drop table if exists dimTime"
dimSong_drop = "Drop table if exists dimSong"

factsongplay = """Create Table factsongplay (
songplayid int auto_increment PRIMARY KEY,
starttime TIMESTAMP,
artistid varchar(255),
userid int,
sessionid int,
songid varchar(36),
location varchar(500),
useragent longtext,
level varchar(255)
)"""

dimArtist = """Create Table dimArtist(
artistid varchar(255) PRIMARY KEY,
artistname varchar(255),
artistlat decimal(9,6),
artistlong decimal(9,6),
location varchar(500))"""

dimUser = """Create Table dimUser(
userid int PRIMARY KEY,
firstname varchar(255),
lastname varchar(255),
gender varchar(10),
level varchar(255)
)"""

dimTime = """Create Table dimTime (
starttime Timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int)"""

dimSong = """Create table dimSong (
songid varchar(36) PRIMARY KEY,
title varchar(255),
duration float,
artistid varchar(255),
year int)"""

drop_table_query = [factsongplay_drop, dimArtist_drop, dimUser_drop, dimSong_drop]
create_table_query = [factsongplay, dimArtist, dimUser, dimSong, dimTime]



insert_songplay = """Insert into factsongplay (starttime, artistid, userid, sessionid,
songid, location, useragent, level) values (%s, %s, %s, %s, %s, %s, %s, %s)"""

insert_dimartist = """Insert Ignore into dimArtist (artistid, artistname, artistlat, artistlong,
location) values (%s, %s, %s, %s, %s)"""

insert_dimuser = """Insert into dimUser(userid, firstname, lastname, gender, level) values (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE level = VALUES(level)"""

insert_dimtime = """Insert IGNORE into dimTime(starttime, hour, day, week, month, year, weekday) values (%s, %s, %s, %s, %s, %s, %s)"""

insert_dimsong = """Insert IGNORE into dimSong(songid, title, duration, artistid, year) values(%s, %s, %s, %s, %s)"""

song_select = """Select s.songid, a.artistid from dimsong s inner join dimartist a on s.artistid = a.artistid where a.artistname=%s 
and s.title=%s and s.duration=%s"""

def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    
    create_tables(cur, conn)
    
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()

