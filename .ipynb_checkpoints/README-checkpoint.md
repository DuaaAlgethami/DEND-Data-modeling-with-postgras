# Data Modeling with Postgres
Udacity Data Engineer Nanodegree project
    

### About Project
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.
    
They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project
    
My role is to create a postgres database schema and ETL pipeline for this analysis.
    
### Project Template
1. *test.ipynb* displays the first few rows of each table to let me check the database.
2. *create_tables.py* drops and creates all tables.
3. *etl.ipynb* reads and processes a single file from song_data and log_data and loads the data into all tables. 
4. *etl.py* reads and processes files from song_data and log_data and loads them into all tables.
5. *sql_queries.py* contains all sql queries, and is imported into the last three files above.

### Schema for Song Play Analysis
Using the song and log datasets, I created the star schema has 1 fact table (songplays), and 4 dimension tables (users, songs, artists, time).
![Image](https://k.top4top.io/p_17474766v1.jpg)

### ETL Pipeline
- Create the sparkify database. And create all the tables.
- Parse all JSON Files (song dataset and log dataset).
- Create and insert Songs and Artists table from song dataset.
- Create and insert Users and Time table from log dataset.
 
### Excution process
1. create_tables.py, to create database and tables.
2. etl.py, to load the tables with all JSON files contents.
3. test.ipynb, to confirm the creation of all tables with the correct columns and contents.