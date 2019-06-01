## Data Modeling with Cassandra

### Project Summary

1. Sparkify (a startup company) wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. So, they need a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.

2. Objectives:

   - [x] Build a data model for Apache Cassandra, resulting in several functional tables to perform different queries
   - [x] Create an ETL pipeline that takes in csv files in folders, aggregates them together, and outputs the data into Apache Cassandra tables

   

#### Files in repository:

- `Project_notebook.ipynb` consists:
  - Part I. Pre-processing input files and building an ETL Pipeline 
  - Part II. Building Apache Cassandra tables for music app
- `Event_data` - folder containing csv input files describing `artist`, `firstName`,`lastName`,`gender`,`iteminSession`,`duration`,`level`,`location`,`method`,`method`,`page`,`registration`, `sessionId`,`song`,`status`,`ts`,`userId`  

  