# Creating a Data Lake using Spark 

## Project Summary

1. A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. We need to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads them back data into S3 as a set of dimensional tables. These tables allow the analytics team to find insights in what songs their users are listening to. 

### Project Goals

   - [x] Load data from S3
   - [x] Process using Spark
   - [x] Execute SQL statements that create the analytics tables from these staging tables

## Files in this repository

- `dl.cfg` - a boilerplate containing AWS credentials    
- `etl.py` -  contains instructions to read, process, and writes data using Spark


### Info about datasets

1. **Song_Dataset**: This is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID, e.g., `song_data/A/B/C/TRABCEI128F424C983.json`. Example song in a json contains the following key and value pairs:

    | key | value |  
    | :---: |  :--- |  
    | num_songs | 1 |  
    | artist_id | "ARJIE2Y1187B994AB7" |  
    | artist_latitude | null |  
    | artist_longitude | null |  
    | artist_location | "" |  
    | artist_name | "Line Renaud" |  
    | song_id | "SOUPIRU12A6D4FA1E1" |  
    | title | "Der Kleine Dompfaff" |  
    | duration | 152.92036 |  
    | year | 0 |  

2. **Log_Dataset**: This dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. Example file path for a json: `log_data/2018/11/2018-11-13-events.json`. Example event data:

    | key | value|  
    | :---: | :--- |
    | artist | "Pavement" |
    | auth | "LoggedIn" | 
    | firstName | "Sylvie" |
    | lastName | "Cruz" |
    | gender | "F" |
    | iteminSession | 0 |
    | length | 99.16036 |
    | level | "free" |
    | location | "Washington-Arlington-Alexandria" | 
    | method | "PUT" |  
    | page | "NextSong" | 
    | registration | 1.540266e+12|
    | sessionId | 345 | 
    | song | "Mercy:The Laundromat" | 
    | status | 200 | 
    | ts | 1541990258796 |
    | userAgent | "Mozilla/5.0(Windos NT 6.1;WOW64)... |  
    | userId | 10 |

5. Source of raw data is stored in S3:
    - s3://udacity-dend/song_data
    - s3://udacity-dend/log_data


### How to run these workflows:
1. Run `>> python etl.py` on the terminal

   
