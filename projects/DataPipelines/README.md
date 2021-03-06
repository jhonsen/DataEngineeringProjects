# Designing Data Pipelines with Airflow  

## Project Summary 
1. A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
    - We need to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills  
    - We also need to consider data quality for downstream analyses and tests 

### Project Goals  
- [ ] Configure appropriate DAGs
- [ ] Stage JSON Data from `S3` to `Redshift`
- [ ] Load fact and dimension tables using operators
- [ ] Perform Data Quality checks

![goal](./images/goal.png)  
  
### Info about datasets

1. **Song_Dataset**: This is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID, e.g., `song_data/A/B/C/TRABCEI128F424C983.json`.

2. **Log_Dataset**: This dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. Example file path for a json: `log_data/2018/11/2018-11-13-events.json`.

3. Source of raw data is stored in S3:
    - s3://udacity-dend/song_data
    - s3://udacity-dend/log_data