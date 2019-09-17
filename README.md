# Data Engineering Projects

This is a repository of my Data Engineering portfolio, which includes Udacity nanodegree projects and other personal interests.   


  
## Projects:

**Topic**: [Data Modeling](./projects/DataModeling/)
  
<img src="../images/modeling.png" alt="datamodel" title="model" width="480" align="center" class="center" style="padding-left: 15px" /> <br />

Projects for this topic:
1. [Project_postgres](./projects/DataModeling/Project_Postgres).   
_Summary:_ Create an ETL pipeline to feed JSON files into tables in a local PostgreSQL database.  

    - Perform data modeling, i.e., build fact and dimension tables
    - Create ETL pipeline from JSON files & load the files into tables
    - Perform queries to check for table integrity
  
2. [Project_cassandra](./projects/DataModeling/Project_Cassandra).  
_Summary:_ Create an ETL pipeline to feed CSV files into tables in a local NoSQL, Apache Cassandra,  database.  

    - Build fact and dimension tables and create a Cassandra database
    - Create an ETL pipeline; load CSV files into Cassandra tables
    - Perform queries to check for table integrity

---
**Topic**: [Cloud Warehousing](./projects/CloudWarehousing).

<img src="../images/Clouds.png" alt="Clouds" title="cloud" width="480" align="center" class="center" style="padding-left: 15px" /> <br />

Projects for this topic:  
1. [Project_Data_Warehouse](./projects/CloudWarehousing/Project_DataWarehouse).  
_Summary:_ Create an ETL pipeline to extract JSON files in `S3`, stages them in `Redshift`, and transforms data into fact and dimensional tables, optimized for queries.  

    - Build fact and dimension tables for the star schema in Redshift
    - Create an ETL pipeline; load data from S3 into staging  tables on Redshift
    - Define SQL statements and perform queries  

---
**Topic**: [Data Lakes with SPARK](./projects/DataLakes).

Projects for this topic:  
1. [Project_Data_Lake](./projects/DataLakes/Project_SPARK).  
_Summary:_ Create an ETL pipeline to extract (JSON) data in `S3`, processes them using SPARK, and loads the data back into S3 as a set of dimensional tables, optimized for queries.  

    - Load Data from S3 & process it using SPARK
    - Deploy SPARK processes on a AWS cluster  

2. [Project_i94](./projects/DataLakes/Project_i94).
_Summary:_ Create an ETL pipeline to extract data from CSV and SAS files, process them with SPARK, and loads them into fact & dimensional tables for analytical queries.  

    - Load data in SAS & CSV formats  
    - Wrangle data and tansforms using SPARK 
    - Build fact & dimensional tables for queries   

---
**Topic**: [Data Pipelines](./projects/DataPipelines).

Projects for this topic:  
1. [Project_Pipeline](./projects/DataPipelines).  
_Summary:_ Create ETL pipelines with Apache Airflow. The pipelines should be dynamic, built from reusable tasks, monitored and able to backfill. The pipelines will include (JSON) data ingestion from `S3`, processing via `Redshift`.   
