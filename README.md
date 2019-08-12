# Data Engineering Projects

This is a repository of my Data Engineering portfolio, which includes _current_ Udacity nanodegree projects and (future) personal projects. 


  
## Projects:

**Topic**: [Data Modeling](./projects/DataModeling/)

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
**Topic**: Cloud Warehousing

Projects for this topic:  
1. [Project_Data_Warehouse](./projects/CloudWarehousing/Project_DataWarehouse).  
_Summary:_ Create an ETL pipeline to extract JSON files in `S3`, stages them in `Redshift`, and transforms data into fact and dimensional tables, optimized for queries.  

    - Build fact and dimension tables for the star schema in Redshift
    - Create an ETL pipeline; load data from S3 into staging  tables on Redshift
    - Define SQL statements and perform queries