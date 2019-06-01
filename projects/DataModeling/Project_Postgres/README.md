## PROJECT DESCRIPTION

1. **Project Summary**.Sparkify (a startup company) wants to understand user behavior regarding song selections. To this end, they needed a data engineer who can setup a database that can be used to perform various SQL queries on. The original datasets exist in the JSON format, which is not flexible and difficult to work with. Hence, we need to create a data model by creating star schema and build an ETL pipeline using postgres & python. This pipeline will take in those JSON files and populate fact and dimension tables. 

2. How to run the Python scripts
  
    a. `create_tables.py` creates fact and dimension tables
       - On the terminal, 
         > python create_tables.py 
       - Alternatively, in a jupyter notebook cell, 
         > !python create_tables.py  
      
        
    b. `etl.py` loads JSON (input) files and populates fact & dimension tables. It requires `sql_queries.py` to run
       - On the terminal, 
         > python etl.py 
       - Alternatively, in a jupyter notebook cell, 
         > !python etl.py  
      
      
3. Other files in the repository
   - `data`           folder containing JSON files, organized within `log_data` and `song_data` sub-directories
   - `etl.ipynb`      notebook showing the step-by-step process of creating the etl pipeline
   - `test.ipynb`     notebook showing sql queries, used to check table content and integrity
   - `sql_queries.py` contains sql commands to create tables and to insert data into tables. File is used by `etl.py` 
   

