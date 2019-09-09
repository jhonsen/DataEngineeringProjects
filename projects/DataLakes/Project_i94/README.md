# Data Engineering Capstone Project (Udacity) 

## Project Scope and Data Sources

- This project aims to build a data warehouse with US immigration and demographic data
- The end goal is to create analytical tables for various agencies that are impacted by the influx US visitors and/or immigrants. For example:  
    - Tour agencies in Hawaii may benefit from knowing, _"to which countries should they target their travel deals & promotions to?"_  
    - Hotel managements may benefit from understanding, _"which months of the year should they increase their staff size?_  

- Data Sources:  
    - I94 Immigration Data from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html)  
    - U.S. City Demographic Data from [OpenSoft](https://travel.trade.gov/research/reports/i94/historical/2016.html)  

## Description of Procedure     
- [EDA notebook](./notebooks/Step12_Exploration.ipynb) contains steps involved in   
    - Data extraction & data wrangling  
    - Quality inspection (e.g., missing values, duplicate data, etc.)
- [Data Model](./notebooks/Step3_Data_Model.md) contains a blueprint of the data model
- [ETL Pipeline](./notebooks/Step4_ETL.ipynb) describes how the data pipelines are created, involving:  
    - Quality inspection
    - Integrity check
- [Project Summary](./notebooks/Step4_Summary.md) contains a summary and future work, including:
    - Contingency plans, i.e., what if the data increases by 100x?
    - Scheduling plans, e.g., What's required for pipelines that run on a daily basis?
    - etc.

---