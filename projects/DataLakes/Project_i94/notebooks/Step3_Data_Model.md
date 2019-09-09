#### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
- The model consists of 2 dimension tables and 1 fact table. 
    - Dimension table #1 - select columns from immigration dataset: `i94_year`,`i94_month`,`i94_arrival_date`, `i94_age`,`i94_country_code`,`i94_state_code`, `i94_port`, `visatype`, `i94_purpose`
    - Dimension table #2 - select columns from demographic dataset: `city`,`state_code`,`age_median`,`foreign_born`, `total_population`, `race`
    - Fact table - joined columns from the two datasets: `year`,`month`,`arrival_date`, `visitor_age`,`country_origin`,`arrival_location`,`visa_type`,`visit_purpose`,`age_in_city`,`foreign_born_in_city`,`city_population`,`race_in_city`

**Note:** columns definitions described in the [notebook](./Step4_ETL.ipynb)

#### 3.2 Mapping Out Data Pipelines
- We will use an ELT process with SPARK (_see figure below_)

![local](../images/local.png)

Steps to pipeline the datasets into this model:
1. Extract data (SAS & CSV) from local disk
2. Start a Spark session and import libraries   
3.  Transform datasets using [etl.py](./etl.py) and [utility_functions.py](./utility_functions.py) (details in [Notebook](./Step4_ETL.ipynb))   
4. Create dimension tables; write parquet files partitioned by `state_code`
5. Create fact table; write a parquet file partitioned by `state_code`
