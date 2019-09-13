import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as T 
from pyspark.sql import SQLContext
from pyspark.sql import functions as F 

from utility_functions import cleaning_immigration_data, cleaning_demographic_data
from utility_functions import quality_check

########## CONFIG IF USING CLOUDS #############
# config = configparser.ConfigParser()
# config.read('dl.cfg')
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = (SparkSession 
            .builder 
            .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") 
            .enableHiveSupport().getOrCreate()
        )
    return spark


def process_immigration_data(spark, input_data, output_data, month='apr'):

    # get filepath to the immigration data file
    immigration_data = os.path.join(input_data,"18-83510-I94-Data-2016/i94_{}16_sub.sas7bdat")
    
    # read i94_immigration data file
    df = (spark.read.format('com.github.saurfang.sas.spark') 
            .load(immigration_data.format(month))
        )

    # clean and prep immigration data 
    df = cleaning_immigration_data(df)

    # extract columns to create dimension table
    dim_immigration_table = (df.groupBy(['year', 'month','entry_port','destination_state',
                                        'citizenship','age','purpose',
                                        'visa_type']).agg({'count':'sum'})
                                        .withColumnRenamed("sum(count)", "count")
                            )
    
    # write dim table to parquet files partitioned by destination_state
    dim_immigration_table.write.mode('append').partitionBy('destination_state').parquet(output_data+'dim_immigration.parquet')

    return dim_immigration_table

def process_demographic_data(spark, input_data, output_data):

    # get filepath to demographic data file
    demographic_data = os.path.join(input_data,"us-cities-demographics.csv")

    # read demographic data file
    df = (spark.read.format('csv')
                         .option("header","true")
                         .option("inferSchema","true")
                         .option("sep",";")
                         .load(demographic_data)
                    )
    
    # clean and prep demographic data 
    df = cleaning_demographic_data(df)

    # extract columns for dim table    
    dim_demographic_table = df.select(['state_code','city','median_age','foreign_born',
                                       'total_population','race','race_count'])
    
    # write dim demographic table to parquet files
    dim_demographic_table.write.mode('overwrite').partitionBy('state_code').parquet(output_data+'dim_demographic.parquet')

    return dim_demographic_table

def process_fact_table(spark, dim_table_1, dim_table_2):

    # create temporary views
    dim_table_1.createOrReplaceTempView("immigration_view")
    dim_table_2.createOrReplaceTempView("demographic_view")

    # Create the fact table by joining the immigration and demographic views
    fact_table = spark.sql('''
    SELECT i.year AS year,
        i.month AS month,
        i.citizenship AS origin_country,
        i.entry_port AS entry_port,
        i.visa_type AS visa_type,
        i.purpose AS visit_purpose,
        i.destination_state AS state_code,
        d.city AS city,
        d.total_population AS city_population,
        SUM(i.count) AS immigration_count
    FROM immigration_view AS i
    JOIN demographic_view AS d 
        ON (i.destination_state = d.state_code)
    ''')

    # Write fact table to parquet files partitioned by destination_state
    fact_table.write.mode("append").partitionBy("state_code").parquet(output_data+"/fact_table.parquet")

    return fact_table

def main():
    spark = create_spark_session()
    input_data = "../data/"
    output_data = "../data/"
    
    dim_immigration = process_immigration_data(spark, input_data, output_data, month='apr')    
    dim_demographic = process_demographic_data(spark, input_data, output_data)
    fact_table =process_fact_table(spark, dim_immigration, dim_demographic)
    
    quality_check(dim_immigration, title='immigration dimension table')
    quality_check(dim_demographic, title='demographic dimension table')
    quality_check(fact_table, title='fact table')

if __name__ == "__main__":
    main()
