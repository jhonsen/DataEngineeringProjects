from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql import types as T 


uppercase_string = udf(lambda s: s.upper())

def cleaning_demographic_data(sparkdf):
    ''' function returns a spark dataframe with selected columns cleaned up 
    '''
    clean_data= (sparkdf.withColumnRenamed('State Code', 'state_code')
                        .withColumn('city',uppercase_string('City'))
                        .withColumn('median_age', col('Median Age').cast('float')) 
                        .withColumnRenamed('Foreign-born','foreign_born')
                        .withColumnRenamed('Total Population', 'total_population')
                        .withColumnRenamed('Race','race')
                        .withColumnRenamed('count', 'race_count')
                ).drop_duplicates().drop_na()
    return clean_data

def cleaning_immigration_data(sparkdf):
    ''' function returns a spark dataframe with selected columns cleaned up
    '''
    clean_data = (sparkdf.withColumn("year", sparkdf['i94yr'].cast(IntegerType()))
                .withColumn("month", sparkdf['i94mon'].cast(IntegerType()))
                .withColumn('i94addr', 
                            when(sparkdf["i94addr"].isNull(), 'unspecified')
                             .otherwise(sparkdf["i94addr"]))
                .withColumn("purpose", sparkdf['i94visa'].cast(IntegerType()))
                .withColumn("citizenship", sparkdf['i94cit'].cast(IntegerType())) 
                .withColumn("age", sparkdf['i94bir'].cast(IntegerType()))
                .withColumn("count", sparkdf['count'].cast(IntegerType()))
                .withColumnRenamed('i94port','entry_port')
                .withColumnRenamed('i94addr','destination_state')
                .withColumnRenamed('visatype', 'visa_type')
                ).drop_duplicates().drop_na()
    return clean_data