from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when
from pyspark.sql import types as T 

def count_nulls(dfspark):
    '''function returns the number of nulls in each column of a spark dataframe
    Input:
        - a spark dataframe
    '''
    num_nulls=dict()
    for col in dfspark.columns:
        total = dfspark.filter(dfspark[col].isNull()).count()
        num_nulls[col]= total
    return num_nulls


uppercase_string = udf(lambda s: s.upper())

def cleaning_demographic_data(sparkdf):
    ''' function returns a spark dataframe with selected columns cleaned up

    Input: 
        - a spark dataframe

    '''
    clean_data= (sparkdf.withColumnRenamed('State Code', 'state_code')
                        .withColumn('city',uppercase_string('City'))
                        .withColumn('median_age', col('Median Age').cast(T.FloatType())) 
                        .withColumnRenamed('Foreign-born','foreign_born')
                        .withColumnRenamed('Total Population', 'total_population')
                        .withColumnRenamed('Race','race')
                        .withColumnRenamed('count', 'race_count')
                ).drop_duplicates().dropna()
    return clean_data

def cleaning_immigration_data(sparkdf):
    ''' function returns a spark dataframe with selected columns cleaned up

    Input: 
        - a spark dataframe
    
    '''
    clean_data = (sparkdf.withColumn("year", sparkdf['i94yr'].cast(T.IntegerType()))
                .withColumn("month", sparkdf['i94mon'].cast(T.IntegerType()))
                .withColumn('i94addr', 
                            when(sparkdf["i94addr"].isNull(), 'unspecified')
                             .otherwise(sparkdf["i94addr"]))
                .withColumn("purpose", sparkdf['i94visa'].cast(T.IntegerType()))
                .withColumn("citizenship", sparkdf['i94cit'].cast(T.IntegerType())) 
                .withColumn("age", sparkdf['i94bir'].cast(T.IntegerType()))
                .withColumn("count", sparkdf['count'].cast(T.IntegerType()))
                .withColumnRenamed('i94port','entry_port')
                .withColumnRenamed('i94addr','destination_state')
                .withColumnRenamed('visatype', 'visa_type')
                ).drop_duplicates().dropna()
    return clean_data

def quality_check(sparkdf, title='table'):
    '''Function returns results of null checks for each column and total rows in table

    Input:
        - title   : STRING, title of table
        - sparkdf : a spark dataframe
    '''
    sparkdf_columns = sparkdf.columns
    total = sparkdf.count()
    print('Performing quality checks for {}'.format(title))
    for colname in sparkdf_columns:
        null_values= sparkdf.filter(sparkdf[colname].isNull()).count()
        print('Number of null values for {} is: {}'.format(colname, null_values))        
    print('Total number of rows is: {}'.format(total))
