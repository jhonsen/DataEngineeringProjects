from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,    
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
   task_id='Stage_events',
   dag=dag,
   table="staging_events",
   aws_credentials_id="aws_credentials",
   redshift_conn_id="redshift",
   s3_bucket="udacity-dend",
   s3_key="log_data",
   region="us-west-2",
   file_type="JSON"
)
stage_songs_to_redshift = StageToRedshiftOperator(
   task_id='Stage_songs',
   dag=dag,
   table="staging_songs",
   aws_credentials_id="aws_credentials",
   redshift_conn_id="redshift",
   s3_bucket="udacity-dend",
   s3_key="song_data",
   region="us-west-2",
   file_type="JSON"
)

load_songplays_table = LoadFactOperator(
   task_id='Load_songplays_fact_table',
   dag=dag, 
   redshift_conn_id='redshift',
   table='songplays',
   query_sql='songplay_table_insert',
   append=False
)

load_user_dimension_table = LoadDimensionOperator(
   task_id='Load_user_dim_table',
   dag=dag,
   redshift_conn_id='redshift',
   table='users',
   query_sql='user_table_insert',
   append=False
)

load_song_dimension_table = LoadDimensionOperator(
   task_id='Load_song_dim_table',
   dag=dag,
   redshift_conn_id='redshift',
   table='songs',
   query_sql='song_table_insert',
   append=False
)

load_artist_dimension_table = LoadDimensionOperator(
   task_id='Load_artist_dim_table',
   dag=dag,
   redshift_conn_id='redshift',
   table='artists',
   query_sql='artist_table_insert',
   append=False
)

load_time_dimension_table = LoadDimensionOperator(
   task_id='Load_time_dim_table',
   dag=dag,
   redshift_conn_id='redshift',
   table='time',
   query_sql='time_table_insert',
   append=False
)

run_quality_checks = DataQualityOperator(
   task_id='Run_data_quality_checks',
   dag=dag,
   redshift_conn_id='redshift',
   tables=['songplays','users','artists','songs','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks
#

start_operator >> stage_events_to_redshift >> load_songplays_table 
start_operator >> stage_songs_to_redshift >> load_songplays_table 

load_songplays_table >> load_user_dimension_table >> run_quality_checks 
load_songplays_table >> load_song_dimension_table >> run_quality_checks 
load_songplays_table >> load_artist_dimension_table >> run_quality_checks 
load_songplays_table >> load_time_dimension_table >> run_quality_checks 

run_quality_checks >> end_operator
