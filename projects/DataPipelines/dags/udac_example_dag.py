from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

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
          #schedule_interval='0 * * * *',
          schedule_interval=None,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
load_user_dimension_table = LoadDimensionOperator(
   task_id='Load_user_dim_table',
   dag=dag,
   redshift_conn_id='redshift',
   #table_nm='users' ,
   query_nm=SqlQueries.user_table_insert
  #  query_nm='user_table_insert'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks
#

start_operator >> load_user_dimension_table >> end_operator
#start_operator >> stage_events_to_redshift >> load_user_dimension_table >> end_operator
#start_operator >> stage_songs_to_redshift
