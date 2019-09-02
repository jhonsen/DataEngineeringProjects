from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id= "",
                 table="",
                 query_sql= "",
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id= redshift_conn_id
        self.table= table
        self.query_sql= query_sql
        self.append=append

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            self.log.info("delete {} fact table".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Inserting from staging tables into fact table")
        fact_sql = getattr(SqlQueries, self.query_sql).format(self.table)
        redshift_hook.run(fact_sql)

