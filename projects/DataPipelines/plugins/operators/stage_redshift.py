from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    copy_sql = """COPY {}
                        FROM '{}'
                        ACCESS_KEY_ID '{}'
                        SECRET_ACCESS_KEY '{}'
                        REGION '{}' 
                        TIMEFORMAT as 'epochmillisecs'
                        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                        FORMAT AS {} '{}'
                    """
   
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 file_type="JSON",
                 json_dest="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id= aws_credentials_id
        self.table = table
        self.s3_bucket= s3_bucket
        self.s3_key= s3_key
        self.region= region
        self.file_type=file_type
        self.json_dest=json_dest
        self.execution_date= kwargs.get('execution_date')
        
    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        
        if self.execution_date:
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            rendered_key= '/'.join([str(year), str(month), str(day)])
            s3_path = "s3://{}/{}/{}".format(self.s3_bucket, rendered_key, self.s3_key.format(**context))
            self.log.info('location is {}'.format(s3_path))
        
        else:    
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            self.log.info('location is {}'.format(s3_path))

        staging= StageToRedshiftOperator.copy_sql.format(
                                    self.table,
                                    s3_path,
                                    credentials.access_key,
                                    credentials.secret_key,
                                    self.region,
                                    self.file_type,
                                    self.json_dest, 
                                    self.execution_date
                                    )
                     
        redshift_hook.run(staging)




