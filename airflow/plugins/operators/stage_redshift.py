from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '58140'
    
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{json_format}'
        {extra_params}
        ;
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 extra_params = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format
        self.extra_params = extra_params

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Delete rows from Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table = self.table,
            s3_path = s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            json_format = self.json_format,
            extra_params = self.extra_params
        )
        redshift.run(formatted_sql)



################
# from airflow.hooks.postgres_hook import PostgresHoo# from airflow.models import BaseOperar
# from airflow.utils.decorators import apply_defats

# class StageToRedshiftOperator(BaseOpetor):
#     

#     @app_defaults
#     def _nit__(self,
#                  # Define your operators params (withefaults) here
#                # Example:
#                  # redshift_conn_id=yr-connection-name
#                *args, **kwargs):

#         super(StageToRedshiftOperator, self).__it__(*args, **kwargs)
#       # Map paramhere
#         # Example:
#       # self.conn_id = conn_id

#   def execute(self, context):
#         self.log.info('StageToRedshiftOperator not implemented yet')





