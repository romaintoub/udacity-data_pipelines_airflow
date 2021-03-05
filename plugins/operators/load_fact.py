from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {table}
        {sql_query}
        ;
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Delete rows fact table")
        redshift.run( "DELETE {}".format( self.table))
        
        self.log.info("Loading song_plays fact table")
        formatted_sql = LoadFactOperator.insert_sql.format(
            table = self.table,
            sql_query = self.sql_query
        )
        redshift.run( formatted_sql)

        
        
# class LoadFactOperator(BaseOperator):

#     ui_color = '#F98866'

#     @apply_defaults
#     def __init__(self,
#                  # Define your operators params (with defaults) here
#                  # Example:
#                  # conn_id = your-connection-name
#                  *args, **kwargs):

#         super(LoadFactOperator, self).__init__(*args, **kwargs)
#         # Map params here
#         # Example:
#         # self.conn_id = conn_id

#     def execute(self, context):
#         self.log.info('LoadFactOperator not implemented yet')
