from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {table}
        {sql_query}
        ;
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query = "",
                 table = "",
                 truncate = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info( f"Delete rows {self.table} dimension table")
            redshift.run( "DELETE {}".format( self.table))
        
        self.log.info( f"Loading {self.table} dimension table")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            table = self.table,
            sql_query = self.sql_query
        )
        redshift.run( formatted_sql)

        
#     @apply_defaults
#     def __init__(self,
#                  # Define your operators params (with defaults) here
#                  # Example:
#                  # conn_id = your-connection-name
#                  *args, **kwargs):

#         super(LoadDimensionOperator, self).__init__(*args, **kwargs)
#         # Map params here
#         # Example:
#         # self.conn_id = conn_id

#     def execute(self, context):
#         self.log.info('LoadDimensionOperator not implemented yet')
