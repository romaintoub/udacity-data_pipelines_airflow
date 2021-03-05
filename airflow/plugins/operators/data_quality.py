from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    columns_sql = """
        SELECT
            column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        ;
        """
        
               
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        tables = ['users', 'artists', 'songs', 'time']
        for table in tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueEdrror(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            formatted_sql = DataQualityOperator.columns_sql.format(
                table = table
            )
            columns = redshift_hook.get_records( formatted_sql)
            
            for column in columns[0]:
                records_col = redshift_hook.get_records( f"SELECT COUNT({column}) FROM {table}")
                diff = num_records - records_col[0][0]
                if diff != 0:
                    self.log.warning(f"Column {column} from {table} table contains {diff} null value(s)")
            
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")

