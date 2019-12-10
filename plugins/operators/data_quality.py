from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        

    def execute(self, context):
        """
        Perform data quality checks on fact and dimension tables.

        Inputs:
            
            redshift_conn_id- connection to redshift cluster
            tables: tables located in redshift cluster
        Returns:
            None
        """

        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = postgres_hook.get_records("SELECT COUNT(*) FROM {}".format(table)) 
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check unsuccessful. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                self.log.error(f"{table} contains no records")
                raise ValueError(f"No records found in {table}")
            self.log.info(f"Data quality check passed successfully on {table} and contains {num_records} records")