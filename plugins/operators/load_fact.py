
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",        
                 append_only = False,
                 *args, **kwargs):
                    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate
            self.log.info(f"Truncate fact table {self.table}")
            postgres_hook.run(f"Truncate table {self.table}")         
        self.log.info(f"Insert data from staging tables into {self.table}")
        sql_statement = f"INSERT INTO {self.table} ({self.sql})"
        postgres_hook.run(sql_statement)
        self.log.info(f"Task {self.task_id} successful")