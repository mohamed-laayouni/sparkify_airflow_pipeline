from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 clear_data=False,
                 sql_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.clear_data = clear_data
        self.sql_query = sql_query

    def execute(self, context):
        logging.info('Loading table {table_name}')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.clear_data:
            redshift_hook.run(f"DELETE FROM {self.table_name}")
        redshift_hook.run(self.sql_query)

