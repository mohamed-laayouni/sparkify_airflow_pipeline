from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class CreateTableOperator(BaseOperator):

    ui_color = '#80a8bd'

    @apply_defaults
    def _init_(self,
               redshift_conn_id="",
               *args,
               **kwargs):
        super(CreateTableOperator, self)._init_(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        query = open('/home/workspace/airflow/create_tables.sql', 'r').read()
        redshift.run(query)
        logging.info('Created Tables succesfully!')