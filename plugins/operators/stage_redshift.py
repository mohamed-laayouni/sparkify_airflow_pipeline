from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.format = format
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        copy_query = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' FORMAT AS json {}".format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format
        )
        redshift.run(copy_query)


#NB: Formatting options are not set in COPY command, if any error arises, might want to look into it


