from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = "udacity-dend-warehouse"
song_s3_key = "song_data"
log_s3_key = "log-data"

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTableOperator(
    task_id='Create_tables',
    redshift_conn_id="redshift",
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table_name="staging_events",
    s3_bucket=s3_bucket,
    s3_key=log_s3_key,
    format="JSON",
    dag=dag,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials="aws_credentials",
    redshift_conn_id="redshift",
    table_name="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=song_s3_key,
    format="JSON",
    dag=dag,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table_name="user_table",
    clear_data=True,
    sql_query=SqlQueries.user_table_insert,
    start_date=default_args["start_date"],
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table_name="song_table",
    clear_data=True,
    sql_query=SqlQueries.song_table_insert,
    start_date=default_args["start_date"],
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table_name="artist_table",
    clear_data=True,
    sql_query=SqlQueries.artist_table_insert,
    start_date=default_args["start_date"],
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table_name="time_table",
    clear_data=True,
    sql_query=SqlQueries.time_table_insert,
    start_date=default_args["start_date"],
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table=["user_table", "songs_table", "artist_table", "time_table"]
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependcies
Begin_execution >> [stage_events, stage_songs] >> Load_songplays_fact_table
Load_songplays_fact_table >> [Load_user_dim_table, Load_song_dim_table, Load_artist_dim_table, Load_time_dim_table] >> Run_data_quality_checks
Run_data_quality_checks >> Stop_execution