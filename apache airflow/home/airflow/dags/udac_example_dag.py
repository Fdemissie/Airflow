from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, SubDagOperator)
from staging_subdag import get_s3_to_redshift_dag
from dim_subdag import get_staging_to_dim_dag
from fact_subdag import get_staging_to_final_dag
import sql
from helpers import create_all_tables
from helpers import SqlQueries


AWS_KEY =''
#os.environ.get('AWS_KEY')
AWS_SECRET =''
#os.environ.get('AWS_SECRET')


   
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None, #'0 * * * *',
          catchup=False,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='staging_events',
    create_sql = create_all_tables.CREATE_STAGING_EVENTS_TABLE_SQL,
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key='log_data/',   #{0}-events.csv'.format('{{ ds }}'),
    delimiter=',',
    headers='1',
    quote_char='"',
    jsonPath='s3://udacity-dend-fwd/category_jsonpath.json',
    file_type='auto',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    }
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='staging_songs',
    redshift_conn_id='redshift',
    create_sql = create_all_tables.CREATE_STAGING_SONGS_TABLE_SQL,
    s3_bucket='udacity-dend',
    s3_key='song_data/',#{0}-events.csv'.format('{{ ds }}'),
    delimiter=',',
    headers='1',
    quote_char='"',
    jsonPath='auto',
    file_type='json',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    }
)



load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',    
        dag=dag,
        table_name='songplays',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.songplay_table_insert,       
        
 
)


load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',    
        dag=dag,
        table_name='users',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.user_table_insert,       
        
 
)




load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',    
        dag=dag,
        table_name='songs',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.song_table_insert,       
        
 
)


load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',    
        dag=dag,
        table_name='artists',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.artist_table_insert,       
        
 
)


load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',    
        dag=dag,
        table_name='time',
        redshift_conn_id='redshift',
        sql_statement=SqlQueries.time_table_insert,       
        
 
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_name='users',
    column='userid',
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table]

[load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table, load_user_dimension_table] >>  run_quality_checks


run_quality_checks >> end_operator