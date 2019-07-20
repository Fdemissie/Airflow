import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import LoadDimensionOperator
from airflow.operators.udacity_plugin import LoadFactOperator
from helpers import SqlQueries
import sql

def get_staging_to_final_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        insert_sql_stmt,
  #      origin_table,
  #      destination_table,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

 #   create_task = PostgresOperator(
 #       task_id=f"create_{table}_table",
 #       dag=dag,
 #       postgres_conn_id=redshift_conn_id,
 #      sql=create_sql_stmt
 #    )
    
    #copy_dim_task = LoadDimensionOperator(
    #    task_id=f"load_dim_{table}_from_staging",
    #    dag=dag,
     #   table=destination_table,
     #   redshift_conn_id=redshift_conn_id,
     #   aws_credentials_id=aws_credentials_id,
     #   origin_table=origin_table,
     #   destination_table=destination_table
    #)
    
    copy_fact_task = LoadFactOperator(
        task_id=f"load_{table}_from_staging",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        insert_sql_stmt=insert_sql_stmt
   #     origin_table=origin_table,
   #     destination_table=destination_table
    )

    #create_task >> copy_fact_task
    #copy_fact_task >> copy_dim_task


    return dag
