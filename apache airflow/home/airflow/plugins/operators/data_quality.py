from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 
                 redshift_conn_id='',
                 table_name='',
                 column='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.column = column

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records("SELECT COUNT(*) FROM %s " % self.table_name)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. %s returned no results" % self.table_name)
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. %s contained 0 rows" % self.table_name)
        self.log.info("Data quality on table %s check passed with %s records" % (self.table_name,records[0][0]))
        
        column_records = redshift_hook.get_records("SELECT COUNT(*) FROM %s WHERE %s = NULL" % (self.table_name, self.column))
        if len(column_records[0]) > 1:
            raise ValueError("Data quality check failed. %s returned NULL value" % self.column)
        
        self.log.info('Data Quality check done')