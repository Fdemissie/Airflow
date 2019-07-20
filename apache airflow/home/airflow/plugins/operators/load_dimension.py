from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 
                 table_name='',
                 redshift_conn_id='',
                 sql_statement='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        

    def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info('Starting to load dim table %s' % self.table_name)
            self.log.info('Select statment %s' % self.sql_statement)
            insert_statement = "INSERT INTO %s %s " % (self.table_name, self.sql_statement)
            redshift.run(insert_statement)
            self.log.info('LoadDimensionOperator is implemented')
