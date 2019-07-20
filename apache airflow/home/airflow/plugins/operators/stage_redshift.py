from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields = ('s3_key', )

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_name = '',
                 create_sql = '',
                 s3_bucket = '',
                 s3_key = '',
                 delimiter = '',
                 headers = 1,
                 quote_char = '',
                 jsonPath = '',
                 file_type = 'csv',
                 aws_credentials = {},
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.headers = headers
        self.quote_char = quote_char
        self.jsonPath = jsonPath
        self.file_type = file_type
        self.aws_credentials = aws_credentials

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Create stage table %s' % self.table_name)
        redshift.run(self.create_sql)
        self.log.info('Emptying stage table %s' % self.table_name)
        redshift.run('DELETE FROM %s' % self.table_name)

        s3_path = 's3://%s/%s' % (self.s3_bucket, self.s3_key)

        print(s3_path)

        copy_statement = """
                    copy %s 
                    from '%s'
                    access_key_id '%s'
                    secret_access_key '%s'
                    json '%s'
                """ % (self.table_name, s3_path, self.aws_credentials.get('key'), self.aws_credentials.get('secret'), self.jsonPath)

        if self.file_type == 'csv':
            file_statement = """
                        delimiter '%s'
                        ignoreheader %s
                        csv quote as '%s';
                    """ % (self.delimiter, self.headers, self.quote_char)

        if self.file_type == 'json':
            file_statement = "json '%s';" % (self.jsonPath)
       
        full_copy_statement = '%s' % (copy_statement) #, file_statement)

        self.log.info('Starting to copy data from S3')

        redshift.run(full_copy_statement)

        self.log.info('Staging done!')