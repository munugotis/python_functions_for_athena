import boto3
import pandas as pd
import io
import re
import time

params = {
    'region': 'eu-west-1',
    'database': 'database',
    's3_output': 'your-bucket-name',
    'path': 'temp/athena/output',
    'query': 'SELECT * FROM tablename LIMIT 10'
    'WorkGroup': 'athena workgroup name'
}

session = boto3.Session()


###############################
#        athena sql select    #
###############################    
def sql_select(query, database, temporary_location, s3_bucket, s3_prefix):
            
    client = boto3.client('athena')
    respons = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': temporary_location,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        },
        WorkGroup='ipf-sds-datascience-ddl'
    )
    status = client.get_query_execution(QueryExecutionId=respons['QueryExecutionId']) 
    file_name = status['QueryExecution']['QueryExecutionId'] + '.csv'
    query_status = status['QueryExecution']['Status']['State']

    while query_status == 'RUNNING' or query_status == 'QUEUED':
        time.sleep(1)
        status = client.get_query_execution(QueryExecutionId=respons['QueryExecutionId'])
        query_status = status['QueryExecution']['Status']['State']

        if query_status == 'FAILED':
            raise Exception('Query status: FAILED')

    s3 = boto3.resource('s3')
    respons = s3.meta.client.get_object(
        Bucket= s3_bucket,
        Key=s3_prefix + file_name,
    )

    x = pd.read_csv(respons['Body'])
    
    return(x)