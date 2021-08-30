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
#        execute_query_ddl    #
###############################    
def execute_query_ddl(query, database, s3_output):
    
    client = boto3.client('athena')
    respons = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
            },    
        ResultConfiguration={
            'OutputLocation': s3_output,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        },
        WorkGroup=WorkGroup
    )

    status = client.get_query_execution(QueryExecutionId=respons['QueryExecutionId'])    
    query_status = status['QueryExecution']['Status']['State']

    while query_status == 'RUNNING' or query_status == 'QUEUED':
        time.sleep(1)
        status = client.get_query_execution(QueryExecutionId=respons['QueryExecutionId'])
        query_status = status['QueryExecution']['Status']['State']
        
    if query_status == 'FAILED':
        raise Exception('Query status: FAILED')

    print('Query execution completed successfully')
    
    return respons