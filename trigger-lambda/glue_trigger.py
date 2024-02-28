import json
import boto3 

glue_workflow = 'opensearch-load-qns'
glue_client = boto3.client('glue')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
   
    key = event['Records'][0]['s3']['object']['key']           
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    print (f'key: {key}, bucket: {bucket}')
    

    glue_client.update_workflow( Name=glue_workflow, DefaultRunProperties={ 'key': key } )
    response = glue_client.start_workflow_run(Name=glue_workflow)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



