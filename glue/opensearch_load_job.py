import io
import sys
import json
import boto3
import zipfile

from opensearchpy import OpenSearch
from awsglue.utils import getResolvedOptions

from constants import KNN_INDEX
from utils import get_secret,chunk_pdf,chunk_txt,create_index,populate_opensearch


s3 = boto3.client('s3')
glue_client = boto3.client('glue')
dynamodb = boto3.client('dynamodb')


args = getResolvedOptions(
sys.argv, 
['WORKFLOW_NAME',
'WORKFLOW_RUN_ID',
'bucket_name',
'secret_name',
'region_name']
)
index_name = "aws-whitepapers"


creds = get_secret()
creds = json.loads(creds)

opensearch_url = creds['opensearch_url']
opensearch_user_name = creds['opensearch_user_name']
opensearch_password = creds['opensearch_password']
jumpstart_endpoint = creds ['jumpstart_endpoint']


opensearch_client = boto3.client('opensearch')

port = 443
auth = (opensearch_user_name, opensearch_password)
opensearch_client = OpenSearch(
hosts = [{'host': opensearch_url, 'port': port}],
http_auth = auth,
use_ssl = True,
verify_certs = True 
) 


def main ():
    try:
      

        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']
        bucket_name = args['bucket_name']
        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
        key = workflow_params['key']        
        print (f'KEY from lambda {key}')

        info = opensearch_client.info()
        print(f"Welcome to {info['version']['distribution']} {info['version']['number']}!")

        create_index(index_name,KNN_INDEX,opensearch_client)


        if key.endswith('.zip'):

            obj = s3.get_object(Bucket=bucket_name, Key=key)
            zip_bytes = obj['Body'].read()
            count = 0 

            # Create a zipfile object in memory
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_ref:
                print ('Hi we are being unzipped')

                # Iterate through each file in the zip archive
                for member in zip_ref.infolist():
                    print (member.filename)
                    path = member.filename
                    print (f'loop  count {count}')
                    count+=1

                    if path.endswith('.pdf'):
                        pdf_content = zip_ref.read(path)
                        chunked_docs = chunk_pdf(pdf_content)
                        print (f'zip pdf doc len is {len(chunked_docs)}')
                        populate_opensearch (chunked_docs,index_name,opensearch_client,bucket_name,key,path)


                    if path.endswith('.txt'):
                        txt_bytes = obj['Body'].read()
                        chunked_docs = chunk_txt(txt_bytes)
                        print (f'zip txt doc len is {len(chunked_docs)}')
                        populate_opensearch (chunked_docs,index_name,opensearch_client,bucket_name,key,path)

        if key.endswith('.pdf'):

            obj = s3.get_object(Bucket=bucket_name, Key=key)
            pdf_bytes = obj['Body'].read()
            chunked_docs = chunk_pdf(pdf_bytes)
            print (f'doc len is {len(chunked_docs)}')
            populate_opensearch (chunked_docs,index_name,opensearch_client,bucket_name,key)

        if key.endswith('.txt'):
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            txt_bytes = obj['Body'].read()
            chunked_docs = chunk_txt(txt_bytes)
            print (f'doc len is {len(chunked_docs)}')
            populate_opensearch (chunked_docs,index_name,opensearch_client,bucket_name,key)




    except Exception as error_msg :
        print (error_msg)
        
    
if __name__ == "__main__":
    main()



