import io
import sys
import json
import boto3
import PyPDF2

from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from langchain.text_splitter import  RecursiveCharacterTextSplitter


args = getResolvedOptions(
sys.argv, 
['WORKFLOW_NAME',
'WORKFLOW_RUN_ID',
'bucket_name',
'secret_name',
'region_name']
)


def get_secret():
    
    secret_name = args['secret_name']
    region_name = args['region_name']
    print (secret_name,region_name)

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    
    return secret

creds = get_secret()
creds = json.loads(creds)
jumpstart_endpoint = creds ['jumpstart_endpoint']




def chunk_pdf(pdf_bytes):
    ''' Gets a pdf object, converts it to text and returns its chunks
        
    '''
    # Read the PDF content
    pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    txt_content = '\n'.join(page.extract_text() for page in pdf_reader.pages)


    # txt_content = pdf_bytes.decode('utf-8')
    text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=2000,
    chunk_overlap=250,
    )
    docs = text_splitter.split_text(txt_content) 
    #print (len(docs))

    return docs

def chunk_txt(txt_bytes):

    txt_content = txt_bytes.decode('utf-8')
    text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=2000,
    chunk_overlap=250,
    )
    docs = text_splitter.split_text(txt_content)

    print (len(docs))

    return docs



def create_index(index_name,index_body, opensearch_client):
    '''Creates an index on opensearc
    
    '''

    try:
        response = opensearch_client.indices.create(index_name, body=index_body)
        # print (response)
        print(json.dumps(response, indent=2))

    except Exception as ex:
    
        if 'resource_already_exists_exception' in str(ex):
            print(f"The index '{index_name}' already exists.")
        else:
            print(f'Error: {ex}')

    return None


def query_endpoint_with_json_payload(encoded_json):
    sagemaker_client = boto3.client('runtime.sagemaker')
    response = sagemaker_client.invoke_endpoint(EndpointName=jumpstart_endpoint, ContentType='application/json', Body=encoded_json)
    return response


def parse_response_multiple_texts(query_response):
    model_predictions = json.loads(query_response['Body'].read())
    embeddings = model_predictions['embedding']
    return embeddings



def process_document(chuncked_text):
    '''Takes a chunked doc returned from doc_chunking and returns its embeddings,
       text and metadata

    
    '''
    text_payload = {"text_inputs": f"{chuncked_text}"}
    query_response = query_endpoint_with_json_payload(json.dumps(text_payload).encode('utf-8'))
    embeddings = parse_response_multiple_texts(query_response)

    return {
        'text': chuncked_text,
        'embedding': embeddings,
        #'metadata': doc.metadata
    }

def get_embeddings (sentence: str) -> list:
    '''Takes a string input returns its embeddings
    
    '''

    text_payload = {"text_inputs": sentence}
    query_response = query_endpoint_with_json_payload(json.dumps(text_payload).encode('utf-8'))
    embeddings = parse_response_multiple_texts(query_response)


    return embeddings[0]


def populate_opensearch (chunked_doc,index_name,opensearch_client,bucket_name,key,inside_zip = None):
    '''Takes in chunked docs and pushes them  to opensearch
    
    
    '''
    meta_data = 's3://'+ bucket_name + '/'  + key
    if key.endswith('.zip'):
        meta_data += '_file_inside:' + inside_zip

    # bucket_name key
    for text in chunked_doc:
        payload = process_document(text)
        #print (payload['metadata'])
        opensearch_client.index(index=index_name,body={"vector_field": payload ['embedding'][0], "text_field": payload['text'],"metadata": {"source":meta_data} })      
    
    return None






