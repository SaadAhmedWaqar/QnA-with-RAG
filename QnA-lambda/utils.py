import os
import json 
import boto3 

from opensearchpy import OpenSearch
from botocore.exceptions import ClientError


def get_secret():
    
    secret_name = os.environ.get('secret_name')
    region_name = os.environ.get('region_name')
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

opensearch_url = creds['opensearch_url']
opensearch_user_name = creds['opensearch_user_name']
opensearch_password = creds['opensearch_password']
jumpstart_endpoint = creds ['jumpstart_endpoint']

modelId =  "anthropic.claude-v2"
contentType = "application/json"
accept =  "application/json"
index_name = "aws-whitepapers"

# modelId = os.environ.get('modelId')
# contentType = os.environ.get('contentType')
# accept = os.environ.get('accept')
# index_name = os.environ.get('index_name')


print (f'model {modelId}, content {contentType}, acc {accept}, index {index_name}')




bedrock_runtime = boto3.client(
service_name = 'bedrock-runtime',
region_name = 'us-east-1'
)


opensearch_client = boto3.client('opensearch')

port = 443
auth = (opensearch_user_name, opensearch_password)
opensearch_client = OpenSearch(
hosts = [{'host': opensearch_url, 'port': port}],
http_auth = auth,
use_ssl = True,
verify_certs = True 
) 


def query_endpoint_with_json_payload(encoded_json):
    sagemaker_client = boto3.client('runtime.sagemaker')
    response = sagemaker_client.invoke_endpoint(EndpointName=jumpstart_endpoint, ContentType='application/json', Body=encoded_json)
    return response


def parse_response_multiple_texts(query_response):
    model_predictions = json.loads(query_response['Body'].read())
    embeddings = model_predictions['embedding']
    return embeddings


def get_embeddings (sentence: str) -> list:
    '''Takes a string input returns its embeddings
    
    '''

    text_payload = {"text_inputs": sentence}
    query_response = query_endpoint_with_json_payload(json.dumps(text_payload).encode('utf-8'))
    embeddings = parse_response_multiple_texts(query_response)

    return embeddings[0]
    
    
def unwrap_search_result (response, knn_size, score):
    '''Unwraps the results returend by semantic search in opensearch
      and appends all the returned text objects in a string 
    
    
    '''

    result = ''

    invalid = 'answer not found in text, please ask a valid question from the docs'
    
    for i in range (knn_size):
        result += response['hits']['hits'][i]['_source']['doc_text']
        if (response['hits']['hits'][i]['_score'] < score):
            return invalid

    return result

    
    
def opensearch_search (question : str, knn_neighbours : int , score : float)  -> str: 
    ''' Takes a question and performs knn search on it's embeddings on opensearch.
        Returns the question and answer

        param question: Question/Query to be searched on opensearch
        type question: string

        param knn_neighbours: number of most relavant searches to capture
        type knn_neighbours: integer

    '''
    # index_name = os.environ.get('index_name')
    
    question_embeddings = get_embeddings(question) 

    query = {
    "size": knn_neighbours,
    "query": {
        "knn": {
        "doc_embeddings": {
            "vector": question_embeddings,
            "k": knn_neighbours}
              }
             }
            }

    response = opensearch_client.search(
        body = query,
        index = index_name
    )

    answer = unwrap_search_result(response,knn_neighbours, score)


    return answer

    
def bedrock_response (prompt:str , user_question: str, opensearch_result: str) -> str:
    

    payload = prompt + user_question + opensearch_result
    body = json.dumps({ 
                "prompt": f"Human: {payload} Assistant: ", 
                "max_tokens_to_sample" : 350,
                "temperature": 0.5,
                "top_k": 250,
                "top_p": 1,
                "stop_sequences":["\\n\\nHuman:"],
                "anthropic_version":"bedrock-2023-05-31"    
    })


    response = bedrock_runtime.invoke_model(
        body=body,
        modelId=modelId,
        accept = accept,
        contentType  = contentType
    )



    answer = json.loads (response.get('body').read())
    completion = answer.get('completion')
    #print (completion)
    return completion