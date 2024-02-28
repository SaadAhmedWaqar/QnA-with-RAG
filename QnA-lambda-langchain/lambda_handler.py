import os
import json
import boto3  

from opensearchpy import OpenSearch
from langchain.vectorstores import OpenSearchVectorSearch



from langchain.embeddings import SagemakerEndpointEmbeddings
from langchain.embeddings.sagemaker_endpoint import EmbeddingsContentHandler


# AWS credentials and region
region = os.environ.get('AWS_REGION')
secret_name = os.environ['OPENSEARCH_SECRET']

# Bedrock Titan model endpoint
bedrock_runtime_client = boto3.client('bedrock-runtime', region_name=region)

# for cors
response_headers = {
    "content-type": "application/json",
    "Access-Control-Allow-Headers": "*",
    "Access-Control-Allow-Origin": "*",  # Required for CORS support to work
    "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,*",
}

def get_response(status_code, message="Success", data=None):
    return {
        "isBase64Encoded": False,
        "statusCode": status_code,
        "headers": response_headers,
        "body": json.dumps(
            {"responseCode": status_code, "message": message, "response": data}
        )
    }

 

# Helper function === START
class SagemakerEndpointEmbeddingsJumpStart(SagemakerEndpointEmbeddings):
    def embed_documents(self, texts, chunk_size = 5):
        results = []
        print(f"length of texts = {len(texts)}")
        _chunk_size = len(texts) if chunk_size > len(texts) else chunk_size
        
        for i in range(0, len(texts), _chunk_size):
            response = self._embedding_func(texts[i : i + _chunk_size])
            print(response)
            results.extend(response)
        return results

class ContentHandlerForEmbeddings(EmbeddingsContentHandler):
    """
    encode input string as utf-8 bytes, read the embeddings
    from the output
    """
    content_type = "application/json"
    accepts = "application/json"
    def transform_input(self, prompt: str, model_kwargs = {}) -> bytes:
        input_str = json.dumps({"text_inputs": prompt, **model_kwargs})
        return input_str.encode('utf-8') 

    def transform_output(self, output: bytes) -> str:
        response_json = json.loads(output.read().decode("utf-8"))
        embeddings = response_json["embedding"]
        if len(embeddings) == 1:
            return [embeddings[0]]
        return embeddings

def _create_sagemaker_embeddings(endpoint_name: str, region: str = "us-east-1") -> SagemakerEndpointEmbeddingsJumpStart:
    # create a content handler object which knows how to serialize
    # and deserialize communication with the model endpoint
    content_handler = ContentHandlerForEmbeddings()

    # read to create the Sagemaker embeddings, we are providing
    # the Sagemaker endpoint that will be used for generating the
    # embeddings to the class
    embeddings = SagemakerEndpointEmbeddingsJumpStart( 
        endpoint_name=endpoint_name,
        region_name=region, 
        content_handler=content_handler
    )
    return embeddings

#login into secret Manager 
def _get_credentials(secret_id: str, region_name: str) -> str:

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    secrets_value = json.loads(response['SecretString'])
    return secrets_value

# load OpenSearch object 
def load_vector_db_opensearch(secret_id: str,
                              region: str,
                              opensearch_domain_endpoint: str,
                              opensearch_index: str
                              ) -> OpenSearchVectorSearch:
    print(f" secret_id={secret_id}, region={region}",f"opensearch_domain_endpoint={opensearch_domain_endpoint}, opensearch_index={opensearch_index}")
    opensearch_domain_endpoint = f"https://{opensearch_domain_endpoint}"
    embeddings_model_endpoint = os.environ.get("EMBEDDING_ENDPOINT_NAME")
    print("embeddings_model_endpoint", embeddings_model_endpoint)
    creds = _get_credentials(secret_id, region)
    http_auth = (creds['opensearch_user_name'], creds['opensearch_password'])
    vector_db = OpenSearchVectorSearch(index_name=opensearch_index,
                                      embedding_function=_create_sagemaker_embeddings(embeddings_model_endpoint,
                                                                                      region),
                                      opensearch_url=opensearch_domain_endpoint,
                                      http_auth=http_auth)
    return vector_db

def lambda_handler(event, context):


    if 'body' not in event or not event['body']:
        
        return {
            'statusCode': 400,
            'body': json.dumps('Error: Request body is missing or empty')
        }

    try:
        
        api_resp = json.loads(event['body'])
        

        if 'question' not in api_resp:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: The key "question" is missing in the request body')
            }
        #### START our main funcuctionality will go here############
        question = api_resp['question'] 
        print("Received question:", question)

        try:
            print('creating os client')
            # Create langchain vector store
            os_creds_secretid_in_secrets_manager = "-".join(os.environ.get("OPENSEARCH_SECRET").split(":")[-1].split("-")[:-1])
            _vector_db = load_vector_db_opensearch(
                os_creds_secretid_in_secrets_manager,
                boto3.Session().region_name,
                os.environ.get("OPENSEARCH_DOMAIN_ENDPOINT"),
                os.environ.get("OPENSEARCH_INDEX_NAME"),
            )
            
            # Perform similarity search using langchain
            print ('starting searrach')
            docs = _vector_db.similarity_search(question, k=3, text_field='text_field')
            score_docs = _vector_db._raw_similarity_search_with_score(question, k=3, text_field='text_field')
            print (score_docs)
            print(docs)
        except Exception as e:
            return {'statusCode': 500, 'body': json.dumps({'message': f'Error performing similarity search: {str(e)}'})}
            

            # Summarize responses using Bedrock Titan
        summarized_responses = []
        modelId = 'amazon.titan-text-lite-v1'
        contentType = 'application/json'
        accept = 'application/json'
        context=''
        
        for doc in docs:
            context += doc.page_content
            # single_response_text = doc['_source']['document_text']  # Accessing document text directly from the document
            
        body = {
            "inputText": f"Question: {question}? With respect to this question, summarize the following text without adding additional information {context} Response(Summarized):",
            "textGenerationConfig": {"maxTokenCount": 4096, "stopSequences": [], "temperature": 0.1, "topP": 1}
        } 

        response = bedrock_runtime_client.invoke_model(
            modelId=modelId,
            contentType=contentType,
            accept=accept, 
            body=json.dumps(body)
        )

        response_body = json.loads(response.get('body').read())
        response_text = str(response_body['results'][0]['outputText'])
        response_text = response_text.replace("\n", "")
        reference_document = doc.metadata['source']  # Accessing reference document directly from the document
        final_response = {"response_text": response_text, "reference_document": reference_document}
        summarized_responses.append(final_response) 
        
        print (f'Response {response_text}') 

        
        return {
        'statusCode': 200,
        'body': json.dumps(final_response)
  
      }
        
    except json.JSONDecodeError as e:

        return {
            'statusCode': 400,
            'body': json.dumps(f'Error decoding JSON: {str(e)}')
        }
        
    except Exception as e:

        return {
            'statusCode': 500,
            'body': json.dumps(f'Unexpected error: {str(e)}')
        }

