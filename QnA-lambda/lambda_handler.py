import json

from constants import prompt
from utils import opensearch_search,bedrock_response



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


        # our main funcuctionality will go here
        

        question = api_resp['question'] 
        print("Received question:", question)

        os_answer = opensearch_search(question,4,0.6)
        print (os_answer)

        answer_summary = bedrock_response(prompt, question, os_answer)

        return {
        'statusCode': 200,
        'body': json.dumps(answer_summary)
  
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


    

