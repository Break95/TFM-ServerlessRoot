# Run inside EC2 instance with IAM policy with access to API Gateway
import boto3
import requests
import json
from aws_requests_auth.aws_auth import AWSRequestsAuth

#client = boto3.client('apigateway')
session  = boto3.Session()
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()


auth = AWSRequestsAuth(aws_access_key=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key,
                       aws_token=credentials.token,
                       aws_host='aq17373i5k.execute-api.us-east-1.amazonaws.com',
                       aws_region='us-east-1',
                       aws_service='execute-api')




#response = requests.post('https://aq17373i5k.execute-api.us-east-1.amazonaws.com/default/testBasicLambda',
#                         data={'from':'ec2', 'to':'lambda'},
#                         auth=auth)


response = requests.post('https://aq17373i5k.execute-api.us-east-1.amazonaws.com/default/lambda-docker',
                         data={'from':'ec2', 'to':'docker'},
                         auth=auth)


print(response.json()['body'])
