import requests 
import json 

# Get servie token from request
token_response = requests.get('https://busy-jackson2.im.grycap.net/system/services/tester', 
                               headers={'Accept':'application/json'}, 
                               auth=requests.auth.HTTPBasicAuth('oscar', 'Udi8oNL1xZUJdC')
                               )
tester_token = json.loads(token_response.text)['token']


requests.post('https://busy-jackson2.im.grycap.net/job/tester',
			      headers = {
				      "Accept": "application/json",
				      "Authorization": 'Bearer ' + tester_token},
			      json=json.dumps({'token': 1}))

