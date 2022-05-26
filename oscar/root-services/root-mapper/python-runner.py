import sys
import base64
import ast
import cloudpickle
import pickle
import ROOT
import requests
import json

def decode_payload(obj):
	pass

def encode_payload(obj):
	return str(base64.b64encode(cloudpickle.dumps(obj)))

'''
The mapper receives a dictionary with the following data:
    - mapper function
    - ranges
    - reduction function
    - red_index
    - reducer token
'''

with open(sys.argv[1]) as json_string: # Add try
	file_cont = json_string.read()
	#print(file_cont)
	payload_dict = ast.literal_eval(file_cont) # At this point we have something similar to AWS Lambda event dictrionary.
	print(payload_dict)
	#print(type(encoded_payload))

	# Get mapper function from payload dict.
	mapper_b64 = ast.literal_eval(payload_dict['mapper'])
	mapper_bytes = base64.b64decode(mapper_b64)
	mapper = cloudpickle.loads(mapper_bytes)
	#print(mapper)

	# Get ranges from payload dict.
	range_b64 = ast.literal_eval(payload_dict['ranges'])
	range_bytes = base64.b64decode(range_b64)
	range = cloudpickle.loads(range_bytes)

	#print(range.start)
	#print(range.end)
	result = mapper(range)


	token = payload_dict['token']
	print(token)

	# Get reduction index
	red_index = int(payload_dict['index'])
	print(f'Reduction index: {red_index}')
	if red_index != 0:
		# Call reducer
		x = requests.post('https://busy-jackson2.im.grycap.net/job/root-reduce',
			      headers = {
				      "Accept": "application/json",
				      "Authorization": 'Bearer ' + token},
			      json=json.dumps({
				      'partial_1': encode_payload(result),
				      'ranges': encode_payload(range),
				      'reducer': payload_dict['reducer'],
				      'index': red_index - 1}))

		print(x)
		# Write to log
	else:
		# Write to file
		file_name = f'{range.start}_{range.end}'
		print(f'{sys.argv[2]}/{file_name}')
		f = open(f'{sys.argv[2]}/{file_name}', "wb")
		cloudpickle.dump(result, f)
		f.close()

		# Write to log
