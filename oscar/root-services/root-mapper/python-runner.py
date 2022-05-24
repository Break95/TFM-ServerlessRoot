import sys
import base64
import ast
import cloudpickle
import pickle
import ROOT

#print(sys.argv[1])

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

	# Write result to file
	file_name =  str(range.start) + '_' + str(range.end) + 'jobname_id'
	#print(sys.argv[2] + '/' + str(file_name))

	f  = open(sys.argv[2] + '/' + file_name, "wb")
	cloudpickle.dump(result, f)
	f.close()

# Connect to inner storage service.


# Aync call to reducer.

# Write logs to bucket.
