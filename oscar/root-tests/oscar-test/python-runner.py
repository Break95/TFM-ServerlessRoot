import sys
import base64
import ast

#print(sys.argv[1])

with open(sys.argv[1]) as json_string:
	file_cont = json_string.read()
	#print(file_cont)
	encoded_payload = ast.literal_eval(file_cont)
	#print(encoded_payload)
	#print(type(encoded_payload))
	encoded_payload = ast.literal_eval(encoded_payload['func'])
	#print(encoded_payload)
	raw_code = base64.b64decode(encoded_payload.decode('utf-8')).decode('utf-8')
	#print(raw_code)
	exec(raw_code)
	func_payload()

