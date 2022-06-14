#!/usr/bin/env python3
import sys
import cloudpickle
from minio import Minio
import ROOT
import urllib3

def minio_connection(endpoint, access, secret):
        # Remove path
        endpoint = endpoint[8:]

        return Minio(
                endpoint,
                access_key=access,
                secret_key=secret,
                secure = False,
                http_client=urllib3.ProxyManager( # Despite insecure request force https
                        f"https://{endpoint}",
                        cert_reqs='CERT_NONE'
                )
        )

'''
The reducer receives a dictionary with the following data:
    - partial_result: partial_1
'''
mc = minio_connection(sys.argv[3], sys.argv[4], sys.argv[5])


# Get external part from bucket.
# Perform this action in the first place to avoid as much as possible
# that the othere reducer finishes.
bucket_name = sys.argv[6].split('/')[0]

partial_1_name = sys.argv[1].split('/')[-1]
partial_2_name = ''
for obj in mc.list_objects(bucket_name, 'reducer-jobs/', recursive=True):
        tmp = obj.object_name.split('/')[1].split('-')
        if tmp[0] == partial_1_name:
                partial_2_name = tmp[1]
                break
        elif tmp[1] == partial_1_name:
                partial_2_name = tmp[0]
                break

if partial_2_name == '':
        print('Last reducer. End.')
        sys.exit(0)

print('Showing partial names')
print(partial_1_name)
print(partial_2_name)

try:
        partial_2_response = mc.get_object(bucket_name, f'partial-results/{partial_2_name}')
        partial_2_bytes = partial_2_response.data
        partial_2_response.release_conn()
        partial_2 = cloudpickle.loads(partial_2_bytes)
except:
        print('We were first to finish. Exiting.')
        sys.exit(0)


# Get reducer from bucket.
reducer_response = mc.get_object(bucket_name, 'functions/reducer')
reducer_bytes = reducer_response.data
reducer_response.release_conn()
reducer = cloudpickle.loads(reducer_bytes)
print(f'Reducer: {reducer}')


# Partial result from input file
f = open(sys.argv[1], 'rb')
partial_1 = cloudpickle.load(f)
f.close()

# Perform reduction
result = reducer(partial_1, partial_2)

# Remove reduced partial results.
#minioClient.remove_object('root-oscar', full_name)

# Generate new name
start_1 = partial_1_name.split('_')
end_2 = partial_2_name.split('_')

reduced_name = ''
if int(start_1[0]) < int(end_2[0]):
        reduced_name = f'{start_1[0]}_{end_2[1]}'
else:
        reduced_name = f'{end_2[0]}_{start_1[1]}'

print(f'Part 1 name: {partial_1_name}')
print(f'Part 2 name: {partial_2_name}')
print(f'New name:    {reduced_name}')

# Write result.
result_bytes = cloudpickle.dumps(result)
f = open(f'{sys.argv[2]}/partial-results/{reduced_name}', 'wb')
f.write(result_bytes)
f.close()

print('Result written to Bucket.')
