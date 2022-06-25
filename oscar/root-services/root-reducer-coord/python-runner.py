#!/usr/bin/env python3
# Receive as input a file a serialized list containing the names of
# the parts/files to reduce.

import sys
import cloudpickle
from minio import Minio
import ROOT
import urllib3


def get_part(mc, bucket_name, object_name):
    '''
    Fectch partial result from MINIO and return it as an object.
    '''
    response = mc.get_object(bucket_name, object_name, )
    response_bytes = response.data
    return cloudpickle.loads(reducer_bytes)

mc = Minio(endpoint=sys.argv[3][8:],
           access_key=sys.argv[4],
           secret_key=sys.argv[5],
           secure=False,
           http_client=urllib3.ProxyManager(
              sys.argv[3],
               cert_reqs='CERT_NONE'
           )
)

bucket_name = sys.argv[6].split('/')[0]

# Get reducer from bucket.
reducer_response = mc.get_object(bucket_name, 'functions/reducer')
reducer_bytes = reducer_response.data
reducer_response.release_conn()
reducer = cloudpickle.loads(reducer_bytes)
print(f'Reducer: {reducer}')

# Read reduction list from input_file
input_file = open(sys.argv[1], 'rb')
parts = cloudpickle.load(input_file)

# Extract one part.
result_name = ''
tmp = parts.pop()
result_name = tmp.split('/')[1]
#part_0 = get_part(mc, bucket_name, tmp)

reducer_response = mc.get_object(bucket_name, tmp)
reducer_bytes = reducer_response.data
reducer_response.release_conn()
part_0 = cloudpickle.loads(reducer_bytes)


# Reduce with the remaining parts of the job.
for part_name in parts:
    print(part_name)
    result_name += part_name.split('/')[1]
    #tmp_part = get_part(mc, bucket_name, part_name)
    reducer_response = mc.get_object(bucket_name, part_name)
    reducer_bytes = reducer_response.data
    reducer_response.release_conn()
    tmp_part = cloudpickle.loads(reducer_bytes)
    part_0 = reducer(part_0, tmp_part)

# Write result
result_bytes = cloudpickle.dumps(part_0)
f = open(f'{sys.argv[2]}/partial-results/{result_name}', 'wb')
f.write(result_bytes)
f.close()

print(f'Reduction {result_name} written to bucket.')
