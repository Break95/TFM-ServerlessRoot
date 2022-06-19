#!/usr/bin/env python3
import sys
import cloudpickle
import ROOT
import urllib3
from minio import Minio
import datetime

rang = None
f = open(sys.argv[1], 'rb')
rang = cloudpickle.load(f)
f.close()

mc = Minio(endpoint=sys.argv[4][8:],
           access_key=sys.argv[5],
           secret_key=sys.argv[6],
           secure=False,
           http_client=urllib3.ProxyManager( # Despite insecure request force https
                sys.argv[4],
                cert_reqs='CERT_NONE')
)

# Bucket job id
bucket_name = sys.argv[3].split('/')[0]
print(f'Bucket Name: {bucket_name}   -  {datetime.datetime.now()}')

# Get mapper function from Bucket.
mapper_response = mc.get_object(bucket_name, 'functions/mapper')
mapper_bytes = mapper_response.data
mapper_response.release_conn()
mapper = cloudpickle.loads(mapper_bytes)
print(f'Mapper: {mapper}  -  {datetime.datetime.now()}')

result = mapper(rang)

# Write Result
file_name = f'{rang.id}_{rang.id}'
print(f'File Name: {file_name} - {datetime.datetime.now()}')


result_bytes = cloudpickle.dumps(result)
f = open(f'{sys.argv[2]}/partial-results/{file_name}', 'wb')
f.write(result_bytes)
f.close()

print(f'Result written to Bucket. - {datetime.datetime.now()}')
