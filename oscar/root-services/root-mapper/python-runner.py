import sys
import cloudpickle
import ROOT
import json
from minio import Minio
import io

'''
The receive file contains two bynary objects:
    - ranges
'''
rang = None
#try:
f = open(sys.argv[1], 'rb')
rang = cloudpickle.load(f)
#except:
#       print('Error reading range input file.')
#      sys.exit(-1)

print(sys.argv[4][8:])
mc = Minio(endpoint=sys.argv[4][8:],
           access_key=sys.argv[5],
           secret_key=sys.argv[6])

# Bucket job id
bucket_name = sys.argv[3].split('/')[0]
print(f'Bucket Name: {bucket_name}')

# Get mapper function from Bucket.
mapper_responese = mc.get_object(bucket_name, 'functions/mapper')
mapper_bytes = mapper_responese.data
mapper = cloudpickle.loads(mapper_bytes)
print(f'Mapper: {mapper}')

result = mapper(rang)

# Write Result
is_tree_type = type(rang).__name__ == 'TreeRange'
attr_start = 'globalstart' if is_tree_type else 'start'
attr_end   = 'globalend'   if is_tree_type else 'end'

file_name = f'{getattr(rang, attr_start)}_{getattr(rang, attr_end)}'
print(f'File Name: {file_name}')

result_bytes = cloudpickle.dumps(result)
result_stream = io.BytesIO(result_bytes)
mc.put_object('root-oscar',
              f'out2/{file_name}',
              result_stream,
              length=len(result_bytes))

print('Result writte to Bucket.')
