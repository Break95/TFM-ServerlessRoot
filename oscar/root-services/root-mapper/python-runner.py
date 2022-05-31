import sys
import cloudpickle
import ROOT
import json
from minio import Minio
from minio.commonconfig import Tags
import io

'''
The receive file contains two bynary objects:
    - ranges
'''
rang = None
#try:
f = open(sys.argv[1], 'rb')
rang = cloudpickle.load(f)
f.close()

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

# Update Tag.
# Get object name that partially matches
#target_job = ''
#for obj in mc.list_objects('root-oscar', 'reducer-jobs', recursive=True):
#    tmp = obj.object_name.split('/')[1].split('-') # Maybe this generates problems in the future.
#    print(tmp)
#    if file_name == tmp[0] or file_name == tmp[1]:
#        target_job = obj.object_name.split('/')[1]

#if target_job == '':
#    print('We shouldnt be here.')
#    sys.exit(-1)

#tags = Tags.new_object_tags()
#tags[file_name] = '1'
#mc.set_object_tags('root-oscar', f'reducer-jobs/{target_job}', tags)

result_bytes = cloudpickle.dumps(result)
f = open(f'{sys.argv[2]}/partial-results/{file_name}', 'wb')
f.write(result_bytes)
f.close()

print('Result written to Bucket.')
