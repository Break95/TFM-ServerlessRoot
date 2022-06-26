#!/usr/bin/env python3
# Utility to wipe all minio data except root-common bucke.t
# Some times the client code doesnt reach the point of performing
# the cleanup due to different reasons and traces are left MINIO.
# This utility wipes that 'remainig' data.

from minio import Minio
import os
import urllib3
from minio.deleteobjects import DeleteObject

# Stablish MINIO connection.
mc = Minio(endpoint=os.environ['minio_endpoint'],
           access_key=os.envrion['minio_access'],
           secret_key=os.envrion['minio_secret'],
           secure=False,
           http_client=urllib3.ProxyManager(
               f'https://{os.environ["minio_endpoint"]}',
               cert_reqs='CERT_NONE'
           ))

#
for bucket in mc.list_buckets():
    bucket_name = bucket.name

    # Skip ROOT shared data.
    if bucket_name == 'root-common':
        continue

    # Delte Files
    delete_obj_list = map(lambda x: DeleteObject(x.object_name),
                          mc.list_objects(bucket_name,
                                          recursive=True))

    errors = mc.remove_objects(bucket_name, delete_obj_list)

    for error in errors:
        print(error)

    # Delete bucket
    try:
        mc.remove_bucket(bucket_name)
        print(f'{bucket_name} deleted.')
    except Exception as e:
        print('Couldn\'t delete bucket.')
        raise e
