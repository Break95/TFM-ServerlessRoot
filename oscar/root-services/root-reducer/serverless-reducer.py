#!/usr/bin/env python3
import sys
import base64
import ast
import cloudpickle
import pickle
from minio import Minio
import ROOT
import requests
import json
from time import sleep
#print(sys.argv[1])

def encode_payload(obj):
        return str(base64.b64encode(cloudpickle.dumps(obj)))

def minio_connection(endpoint, access, secret):
        # Remove path
        endpoint = endpoint[8:]

        return Minio(
            endpoint,
            access_key=access,
            secret_key=secret)

'''
The reducer receives a dictionary with the following data:
    - reducer: reduction function
    - partial_result: partial_1
    - red_index: number of pending reductions in this branch
    - ranges
'''
print(sys.argv[1:])
with open(sys.argv[1]) as json_string:
    file_cont = json_string.read()
    payload_dict = ast.literal_eval(file_cont)
    print(payload_dict)

    # Get reduction function
    reducer_b64 = ast.literal_eval(payload_dict['reducer'])
    reducer_bytes = base64.b64decode(reducer_b64)
    reducer = cloudpickle.loads(reducer_bytes)

    # Get ranges
    range_b64 = ast.literal_eval(payload_dict['ranges'])
    range_bytes = base64.b64decode(range_b64)
    range = cloudpickle.loads(range_bytes)

    # Left partial result
    partial_1_b64 = ast.literal_eval(payload_dict['partial_1'])
    partial_1_bytes = base64.b64decode(partial_1_b64)
    partial_1 = cloudpickle.loads(partial_1_bytes)

    # Get reduction index
    red_index = int(payload_dict['index'])

    # Wait for partial result written to bucket.
    #   Perform this as last task to reduce wait time.
    minioClient = minio_connection(sys.argv[3], sys.argv[4], sys.argv[5])
    found = False
    target_start_range = range.end
    full_name = ''

    print(f'Searching for {target_start_range}')

    while not found:
        print()
        files = minioClient.list_objects('root-oscar', 'out/',
                             recursive=True,
                             start_after=f'out/{range.start}')

        print(files)

        for obj in files:
                full_name = obj.object_name
                file_name = full_name.split('/')[1].split('_')[0]

                print(target_start_range)
                print(file_name)
                if target_start_range == int(file_name):
                        found = True
                        break
        sleep(1)

    print(full_name)
    minioClient.fget_object('root-oscar', full_name, f'/tmp/{full_name}')

    # Perform reduction
    handle = open(f'/tmp/{full_name}', "rb")
    partial_2 = cloudpickle.load(handle)
    result = reducer(partial_1, partial_2)

    # Remove reduced partial result.
    minioClient.remove_object('root-oscar', full_name)

    new_end = full_name.split('/')[1].split('_')[1]
    range.end = int(new_end)
    # Decide wether to continue or stop reducing.
    if red_index != 0:
        x = requests.post(
            'https://busy-jackson2.im.grycap.net/job/' + sys.argv[7],
            headers = {
                "Accept": "application/json",
                    "Authorization": 'Bearer ' + sys.argv[6]},
            json=json.dumps({
                'partial_1': encode_payload(result),
                'ranges': encode_payload(range),
                'reducer': payload_dict['reducer'],
                'index': red_index - 1
            })
        )

        print(x)
    else:
        # Write to file
        print(new_end)
        file_name = f'{range.start}_{new_end}'
        f = open(f'{sys.argv[2]}/{file_name}', "wb")
        cloudpickle.dump(result, f)
        f.close()
