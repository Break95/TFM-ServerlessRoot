#!/usr/bin/env python3
# Coordinator in charge of invoking the reduction
# processes required
import sys
from minio import Minio
import urllib3
import cloudpickle
import io
import ROOT

def create_reducer_job(mc, bucket_name, job_name, job_data):
    job_bytes = cloudpickle.dumps(job_data)
    job_stream = io.BytesIO(job_bytes)
    mc.put_object(bucket_name, f'reducer-jobs/{job_name}',
                  job_stream,
                  length = len(job_bytes))


# Read coordinator config from imput file.
with open(sys.argv[1] , 'r') as config_file:
    parts_per_red = int(config_file.readline())
    total_reductions = int(config_file.readline())

# Configure minio client
mc = Minio(endpoint=sys.argv[3][8:],
           access_key=sys.argv[4],
           secret_key=sys.argv[5],
           secure=False,
           http_client=urllib3.ProxyManager(
               sys.argv[3],
               cert_reqs='CERT_NONE'
           )
)

# Extract bucket name
bucket_name = sys.argv[2].split('/')[0]
event_count = 0
event_list = []
job_name = ''
final_reduce_names = [''] * parts_per_red

# Listen to writes to partial-results.
with mc.listen_bucket_notification(bucket_name,
                                   prefix='partial-results',
                                   events=['s3:ObjectCreated:*']) as events:
    for event in events:
        event_count += 1
        object_name = event['Records'][0]['s3']['object']['key']
        event_name = object_name.split('/')[1]
        event_list.append(object_name)
        final_reduce_names[event_count-1] = object_name
        job_name += f'{event_name}_'

        # If we have the same amount of events queued as
        # the specified number of partial results to reduce together
        # trigger the reduction.
        if event_count == parts_per_red:
            # Check if all reductions have been performed.
            # Last reduce is performed by the coordinator to simplify things and avoid
            # extra time asociated with starting up a new service.
            if total_reductions == 1:
                break

            # Trigger the reduction.
            create_reducer_job(mc, bucket_name, job_name, event_list)
            total_reductions -= 1
            
            # Reset variables
            event_count = 0
            event_list = []
            job_name = ''

# Perform last reduction.
# Load reducer function
print(final_reduce_names)
reducer_response = mc.get_object(bucket_name, 'functions/reducer')
reducer_bytes = reducer_response.data
reducer_response.release_conn()
reducer = cloudpickle.loads(reducer_bytes)
print(f'Reducer: {reducer}')

# Get first part
response = mc.get_object(bucket_name, final_reduce_names.pop() )
part_0 = cloudpickle.loads(response.data)
response.release_conn()

# Perform reduction
for part in final_reduce_names:
    part_response = mc.get_object(bucket_name, part)
    tmp_part = cloudpickle.loads(part_response.data)
    part_response.release_conn()
    part_0 = reducer(part_0, tmp_part)

# Write result to final-result/ where client should be listening.
result_bytes = cloudpickle.dumps(part_0)
result_stream = io.BytesIO(result_bytes)
mc.put_object(bucket_name, f'final-result/{job_name}',
              result_stream,
              length = len(result_bytes))
