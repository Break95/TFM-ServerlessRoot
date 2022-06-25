#!/usr/bin/env python3
# Coordinator in charge of invoking the reduction
# processes required
import sys
from minio import Minio
import urllib3
import cloudpickle
import io


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

# Listen to writes to partial-results.
with mc.listen_bucket_notification(bucket_name,
                                   prefix='partial-results',
                                   events=['s3:ObjectCreated:*']) as events:
    for event in events:
        event_count += 1
        object_name = event['Records'][0]['s3']['object']['key']
        event_name = object_name.split('/')[1]
        event_list.append(object_name)
        job_name += f'{event_name}_'

        # If we have the same amount of events queued as
        # the specified number of partial results to reduce together
        # trigger the reduction.
        if event_count == parts_per_red:
            # Trigger the reduction.
            create_reducer_job(mc, bucket_name, job_name, event_list)

            # Check if all reductions have been performed.
            if total_reductions == 0:
                break

            total_reductions -= 1
            
            # Reset variables
            event_count = 0
            event_list = []
            job_name = ''
