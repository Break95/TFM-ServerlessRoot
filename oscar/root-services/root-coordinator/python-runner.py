#!/usr/bin/env python3

# Coordinator in charge of invoking the reduction
# processes required

import sys
from minio import Minio
import urllib3

# Read how many


# Configure minio client
mc = Minio(endpoint=sys.argv[4][8:],
           access_key=sys.argv[5],
           secret_key=sys.argv[6],
           secure=False,
           http_client=urllib3.ProxyManager(
               sys.argv[4],
               cert_reqs='CERT_NONE'
           )
)

# Extract bucket name
bucket_name = sys.argv[3].split('/')[0]

# Listen to writes to partial-results.
with mc.listen_bucket_notification(bucket_name,
                                   prefix='partial-results',
                                   events=['s3:ObjectCreated:*']) as events:
    pass
