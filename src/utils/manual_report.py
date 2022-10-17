#!/usr/bin/env python3
# Generate cvcs data on demand.
# 1 argument is required.
#     1 - bucket_name
# 1 argumen is optional
#     2 - file_prefix
#
# python3 manual_report.py bucket_name file_prefix
#
# NOTE: as cloudpickle is used to serialize the benchmarked
# data pytohn, psutil and cloudpickle versions must be the same.

import glob
from minio import Minio
import sys
import os
import urllib3
import cloudpickle
import json
import csv
import psutil

if len(sys.argv) < 2:
    print('Please provide a bucket Name')

bucket_name = sys.argv[1]

mc = Minio(os.environ['minio_endpoint'], os.environ['minio_access'], os.environ['minio_secret'],
           secure = False,
           http_client=urllib3.ProxyManager(
               f"https://{os.environ['minio_endpoint']}",
               cert_reqs = 'CERT_NONE'
           ))

# TODO: Filter duplicate reducers.
try:
    bench_results = mc.list_objects(bucket_name, 'benchmarks/', recursive=True)
except:
    print('The specified bucket name does not exist.')
    exit

results = {'mapper': {}, 'reducer': {}}

reducer_counts = {}

# Get benchmark results from MINIO.
for obj in bench_results:
    tmp = obj.object_name
    print(tmp)
    parts = tmp.split('/')[1].split('_')

    bench_response = mc.get_object(bucket_name, tmp)
    bench_bytes = bench_response.data
    bench_response.release_conn()

    name = f'{parts[1]}_{parts[2]}'
    if name in results[parts[0]]:
        results[parts[0]][name] = results[parts[0]][name] | {parts[3]: cloudpickle.loads(bench_bytes)}
    else:
        results[parts[0]][name] = {parts[3]: cloudpickle.loads(bench_bytes)}

    # Add working node
    results[parts[0]][name] = results[parts[0]][name] | {'node': parts[5]}

    if parts[0] == 'reducer':
        if name in reducer_counts:
            reducer_counts[name] = reducer_counts[name] + 1
        else:
            reducer_counts[name] = 1


data_dict = json.loads(json.dumps(results))

job_id = ''
folder = ''

# Generate CSV
delimiter= '|'
headings_process = [
            # function:   mapper or reducer.
            # id:         what mapper or reducer is it, i.e 0_0, 4_8.
            # phase:      start or end.
            'function', 'id', 'phase',
            # CPU Metrics
            'cpu_user', 'cpu_system', 'cpu_child_user', 'cpu_child_sys', 'iowait',
            # IO Counters
            'io_read_count', 'io_write_count', 'io_read_bytes', 'io_write_bytes',
            'io_read_chars', 'io_write_chars',
            # Ctx switches
            'ctx_voluntary', 'ctx_involuntary',
            # Memory metrics
            'mem_rss', 'mem_vms', 'mem_shared', 'mem_text', 'mem_lib', 'mem_data',
            'mem_dirty', 'mem_uss', 'mem_pss', 'mem_swap',
            # System Network
            'net_bytes_sent', 'net_bytes_recv', 'net_packets_sent', 'net_packets_recv',
            'net_errin', 'net_errout', 'net_dropin', 'net_dropout',
            # Create time (only taken on start)
            'create_time',
            # Cluster node in which the function has been executed.
            'node'
]

headings_usage = ['function', 'id', 'time', 'cpu_percent', 'mem_percent', 'node']

csv_row = headings_process

csv_f_proc = open(f'{folder}{job_id}_process.csv' , 'w', newline='')
proc_writer = csv.writer(csv_f_proc, delimiter='|')
proc_writer.writerow(headings_process)
print(f'{job_id}_process.csv')

csv_f_usage = open(f'{folder}{job_id}_usage.csv', 'w', newline='')
usage_writer = csv.writer(csv_f_usage, delimiter='|')
usage_writer.writerow(headings_usage)
print(f'{job_id}_usage.csv')

for fun in data_dict.keys():
            csv_base = [fun]
            for id in data_dict[fun].keys():
                csv_id = csv_base + [id]

                # Process
                for phase in ['start', 'end']:
                    csv_phase = csv_id + [phase]
                    csv_row = csv_phase

                    #for metric in data_dict[fun][id][phase].keys():
                    for metric in ['cpu_times', 'io_counters', 'num_ctx_switches', 'memory_full_info']:
                        csv_row = csv_row + data_dict[fun][id][phase][metric]

                    if phase == 'start':
                        csv_row = csv_row + data_dict[fun][id]['netiost']
                        csv_row = csv_row + [data_dict[fun][id][phase]['create_time']]
                    else:
                        csv_row = csv_row + data_dict[fun][id]['netioend']
                        csv_row = csv_row + [0]

                    csv_row = csv_row + [data_dict[fun][id]['node']]

                    proc_writer.writerow(csv_row)

                # Usage
                ts = 0.0
                csv_row = csv_id
                for snapshot in data_dict[fun][id]['cpupercent']:
                    csv_row = csv_id + [ts] + snapshot + [data_dict[fun][id]['node']]
                    ts += 0.5
                    usage_writer.writerow(csv_row)

csv_f_proc.close()
csv_f_usage.close()
