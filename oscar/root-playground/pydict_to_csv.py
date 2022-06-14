#!/usr/bin/env python3
# Script to pass the bechmark data from the generated python dictionary
# stored as a json to csv files. For each job 3 files are generated.
#     - File containing the cpu and mem usage %s.
#     - File with process metrics cpu, mem, io, system network.
import csv
import json
import sys

# Load data from json into dictionary.
f = open(sys.argv[1], 'r')
data_dict = json.load(f)
f.close()

# Generate CSV
delimiter= '|'
headings_process = [
    # function:   mapper or reducer,
    # id:         what mapper or reducer is it,
    # phase:      start or end
    'function', 'id', 'phase',

    # CPU Metrics
    'cpu_user', 'cpu_system', 'cpu_child_user', 'cpu_child_sys', 'iowait',

    # IO Counters
    'io_read_count', 'io_write_count', 'io_read_bytes', 'io_write_bytes',
    'io_read_chars', 'io_write_chars',

    # Ctx switches
    'ctx_voluntary', 'ctx_involuntary',

    # Memory metrics
    'mem_rss', 'mem_vms', 'mem_shared', 'mem_text', 'mem_lib', 'mem_data', 'mem_dirty',
     'mem_uss', 'mem_pss', 'mem_swap',

    # TODO: Check if this data is for deployed job or full node.
    # System Network
    'net_bytes_sent', 'net_bytes_recv', 'net_packets_sent', 'net_packets_recv',
    'net_errin', 'net_errout', 'net_dropin', 'net_dropout',

    # Create time (only taken on start)
    'create_time'
]

headings_usage = [
    'function', 'id', 'time', 'cpu_percent', 'mem_percent'
]

csv_row = headings_process
job_id = sys.argv[1]
print(job_id)

csv_f_proc = open(f'{job_id}_process.csv' , 'w', newline='')
proc_writer = csv.writer(csv_f_proc, delimiter='|')
proc_writer.writerow(headings_process)

csv_f_usage = open(f'{job_id}_usage.csv', 'w', newline='')
usage_writer = csv.writer(csv_f_usage, delimiter='|')
usage_writer.writerow(headings_usage)

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

            proc_writer.writerow(csv_row)
        
        # Usage
        ts = 0.0
        csv_row = csv_id
        for snapshot in data_dict[fun][id]['cpupercent']:
            csv_row = csv_id + [ts] + snapshot
            ts += 0.5
            usage_writer.writerow(csv_row)

csv_f_proc.close()
csv_f_usage.close()
