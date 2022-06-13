#!/usr/bin/env python3
import subprocess
import psutil
import cloudpickle
from time import sleep
import sys
import uuid

reducer_args = ['python3', '/opt/python-runner.py'] + sys.argv[1:]
print(reducer_args)

p = psutil.Process()
p_attrs = ['create_time', 'cpu_times', 'io_counters',
           'num_ctx_switches', 'memory_full_info', 'net_io_counters']

bench_start = p.as_dict(attrs=p_attrs)

# Call to actual process.
reducer_p = psutil.Popen(reducer_args)
reducer = psutil.Process(reducer_p.pid)

cpu_usage = []
# TODO: Write directly to file to avoid high memory usage in case of
# long running mappers.
# TODO: change polling to another strategy and also add call stack tracing.
while reducer_p.poll() is None:
    cpu_usage.append([reducer.cpu_percent(), reducer.memory_percent()])
    sleep(0.5)

bench_end = p.as_dict(attrs=p_attrs[1:])


#Write benchmarks to files.
file_name = sys.argv[1].split('/')[-1]
ts = uuid.uuid1()

f_bs = open(f'{sys.argv[2]}/benchmarks/reducer_{file_name}_start_{ts}', 'wb')
f_be = open(f'{sys.argv[2]}/benchmarks/reducer_{file_name}_end_{ts}', 'wb')
f_cp = open(f'{sys.argv[2]}/benchmarks/reducer_{file_name}_cpupercent_{ts}', 'wb')
f_bs.write(cloudpickle.dumps(bench_start))
f_be.write(cloudpickle.dumps(bench_end))
f_cp.write(cloudpickle.dumps(cpu_usage))
f_bs.close()
f_be.close()
f_cp.close()