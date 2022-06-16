#!/usr/bin/env python3
import subprocess
import psutil
import cloudpickle
from time import sleep
import sys
import uuid
import os

mapper_args = ['python3', '/opt/python-runner.py'] + sys.argv[1:]

p = psutil.Process()
p_attrs = ['create_time', 'cpu_times', 'io_counters',
           'num_ctx_switches', 'memory_full_info']

bench_start = p.as_dict(attrs=p_attrs)
net_start = psutil.net_io_counters()

# Call to actual process.
mapper_p = psutil.Popen(mapper_args)
mapper = psutil.Process(mapper_p.pid)

cpu_usage = []
# TODO: Write directly to file to avoid high memory usage in case of
# long running mappers.
# TODO: change polling to another strategy and also add call stack tracing.
while mapper_p.poll() is None:
    cpu_usage.append([mapper.cpu_percent(), mapper.memory_percent()])
    sleep(0.5)

bench_end = p.as_dict(attrs=p_attrs[1:])
net_end = psutil.net_io_counters()

#Write benchmarks to files.
file_name = sys.argv[1].split('/')[-1]
ts = uuid.uuid1()

node = os.environ['RESOURCE_ID']
f_bs = open(f'{sys.argv[2]}/benchmarks/mapper_{file_name}_start_{ts}_{node}', 'wb')
f_be = open(f'{sys.argv[2]}/benchmarks/mapper_{file_name}_end_{ts}_{node}', 'wb')
f_cp = open(f'{sys.argv[2]}/benchmarks/mapper_{file_name}_cpupercent_{ts}_{node}', 'wb')
f_ns = open(f'{sys.argv[2]}/benchmarks/mapper_{file_name}_netiost_{ts}_{node}', 'wb')
f_ne = open(f'{sys.argv[2]}/benchmarks/mapper_{file_name}_netioend_{ts}_{node}', 'wb')

f_bs.write(cloudpickle.dumps(bench_start))
f_be.write(cloudpickle.dumps(bench_end))
f_cp.write(cloudpickle.dumps(cpu_usage))
f_ns.write(cloudpickle.dumps(net_start))
f_ne.write(cloudpickle.dumps(net_end))

f_bs.close()
f_be.close()
f_cp.close()
f_ns.close()
f_ne.close()
