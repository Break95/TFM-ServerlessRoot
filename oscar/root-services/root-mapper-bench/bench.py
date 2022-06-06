#!/usr/bin/env python3
import subprocess
import psutil

mapper_args = ['python3', 'python-runner.py'] + sys.argv[2:]

p = psutil.Process()
p_attrs = ['create_time', 'cpu_times', 'io_counters',
           'num_ctx_switches', 'memory_full_info']

bench_start = p.as_dict(attrs=p_attrs)

# Call to actual process.
subprocess.Popen(mapper_args)

bench_end = p.as_dict(attrs=p_attrs[1:])

#Write benchmarks to file.
f_bs = open(f'{sys.argv[2]}/benchmarks/{file_name}_start', 'wb')
f_be = open(f'{sys.argv[2]}/benchmarks/{file_name}_start', 'wb')
f_bs.write(cloudpickle.dumps(bench_start))
f_be.write(cloudpickle.dumps(bench_end))
f_bs.close()
f_be.close()
