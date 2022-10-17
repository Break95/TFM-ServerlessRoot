#!/usr/bin/env python3

import psutil

print('##### CPU Info #####')
print(psutil.cpu_times())
print(psutil.cpu_count())
print(psutil.cpu_stats())

print('##### Memory Info #####')
print(psutil.virtual_memory())

print('##### Disks info #####')
print(psutil.disk_io_counters(perdisk=True))
