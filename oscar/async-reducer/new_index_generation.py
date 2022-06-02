#!/usr/bin/env python3
from math import floor,log,ceil
# Job generation demo.

ranges = [
    # Index : Range
    [0],
    [1],
    [2],
    [3],
    [4],
    [5],
    [6],
    [7]
]


max_depth = ceil(log(len(ranges))/log(2))
array_job = []
tmp = [ [rang[0]] * 4 for rang in ranges ]
array_job.append(tmp)
depth = 1
# Get initial array of ranges.
while max_depth >= depth:
    array_job.append([])

    tmp_len = len(array_job[depth-1])
    odd = False
    if tmp_len % 2 != 0:
        tmp_len -= 1
        odd = True

    for rang in range(0,tmp_len,2):
        t1 = array_job[depth-1][rang]
        t2 = array_job[depth-1][rang+1]
        array_job[depth].append([t1[0], t1[3], t2[0], t2[3]])

    # If odd add last elements.
    if odd:
        array_job[depth].append(array_job[depth-1][-1])

    depth += 1

mappers = array_job[0]
reducers = array_job[1:]

print('Mappers:')
print(mappers)

print('Raw Reducers:')
for i in range(len(reducers)):
    print(reducers[i])

flatten = [f'{job[0]}_{job[1]}-{job[2]}_{job[3]}' for elem in reducers for job in elem ]
print(flatten)
