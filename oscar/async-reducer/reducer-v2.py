#!/usr/bin/env python3
from math import floor,log,ceil
# Job generation demo.

ranges = [
    # Index : Range
    [  0,100],
    [100,200],
  #  [200,300],
  #  [300,400],
  #  [400,500],
  #  [500,600],
  #  [600,700]
  #  [700,800]
]


max_depth = ceil(log(len(ranges))/log(2))
array_job = []
ranges = [[elem[0], 0, elem[1]] for elem in ranges]
array_job.append(ranges)
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
        array_job[depth].append([t1[0], t1[2], t2[2]])

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

flatten = [f'{job[0]}-{job[1]}_{job[1]}-{job[2]}' for elem in reducers for job in elem ]
print(flatten)
