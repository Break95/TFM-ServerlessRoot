#!/usr/bin/env python3
import os
import sys
from random import randint
from time import sleep
from math import ceil, floor, log
from itertools import chain
import glob
# Arguments would be passed to lambda
# functions as dictionary/payload entries.
def mapper(index, ranges, reductions):
    print(f'Running mapper. Index: {index} - Range: {ranges[0]}_{ranges[1]}  ')
    range_start = ranges[0]
    range_end = ranges[1]

    # Process data.
    sleep(randint(1,4))

    # Only designed job performs the reduction.
    if (index == 0):
        # Spawn reducer process.
        pid = os.fork()
        if pid == 0:
            reducer(reductions)
            sys.exit(0)
    else:
        # Store result in folder.
        file_name = "intermediate/" + str(range_start) + "_" + str(range_end)
        f = open(file_name, "x")
        f.write("--")
        f.close()


def reducer(reductions):

    while reductions != 0:
        file_list = glob.glob('./intermediate/*')
        count = len(file_list)
        reductions -= count

        print(f'Reducing {count} parts. {reductions} remaining')

        for f in file_list:
            os.remove(f)

        sleep(1)

def index_generator2(num_jobs, reduction_factor):
    x = log(num_jobs) / log(reduction_factor)
    max_jobs = 2**floor(x) #Rename this variable, no longer holds max_jobs
    depth = ceil(x)

    if x.is_integer():
        remainder = 0
    else:
        remainder = num_jobs - max_jobs

    indexes = [0]
    for i in range(depth):
        indexes = list(chain.from_iterable( [[elem + 1, 0] for elem in indexes] ))

    print('Full tree')
    print(indexes)
    last_tree_size = int(len(indexes)/2)
    #indexes = indexes[0:last_tree_size]
    #print(indexes)

    while remainder > 0:
        x = log(remainder) / log(reduction_factor)
        max_jobs = 2**floor(x)
        depth = ceil(x)


        # Remove right hand side.
        indexes = indexes[0:last_tree_size]
        last_tree_size += max_jobs

        print(f'Prunned tree:')
        print(indexes)

        print(f'Current remainder: {remainder}')
        print(f'Adding reaminder')
        sub_tree = [0]


        if x.is_integer():
            remainder = 0
        else:
            remainder = remainder - max_jobs

        for i in range(depth):
            sub_tree = list(chain.from_iterable( [[elem+1,0] for elem in sub_tree] ))

        #last_tree_size = len(sub_tree)
        print('Subtree:')
        print(sub_tree)
        indexes = indexes + sub_tree
        print(indexes)

    print(f'Outside while')
    print(indexes)
    return indexes


dict = {
    # Index : Range
    0: [  0,100],
    1: [100,200],
    2: [200,300],
    3: [300,400],
    4: [400,500]
}

#Calculate tree depth.
reduction_factor = 2
ids = index_generator2(len(dict), reduction_factor)


# Execute as many mappers as jobs.
for key in dict.keys():

    pid = os.fork()
    if pid == 0:
        mapper(0, dict[key], len(ids)-1)
        sys.exit(0)
    else:
        mapper(1, dict[key], 0)
        sys.exit(0)
