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
def mapper(index, ranges):
    print(f'Running mapper. Index: {index} - Range: {ranges[0]}_{ranges[1]}  ')
    range_start = ranges[0]
    range_end = ranges[1]

    # Process data.
    #sleep(randint(1,4))

    # Decide if mapper invokes reducer or stores the data to file.
    if (index != 0):
        # Spawn reducer process.
        pid = os.fork()
        if pid == 0:
            reducer(index, ranges)
            sys.stdout.flush()
            sys.exit(0)
    else:
        # Store result in folder.
        file_name = "intermediate/" + str(range_start) + "_" + str(range_end)
        print(f'Mapper writing: {file_name}')
        f = open(file_name, "x")
        f.write("--")
        f.close()


def reducer(index, ranges):    # Last reducer write to output bucket.
    #if index == 0:
    #    sys.exit(0)
    #blocksize = ranges[1] - ranges[0];
    #new_end = ranges[1] + blocksize;
    sleep(3)
    new_end = glob.glob(f'./intermediate/{ranges[1]}_*')[0].split('_')[1]

    print(f'Running reducer. Index: {index} - Range: {ranges[0]}_{new_end}')


    # Check for the next file. Wait 1 second if it doesnt exist.
    nextFile = f'intermediate/{ranges[1]}_{new_end}'
    if not os.path.exists(nextFile):
        print(f'Couldnt find dile {nextFile}')
        sys.exit(1)

    # Perform reduction
    os.remove(nextFile)
    index = index -1
    print(f'New index: {index}')
    if (index >= 1):
        pid = os.fork()
        if pid == 0:
            ranges[1] = new_end
            reducer(index, ranges)
            sys.stdout.flush()
            sys.exit()
    else: # Write to intermediate bucket.
        file_name = f'intermediate/{ranges[0]}_{new_end}'

        print(f'Writing file {file_name}')

        f = open(file_name, 'w')
        f.write('--')
        f.close()


def index_generator(num_jobs, reduction_factor):
    depth = ceil(num_jobs/reduction_factor)
    indexes = [0]

    for i in range(depth):
        #for elem in range(len(indexes)):
            #indexes.append(indexes[elem] + 1)
            #indexes.append(0)
        indexes = list(chain.from_iterable( [[elem + 1, 0] for elem in indexes] ))

    return indexes


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
    4: [400,500],
    5: [500,600],
    6: [600,700],
    7: [700,800]
}

#Calculate tree depth.
reduction_factor = 2

ids = index_generator2(len(dict), reduction_factor)

#sys.exit(0)
# Execute as many mappers as jobs.
for key in dict.keys():

    id = ids.pop(0)
    pid = os.fork()
    if pid == 0:
        mapper(id, dict[key])
        sys.stdout.flush()
        sys.exit(0)
