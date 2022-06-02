#!/usr/bin/env python3

import os
import json
#import requests
import base64
#import inspect
import cloudpickle
from minio import Minio
import time
from math import log, ceil, floor
from itertools import chain
from time import sleep
from DistRDF import DataFrame
from DistRDF import HeadNode
from DistRDF.Backends import Base
from DistRDF.Backends import Utils
import io
#try:
#    import dask
#    from dask.distributed import Client, LocalCluster, progress, get_worker
#except ImportError:
#    raise ImportError(("cannot import an OSCAR component. Refer to the OSCAR documentation "
#                       "for installation instructions."))


class OSCARBackend(Base.BaseBackend):
    """OSCAR backend for distributed RDataFrame."""

    def __init__(self, oscarclient=None):
        super(OSCARBackend, self).__init__()
        # If the user didn't explicitly pass a Client instance, the argument
        # `OSCARclient` will be `None`. In this case, we create a default OSCAR
        # client connected to a cluster instance with N worker processes, where
        # N is the number of cores on the local machine.
        self.client = oscarclient #(oscarclient if oscarclient is not None else
            #    Client(LocalCluster(n_workers=os.cpu_count(), threads_per_worker=1, processes=True)))

    def optimize_npartitions(self):
        # For Serverless the decission process should be different, theoretically
        # any serverless approach can execute as many functions, concurrently,
        # as we want. We should take decissions based of the 'function' resources,
        # i.e. memory. Focused on partition size not number of partitions.

        # Maybe create test payloads during setup to decide upon size of partition

        """
        Attempts to compute a clever number of partitions for the current
        execution. Currently, we try to get the total number of worker logical
        cores in the cluster.
        """
        #workers_dict = self.client.scheduler_info().get("workers")
        workers_dict = None
        if workers_dict:
            # The 'workers' key exists in the dictionary and it is non-empty
            return sum(worker['nthreads'] for worker in workers_dict.values())
        else:
            # The scheduler doesn't have information about the workers
            # print('Min Partitions' + str(self.MIN_NPARTITIONS))
            return self.MIN_NPARTITIONS


    def ProcessAndMerge(self, ranges, mapper, reducer):
        """
        Performs map-reduce using Dask framework.

        Args:
            mapper (function): A function that runs the computational graph
                and returns a list of values.

            reducer (function): A function that merges two lists that were
                returned by the mapper.

        Returns:
            list: A list representing the values of action nodes returned
            after computation (Map-Reduce).
        """

        # TODO: there is repeated code, refactor this.
        '''
        def index_generator(num_jobs, reduction_factor):
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

            last_tree_size = int(len(indexes)/2)

            while remainder > 0:
                x = log(remainder) / log(reduction_factor)
                max_jobs = 2**floor(x)
                depth = ceil(x)

                # Remove right hand side.
                indexes = indexes[0:last_tree_size]
                last_tree_size += max_jobs

                sub_tree = [0]

                if x.is_integer():
                    remainder = 0
                else:
                    remainder = remainder - max_jobs

                for i in range(depth):
                    sub_tree = list(chain.from_iterable( [[elem+1,0] for elem in sub_tree] ))

                indexes = indexes + sub_tree

            return indexes
        '''

        def index_generator2(ranges):
            max_depth = ceil(log(len(ranges))/log(2))
            init_ranges = [[rang.id] * 4 for rang in ranges]
            array_job = []
            array_job.append(init_ranges)
            depth = 1

            while max_depth >= depth:
                # Add new level
                array_job.append([])

                tmp_len = len(array_job[depth-1])
                odd = False
                if tmp_len % 2 != 0:
                    tmp_len -= 1
                    odd = True

                for rang in range(0, tmp_len,2):
                    t1 = array_job[depth-1][rang]
                    t2 = array_job[depth-1][rang+1]
                    array_job[depth].append([t1[0], t1[3], t2[0], t2[3]])

                # If odd add last element
                if odd:
                    array_job[depth].append(array_job[depth-1][-1])

                depth += 1

            reducers = array_job[1:]
            flattened = [f'{job[0]}_{job[1]}-{job[2]}_{job[3]}' for elem in reducers for job in elem]
            return flattened

        
        # Generate indexing.
        reduction_factor = 2
        reduce_indices = index_generator2(ranges)
        print(reduce_indices)


        # Data Storage Connection
        mc = Minio(self.client['minio_endpoint'],
            access_key=self.client['minio_access'],
            secret_key=self.client['minio_secret'])

        # Write mapper and reducer functions to bucket.
        mapper_bytes = cloudpickle.dumps(mapper)
        reducer_bytes = cloudpickle.dumps(reducer)

        mapper_stream = io.BytesIO(mapper_bytes)
        reducer_stream = io.BytesIO(reducer_bytes)

        mc.put_object('root-oscar',
                      'functions/mapper',
                      mapper_stream,
                      length = len(mapper_bytes))

        mc.put_object('root-oscar',
                      'functions/reducer',
                      reducer_stream,
                      length = len(reducer_bytes))

        # Write reduction jobs.
        for reducer_job in reduce_indices:
            mc.put_object('root-oscar',
                          f'reducer-jobs/{reducer_job}',
                          io.BytesIO(b'\xff'),
                          length=1)

        # Write mappers jobs.
        for rang in ranges:
            rang_bytes = cloudpickle.dumps(rang)
            rang_stream = io.BytesIO(rang_bytes)
            file_name = f'{rang.id}_{rang.id}'
            mc.put_object('root-oscar',
                          f'mapper-jobs/{file_name}',
                          rang_stream,
                          length = len(rang_bytes))

        # Retrieve files
        # Obtain final result
        found = False
        start = ranges[0].id
        end = ranges[-1].id

        target_name = f'{start}_{end}'
        print(f'Target Name: {target_name}')
        full_name = ''
        final_result = None

        # TODO: Include restAPI server to use webhook instead of polling.
        if mc.bucket_exists("root-oscar"):
            while not found:
                files = mc.list_objects('root-oscar', 'partial-results/',
                             recursive=True)

                for obj in files:
                    full_name = obj.object_name
                    file_name = full_name.split('/')[1]
                    print(f'File name: {file_name}')
                    if target_name == file_name:
                            found = True
                            break
                print(f'Waiting for final result {target_name}, sleeping 1 second.')
                sleep(1)
            
            result_response = mc.get_object('root-oscar',  f'partial-results/{target_name}')
            result_bytes = result_response.data
            final_result = cloudpickle.loads(result_bytes)
        else:
            print('Bucket does not exist.')

        return final_result

    def distribute_unique_paths(self, paths):
        """
        Dask supports sending files to the workes via the `Client.upload_file`
        method. Its stated purpose is to send local Python packages to the
        nodes, but in practice it uploads the file to the path stored in the
        `local_directory` attribute of each worker.
        """
        for filepath in paths:
            self.client.upload_file(filepath)

    def make_dataframe(self, *args, **kwargs):
        """
        Creates an instance of distributed RDataFrame that can send computations
        to a Dask cluster.
        """
        # Set the number of partitions for this dataframe, one of the following:
        # 1. User-supplied `npartitions` optional argument
        # 2. An educated guess according to the backend, using the backend's
        #    `optimize_npartitions` function
        # 3. Set `npartitions` to 2
        npartitions = kwargs.pop("npartitions", self.optimize_npartitions())
        headnode = HeadNode.get_headnode(self, npartitions, *args)
        return DataFrame.RDataFrame(headnode)


