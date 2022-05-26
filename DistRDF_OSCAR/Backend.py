#!/usr/bin/env python3

import os
import json
import requests
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
        
        # Generate indexing.
        reduction_factor = 2
        reduce_indices = index_generator(len(ranges), reduction_factor)
        print(reduce_indices)

        omapper = str(base64.b64encode(cloudpickle.dumps(mapper)))
        oreducer = str(base64.b64encode(cloudpickle.dumps(reducer)))

        for rang in ranges:
             orange = str(base64.b64encode(cloudpickle.dumps(rang)))
             x = requests.post(self.client['endpoint'],
                           headers= {
                               "Accept": "application/json",
                               "Authorization": "Bearer " + self.client['token']},
                           json=json.dumps(  {'mapper': omapper,
                                              'ranges': orange,
                                              'reducer': oreducer,
                                              'index': reduce_indices.pop(0),
                                              'token': self.client['red_token']}))
             print(x)                                 
             

        # Retrieve files
        # Obtain final result
        mc = Minio(self.client['minio_endpoint'],
            access_key=self.client['minio_access'],
            secret_key=self.client['minio_secret'])

        found = False

        start = ranges[0].start
        end = ranges[-1].end
        target_name = f'{start}_{end}'
        full_name = ''
        final_result = None
        
        if mc.bucket_exists("root-oscar"):
            while not found:
                files = mc.list_objects('root-oscar', 'out/',
                             recursive=True)


                for obj in files:
                    full_name = obj.object_name
                    file_name = full_name.split('/')[1]

                    if target_name == file_name:
                            found = True
                            break
                print(f'Waiting for final result, sleeping 1 second.')
                sleep(1)
            
            mc.fget_object('root-oscar', full_name, f'/tmp/{full_name}')
            handle = open(f'/tmp/{full_name}', "rb")
            final_result = cloudpickle.load(handle)

            mc.remove_object('root-oscar', full_name)

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
        headnode = HeadNode.get_headnode(npartitions, *args)
        return DataFrame.RDataFrame(headnode, self)


