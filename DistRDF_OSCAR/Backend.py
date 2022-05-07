#!/usr/bin/env python3

import os
import json
import requests
import base64
#import inspect
import cloudpickle
from minio import Minio
import time

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
        #headers = self.headers
        #shared_libraries = self.shared_libraries


        #print(inspect.getsource(mapper))
        #print('#####')
        #print(cloudpickle.dumps(mapper))
        omapper = str(base64.b64encode(cloudpickle.dumps(mapper)))
        for range in ranges:
             orange = str(base64.b64encode(cloudpickle.dumps(range)))
             requests.post(self.client['endpoint'],
                           headers= {
                               "Accept": "application/json",
                               "Authorization": "Bearer " + self.client['token']},
                           json=json.dumps(  {'mapper': omapper,
                                              'ranges': orange}),
                           verify=False)

        #oreducer = str(base64.b64encode(cloudpickle.dumps(reducer)))


        # Check endpoint for files to be uploaded
        # This asumes we have acces to the "intermediate" storage service.
        minioClient = Minio(
            "localhost:30300",
            access_key="minio",
            secret_key="minioPassword",
            secure=False
        )

        # Retrieve files
        print(minioClient.list_buckets())
        time.sleep(10)
        mergeables_lists = []
        #for range in ranges:
        if minioClient.bucket_exists("root-oscar"):
            for range in ranges:
                response = minioClient.get_object("root-oscar",       # Bucket name
                                             "intermediate-out/" +  str(range.start) + '_' + # Object name
                                             str(range.end) + 'jobname_id')
                with open('my-testfile', 'wb') as file_data:
                    for d in response.stream(32*1024):
                        file_data.write(d)

                with open('my-testfile', 'rb') as file_data:
                    mergeables_lists.append(cloudpickle.loads(file_data.read()))
                #print(response.status)
                #print(response)                print(response.data.decode())
                #mergeable_lists.append(cloudpickle.dumps(response))
        else:
            print('Bucket does not exist.')
            return None

        while len(mergeables_lists) > 1:
            mergeables_lists.append(
                reducer(mergeables_lists.pop(0), mergeables_lists.pop(0)))

        final_results = mergeables_lists.pop()

        return final_results

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
