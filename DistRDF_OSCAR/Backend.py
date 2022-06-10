#!/usr/bin/env python3

import os
import time
from math import log, ceil, floor
from itertools import chain
from time import sleep
from DistRDF import DataFrame
from DistRDF import HeadNode
from DistRDF.Backends import Base
from DistRDF.Backends import Utils
import io
import uuid
import asyncio


try:
        import requests
        import cloudpickle
        from minio import Minio
except ImportError:
        raise ImportError(("Cannot import library 'requests', 'cloudpickle' or 'minio. "))


# TODO: move this to an utils.py file
def service_yaml_to_http(bucket_name, function, benchmarking):
        # TODO: read from yaml template instead of hardcoding the dictionary.

        path = ''
        if function == 'mapper':
                path = f"{bucket_name}/mapper-jobs"
        else:
                path = f"{bucket_name}/partial-results"

        prefixes = ['partial-results', 'logs']
        if benchmarking == True:
                prefixes.append('benchmarking')

        #TODO: maybe encode this to base64?
        script = ''
        with open(f'/usr/local/lib/root/DistRDF/Backends/OSCAR/service-scripts/root-{function}.sh' ,'r') as script_file:
                script = script_file.read()

        http_body = {
            # Name service strucure
            # bucket_name-root-[mapper|reducer]
            "name": f"{bucket_name}-{function}",
            "memory": "1Gi",
            "cpu": "1.0",
            "image": f"ghcr.io/break95/root-{function}",
            "alpine": False,
            #"script": "root-map.sh",
            "script": script,
            "input": [
                {
                    "storage_provider": "minio.default",
                    "path": path
                }
            ],
            "output": [
                {
                    "storage_provider": "minio.default",
                    "path": f"{bucket_name}",
                    "prefix": prefixes
                }
            ]
        }

        if benchmarking:
            http_body['output'][0]['prefix'].append('benchmarking')

        return http_body

async def create_service(bucket_name, benchmarking, service, oscar_endpoint, access, secret):
        """
        If needed, create services asynchronously at the beginning of the data frame  and
        check for the results of the service creation in `process_and_merge` to avoid waiting
        as much as possible.
        """
        print(f'Creating service {service} for {bucket_name}')
        request_body = service_yaml_to_http(
                bucket_name,
                service,
                benchmarking
        )

        return(requests.post(f"{oscar_endpoint}/system/services",
                      auth=requests.auth.HTTPBasicAuth(access,
                                                       secret),
                      json=request_body,
                      verify = False))


class OSCARBackend(Base.BaseBackend):
    """OSCAR backend for distributed RDataFrame."""

    def __init__(self, oscarclient=None):
        super(OSCARBackend, self).__init__()
        # If the user didn't explicitly pass a Client instance, the argument
        # `OSCARclient` will be `None`. In this case, we create a default OSCAR
        # client connected to a cluster instance with N worker processes, where
        # N is the number of cores on the local machine.

        # Generate uuid to allow job concurrency.
        oscarclient['uuid'] = uuid.uuid4()

        # O
        if oscarclient['benchmarking']:
            oscarclient['bucket_name'] = f"{oscarclient['bucket_name']}-benchmark"

        print(oscarclient['bucket_name'])

        # Stablish Minio connection
        #mc = Minio(self.client['minio_endpoint'],
        #    access_key=self.client['minio_access'],
        #    secret_key=self.client['minio_secret'])

        # We need this version due to no ssl certificates.
        import urllib3
        mc = Minio(
            oscarclient['minio_endpoint'],
            oscarclient['minio_access'],
            oscarclient['minio_secret'],
            secure = False,
            http_client=urllib3.ProxyManager( # Despite insecure request force https
                f"https://{os.environ['minio_endpoint']}",
                cert_reqs='CERT_NONE'
            )
        )

        # Check if bucket exists.
        if not mc.bucket_exists(oscarclient['bucket_name']):
            print('Bucket does not exist. Trying to create it.')
            if not oscarclient['oscar_endpoint']or not oscarclient['oscar_access'] or not oscarclient['oscar_secret']:
                print('Missing OSCAR credentials. Please provide endpoint and access credentials')
                return
            # Deploy bucket and services.
            try:
                # Create bucket
                print('Creating bucket...')
                mc.make_bucket(f"{oscarclient['bucket_name']}")
                print('Bucket created!')
            except Exception as e:
                print('Couldn\'t create bucket.')
                print(e)
                self.client = None
                return

            try:
                print('Creating services...')
                # Create services associated to bucket.
                services = ['mapper', 'reducer']
                oscarclient['tasks'] = []

                for service in services:
                        oscarclient['tasks'].append(asyncio.create_task(create_service(
                                                    oscarclient['bucket_name'],
                                                    oscarclient['benchmarking'],
                                                    service,
                                                    oscarclient['oscar_endpoint'],
                                                    oscarclient['oscar_access'],
                                                    oscarclient['oscar_secret']))
                                     )

                print('Done creating services!')
            except Exception as e:
                print('Exception creating services.')
                print('Deleting associated bucket')
                print(e)
                raise

        oscarclient['mc'] = mc

        # TODO: Should we check if services exist??

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


    def benchmark_report(self, mc, bucket_name):
        """
        Generate perfomance report based on results of job.
        """

        bench_results = mc.list_objects(bucket_name, 'benchmarks/', recursive=True)

        mappers = {}
        reducers = {}

        for obj in bench_results:
            tmp = obj.object_name
            print(tmp)
            parts = tmp.split('/')[1].split('_')


            bench_response = mc.get_object(bucket_name, tmp)
            bench_bytes = bench_response.data
            # Mapper
            if parts[0] == parts[1]:
                mappers[f'{parts[0]}_{parts[2]}'] = cloudpickle.loads(bench_bytes)['cpu_times']
            # Reducer
            else:
                reducers[f'{tmp}'] = cloudpickle.loads(bench_bytes)['cpu_times']

        mkeys = mappers.keys()

        while len(mappers) != 0:
                k1,v1 = mappers.popitem()
                if k1.split('_')[-1] == 'start':
                        k2 = f"{k1.split('_')[0]}_end"
                        v2 = mappers.pop(k2)
                else:
                        k2 = f"{k1.split('_')[0]}_start"
                        v2 = mappers.pop(k2)


        print('Mappers')
        print(mappers)
        print('Reducers')
        print(reducers)


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

        mc = self.client['mc']

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

                # If odd move last element of previous depth to next one.
                if odd:
                    array_job[depth].append(array_job[depth-1].pop())

                depth += 1

            reducers = array_job[1:]
            flattened = [f'{job[0]}_{job[1]}-{job[2]}_{job[3]}' for elem in reducers for job in elem]
            return flattened

        
        # Generate indexing.
        reduce_indices = index_generator2(ranges)
        print(reduce_indices)

        # Read bucket name
        bucket_name = self.client['bucket_name']
        print(bucket_name)

        # If we have created the services during client instantiation,
        # check that they have been created succesfully.
        if 'tasks' in self.client:
                for task in self.client['tasks']:
                        print(task)

        # Write mapper and reducer functions to bucket.
        # TODO: refactor this for less repeated code.
        mapper_bytes = cloudpickle.dumps(mapper)
        reducer_bytes = cloudpickle.dumps(reducer)

        mapper_stream = io.BytesIO(mapper_bytes)
        reducer_stream = io.BytesIO(reducer_bytes)

        mc.put_object(bucket_name,
                      'functions/mapper',
                      mapper_stream,
                      length = len(mapper_bytes))

        mc.put_object(bucket_name,
                      'functions/reducer',
                      reducer_stream,
                      length = len(reducer_bytes))

        # Write reduction jobs.
        for reducer_job in reduce_indices:
            mc.put_object(bucket_name,
                          f'reducer-jobs/{reducer_job}',
                          io.BytesIO(b'\xff'),
                          length=1)

        # Write mappers jobs.
        for rang in ranges:
            rang_bytes = cloudpickle.dumps(rang)
            rang_stream = io.BytesIO(rang_bytes)
            file_name = f'{rang.id}_{rang.id}'
            mc.put_object(bucket_name,
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
        if mc.bucket_exists(bucket_name):
            while not found:
                files = mc.list_objects(bucket_name, 'partial-results/',
                             recursive=True)

                for obj in files:
                    full_name = obj.object_name
                    file_name = full_name.split('/')[1]
                    print(f'File name: {file_name}')
                    if target_name == file_name:
                            found = True
                            break
                print(f'Waiting for final result {target_name}, sleeping 1 seconds.')
                sleep(1)
            
            result_response = mc.get_object(bucket_name,  f'partial-results/{target_name}')
            result_bytes = result_response.data
            final_result = cloudpickle.loads(result_bytes)

            if self.client['benchmarking']:
                     self.benchmark_report(mc, bucket_name)
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


    def bucket_cleanup():
        """
        More performance questions on deleting files starting by x. Instead
        of simply whiping 'folder/bucket'.
        """
        pass
