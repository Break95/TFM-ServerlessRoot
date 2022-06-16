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
import csv

try:
    import requests
    import cloudpickle
    from minio import Minio
except ImportError:
    raise ImportError(("Cannot import library 'requests', 'cloudpickle' or 'minio. "))

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class OSCARBackend(Base.BaseBackend):
    """OSCAR backend for distributed RDataFrame."""

    def __init__(self, oscarclient=None):
        super(OSCARBackend, self).__init__()
        # If the user didn't explicitly pass a Client instance, the argument
        # `OSCARclient` will be `None`. In this case, we create a default OSCAR
        # client connected to a cluster instance with N worker processes, where
        # N is the number of cores on the local machine.

        self.client = oscarclient

        # Generate uuid to allow job concurrency.
        self.client['uuid'] = uuid.uuid4()

        # O
        if self.client['benchmarking']:
            self.client['bucket_name'] = f"{self.client['bucket_name']}-{self.client['uuid']}-benchmark"
        else:
            self.client['bucket_name'] = f'{self.client["bucket_name"]}-{self.client["uuid"]}'
        print(self.client['bucket_name'])

        # Stablish Minio connection
        # We need this version due to no ssl certificates.
        import urllib3
        mc = Minio(
            self.client['minio_endpoint'],
            self.client['minio_access'],
            self.client['minio_secret'],
            secure = False,
            http_client=urllib3.ProxyManager( # Despite insecure request force https
                f"https://{self.client['minio_endpoint']}",
                cert_reqs='CERT_NONE'
            )
        )

        self.client['mc'] = mc

        # Check if bucket exists.
        if not mc.bucket_exists(self.client['bucket_name']):
            print('Bucket does not exist. Trying to create it.')
            if not self.client['oscar_endpoint'] or not self.client['oscar_access'] or not self.client['oscar_secret']:
                print('Missing OSCAR credentials. Please provide endpoint and access credentials')
                return
            # Create Bucket.
            try:
                # Create bucket
                print('Creating bucket...')
                mc.make_bucket(f"{self.client['bucket_name']}")
                print('Bucket created!')
            except Exception as e:
                print('Couldn\'t create bucket.')
                print(e)
                self.client = None
                return
        else:
            print('Bucket already exists.')

        # TODO: Should we check if services exist??
        # Create services.
        self._service_creation()


    def _service_creation(self):
        """
        ASD
        """
        try:
            print('Creating services...')
            # Create services associated to bucket.
            services = ['mapper', 'reducer']
            self.client['tasks'] = []
            self.client['services'] = []

            for service in services:
                    self.client['tasks'].append(asyncio.create_task(self._create_service(
                            self.client['bucket_name'],
                            self.client['benchmarking'],
                            service,
                            self.client['oscar_endpoint'],
                            self.client['oscar_access'],
                            self.client['oscar_secret']))
                        )

            print('Done creating services!')
        except Exception as e:
            print('Exception creating services.')
            print('Deleting associated bucket')
            print(e)
            raise


    def _service_yaml_to_http(self, function):
        # TODO: read from yaml template instead of hardcoding the dictionary?
        bucket_name = f'{self.client["bucket_name"]}'
        print(bucket_name)

        # Path were OSCAR will trigger the service.
        path = ''
        if function == 'mapper':
            path = f"{bucket_name}/mapper-jobs"
        else:
            path = f"{bucket_name}/partial-results"

        prefixes = ['partial-results', 'logs']
        script_suffix = ''
        benchmarking = self.client['benchmarking']
        if benchmarking == True:
            prefixes.append('benchmarks')
            script_suffix = '-benchmark'

        # TODO: maybe encode this to base64?
        script = ''
        with open(f'/usr/local/lib/root/DistRDF/Backends/OSCAR/service-scripts/root-{function}{script_suffix}.sh' ,'r') as script_file:
                script = script_file.read()

        service_name = f"{function[0]}-{self.client['uuid']}"
        self.client['services'].append(service_name)

        http_body = {
            # Name service strucure
            # bucket_name-root-[mapper|reducer]
            #"name": f"{bucket_name}-{function}",
            "name": service_name,
            "memory": "1Gi",
            "cpu": "1.0",
            "image": f"ghcr.io/break95/root-{function}{script_suffix}",
            "alpine": False,
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

        # Also output to benchmarking folder if needed.
        if benchmarking:
            http_body['output'][0]['prefix'].append('benchmarking')

        return http_body


    async def _create_service(self, bucket_name, benchmarking, service, oscar_endpoint, access, secret):
        """
        If needed, create services asynchronously at the beginning of the data frame  and
        check for the results of the service creation in `process_and_merge` to avoid waiting
        as much as possible.
        """
        print(f'Creating service {service} for {bucket_name}')
        request_body = self._service_yaml_to_http(service)

        return(requests.post(f"{oscar_endpoint}/system/services",
                      auth=requests.auth.HTTPBasicAuth(access,
                                                       secret),
                      json=request_body,
                      verify = False))


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
        import glob

        # TODO: Filter duplicate reducers.
        bench_results = mc.list_objects(bucket_name, 'benchmarks/', recursive=True)
        results = {'mapper': {},
                   'reducer': {}}

        for obj in bench_results:
            tmp = obj.object_name
            parts = tmp.split('/')[1].split('_')
            print(parts)
            bench_response = mc.get_object(bucket_name, tmp)
            bench_bytes = bench_response.data
            bench_response.release_conn()

            name = f'{parts[1]}_{parts[2]}'
            if name in results[parts[0]]:
                results[parts[0]][name] = results[parts[0]][name] | {parts[3]: cloudpickle.loads(bench_bytes)}
            else:
                results[parts[0]][name] = {parts[3]: cloudpickle.loads(bench_bytes)}

            # Add working node
            print(parts)
            results[parts[0]][name] = results[parts[0]][name] | {'node': parts[5]}

        import json # For some reason this in necessary.
        self.report_to_csv(json.loads(json.dumps(results)))
        #import json
        #file_name = f'{self.client["uuid"]}_bench_results'
        #with open(file_name, 'w') as result_file:
        #    result_file.write(json.dumps(results))
        #    print(f'Benchmark results written to {file_name}')


    def report_to_csv(self, data_dict):
        job_id = self.client['uuid']

        # Generate CSV
        delimiter= '|'
        headings_process = [
            # function:   mapper or reducer.
            # id:         what mapper or reducer is it, i.e 0_0, 4_8.
            # phase:      start or end.
            'function', 'id', 'phase',

            # CPU Metrics
            'cpu_user', 'cpu_system', 'cpu_child_user', 'cpu_child_sys', 'iowait',

            # IO Counters
            'io_read_count', 'io_write_count', 'io_read_bytes', 'io_write_bytes',
            'io_read_chars', 'io_write_chars',

            # Ctx switches
            'ctx_voluntary', 'ctx_involuntary',

            # Memory metrics
            'mem_rss', 'mem_vms', 'mem_shared', 'mem_text', 'mem_lib', 'mem_data',
            'mem_dirty', 'mem_uss', 'mem_pss', 'mem_swap',

            # TODO: Check if this data is for deployed job or full node.
            # System Network
            'net_bytes_sent', 'net_bytes_recv', 'net_packets_sent', 'net_packets_recv',
            'net_errin', 'net_errout', 'net_dropin', 'net_dropout',

            # Create time (only taken on start)
            'create_time',

            # Cluster node in which the function has been executed.
            'node'
        ]

        headings_usage = [
            'function', 'id', 'time', 'cpu_percent', 'mem_percent', 'node'
        ]

        csv_row = headings_process

        csv_f_proc = open(f'{job_id}_process.csv' , 'w', newline='')
        proc_writer = csv.writer(csv_f_proc, delimiter='|')
        proc_writer.writerow(headings_process)
        print(f'{job_id}_process.csv')

        csv_f_usage = open(f'{job_id}_usage.csv', 'w', newline='')
        usage_writer = csv.writer(csv_f_usage, delimiter='|')
        usage_writer.writerow(headings_usage)
        print(f'{job_id}_usage.csv')

        for fun in data_dict.keys():
            csv_base = [fun]
            for id in data_dict[fun].keys():
                csv_id = csv_base + [id]

                # Process
                for phase in ['start', 'end']:
                    csv_phase = csv_id + [phase]
                    csv_row = csv_phase

                    #for metric in data_dict[fun][id][phase].keys():
                    for metric in ['cpu_times', 'io_counters', 'num_ctx_switches', 'memory_full_info']:
                        csv_row = csv_row + data_dict[fun][id][phase][metric]

                    if phase == 'start':
                        csv_row = csv_row + data_dict[fun][id]['netiost']
                        csv_row = csv_row + [data_dict[fun][id][phase]['create_time']]
                    else:
                        csv_row = csv_row + data_dict[fun][id]['netioend']
                        csv_row = csv_row + [0]

                    csv_row = csv_row + [data_dict[fun][id]['node']]

                    proc_writer.writerow(csv_row)

                # Usage
                ts = 0.0
                csv_row = csv_id
                for snapshot in data_dict[fun][id]['cpupercent']:
                    csv_row = csv_id + [ts] + snapshot + [data_dict[fun][id]['node']]
                    ts += 0.5
                    usage_writer.writerow(csv_row)

        csv_f_proc.close()
        csv_f_usage.close()


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
        #for fun in [mapper, reducer]:
        #        fun_bytes = cloudpickle.dumps(fun)
        #        fun_stream = io.BytesIO(fun_bytes)
        #        mc.put_object(bucket_name,
        #                      f'function')


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

        # TODO: Change this for a call to _progress()
        from IPython.display import clear_output

        while not found:
            files = mc.list_objects(bucket_name, 'partial-results/',
                             recursive=True)

            clear_output(wait=True)

            for obj in files:
                full_name = obj.object_name
                file_name = full_name.split('/')[1]
                print(f'File name: {file_name}')
                if target_name == file_name:
                        found = True
                        break

            print(f'Waiting for final result {target_name}, sleeping 10 seconds.', flush=True)
            sleep(10)
            
        result_response = mc.get_object(bucket_name,  f'partial-results/{target_name}')
        result_bytes = result_response.data
        final_result = cloudpickle.loads(result_bytes)

        if self.client['benchmarking']:
                self.benchmark_report(mc, bucket_name)

        # Cleanup bucket and asociated services.
        self._cleanup()

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


    def _cleanup(self):
        """
        More performance questions on deleting files starting by x. Instead
        of simply whiping 'folder/bucket'.
        """
        from minio.deleteobjects import DeleteObject
        # Clean bucket data
        delete_obj_list = map(lambda x: DeleteObject(x.object_name),
                              self.client['mc'].list_objects(self.client['bucket_name'],
                                                             recursive=True))

        print('Deleting objects.')
        errors = self.client['mc'].remove_objects(self.client['bucket_name'], delete_obj_list)

        for error in errors:
            print(error)

        try:
            self.client['mc'].remove_bucket(self.client['bucket_name'])
            print('Bucket Deleted.')
        except Exception as e:
            print('Couldn\'t delete bucket.')
            print(e)

        # Clean services from OSCAR.
        print('Deleting services')
        if 'services' in self.client:
            for service in self.client['services']:
                print(service)
                print(requests.delete(
                    f"{self.client['oscar_endpoint']}/system/services/{service}",
                    auth=requests.auth.HTTPBasicAuth(
                         self.client['oscar_access'],
                         self.client['oscar_secret']),
                    verify = False))


    def _progress(self, start, end):
        """
        Print progress of running job.
        """
        pass
