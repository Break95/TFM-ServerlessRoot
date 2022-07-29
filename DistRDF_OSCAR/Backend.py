#!/usr/bin/env python3
import os
import time
from time import sleep
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
from copy import deepcopy
import urllib3
try:
    import requests
    import cloudpickle
    from minio import Minio
except ImportError:
    raise ImportError(("Cannot import library 'requests', 'cloudpickle' or 'minio. "))

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import ROOT # Imported to use stopwatch inside ProcessAndMerge to better measure time.

class OSCARBackend(Base.BaseBackend):
    """OSCAR backend for distributed RDataFrame."""

    def __init__(self, oscarclient=None):
        # Check oscarclient has been paased to the constructor.
        if not oscarclient:
           raise AttributeError(("oscarclient can't be None"))

        # Check at least minio endpoint and credentials have been configured.
        try:
            oscarclient['minio_endpoint']
            oscarclient['minio_access']
            oscarclient['minio_secret']
        except AttributeError as e:
            raise KeyError(("Missing minio parameter" + str(e)))

        super(OSCARBackend, self).__init__()

        self.client = deepcopy(oscarclient)

        # Generate uuid to allow job concurrency.
        self.client['uuid'] = uuid.uuid4()

        if 'bucket_name' not in self.client:
            self.client['bucket_name'] = "default"

        # Setup bucket name.
        if 'benchmarking' in self.client:
            self.client['bucket_name'] = f'{self.client["bucket_name"]}-{self.client["uuid"]}-benchmark'
        else:
            self.client['bucket_name'] = f'{self.client["bucket_name"]}-{self.client["uuid"]}'
            self.client['benchmarking'] = False
        print(f"Bucket name: {self.client['bucket_name']}")

        # Create MINIO client
        # We need this version (use of http_client) due to no ssl certificates.
        self.client['mc'] = Minio(
            self.client['minio_endpoint'],
            self.client['minio_access'],
            self.client['minio_secret'],
            secure = False,
            http_client=urllib3.ProxyManager( # Despite insecure request force https
                f"https://{self.client['minio_endpoint']}",
                cert_reqs='CERT_NONE'
            )
        )
        #print(self.client['mc'].bucket_exists('root-common'))

        # Check if bucket exists. Should never exist.
        if not self.client['mc'].bucket_exists(self.client['bucket_name']):
            print('Bucket does not exist. Trying to create it.')

            # Check oscar credentials
            if not self.client['oscar_endpoint'] or not self.client['oscar_access'] or not self.client['oscar_secret']:
                print('Missing OSCAR credentials. Please provide endpoint and access credentials to create the services.')
                return

            # Create Bucket.
            try:
                # Create bucket
                print('Creating bucket...')
                self.client['mc'].make_bucket(f"{self.client['bucket_name']}")
                print('Bucket created!')
            except Exception as e:
                print('Couldn\'t create bucket.')
                raise e

            # Create services asociated to the new bucket.
            try:
                self._service_creation()
            except Exception as e:
                print('Error creating services.')
                raise e
        else:
            print('ASD')
            # In current implementation bucket shouldn't exist. So, raise exception/
            raise Exception(('Bucket already exists.'))
            #print('Bucket already exists.')

        print('Done setting up OSCAR backend!')


    def _service_creation(self):
        """
        ASD
        """
        try:
            print('Creating services...')

            # Decide which services we are need to create depending on
            # the selected backend.
            if 'backend' in self.client:
                backend = self.client['backend']
            else:
                self.client['backend'] = 'tree_reduce'
                backend = 'tree_reduce'

            services = ['mapper']

            if backend == 'tree_reduce':
                services.append('reducer')
            elif backend == 'tree_v2_reduce':
                services.append('reducer-v2')
            else: # Coordinator
                services.append('coordinator')
                services.append('reducer-coord') # Reducer depending on coordinator writes.

            self.client['tasks'] = []
            self.client['services'] = []

            for service in services:
                    self.client['tasks'].append(self._create_service(service))

            print('Done creating services!')

        except Exception as e:
            print('Error creating services.')
            # print('Deleting associated bucket')
            # print(e)
            raise e


    def _create_service(self, service):
        """
        If needed, create services asynchronously at the beginning of the data frame  and
        check for the results of the service creation in `process_and_merge` to avoid waiting
        as much as possible.
        """
        print(f'Creating {service} service for bucket {self.client["bucket_name"]}')
        request_body = self._service_yaml_to_http(service)

        return(requests.post(f'{self.client["oscar_endpoint"]}/system/services',
                      auth=requests.auth.HTTPBasicAuth(self.client["oscar_access"],
                                                       self.client["oscar_secret"]),
                      json=request_body,
                      verify = False))


    def _service_yaml_to_http(self, function):
        # TODO: read from yaml template instead of hardcoding the dictionary?
        bucket_name = f'{self.client["bucket_name"]}'
        #print(bucket_name)

        # Path were OSCAR will trigger the service.
        trigger_path = ''
        out_prefixes = []
        if function == 'mapper':
            trigger_path = f"{bucket_name}/mapper-jobs"
            out_prefixes.append('partial-results')
        elif function == 'reducer' or function == 'reducer_v2':
            trigger_path = f"{bucket_name}/partial-results"
            out_prefixes.append('partial-results')
        elif function == 'reducer-coord':
            trigger_path = f'{bucket_name}/reducer-jobs'
            out_prefixes.append('partial-results')
        else: # We should be writting path for coordinator in this case.
            trigger_path = f'{bucket_name}/coord-config'
            # The coordinator doesnt write to the container file system
            # It writes to minio directly through the minio client
            # So, no out prefixes.


        print(f'Trigger path: {trigger_path}')
        script_suffix = ''

        unbenchmarked = ['reducer-coord', 'coordinator'] # Services not available for benchmarking
        if 'benchmarking' in  self.client and self.client['benchmarking'] == True and function not in unbenchmarked:
            out_prefixes.append('benchmarks')
        script_suffix = '-benchmark'

        print(f'Write prefixes: {out_prefixes}')


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
            #"memory": self.client['mem_val'],
            #"cpu": self.client['cpu_val'],
            "memory" : '3Gi',
            "cpu": '1',
            "image": f"ghcr.io/break95/root-{function}{script_suffix}",
            "alpine": False,
            "script": script,
            "input": [
                {
                    "storage_provider": "minio.default",
                    "path": trigger_path
                }
            ],
            "output": [
                {
                    "storage_provider": "minio.default",
                    "path": f"{bucket_name}",
                    "prefix": out_prefixes
                }
            ]
        }

        return http_body


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

        # NOTE: We check for the services to be ready before starting the
        # stopwatch as in a production implementation this functions should be ready
        # before hand.
        # If we have created the services during client instantiation,
        # check that they have been created succesfully.
        if 'tasks' in self.client:
                for task in self.client['tasks']:
                        print(task)

        # Start timer
        print('Starting timer')
        t = ROOT.TStopwatch()

        # Set up mapper and reducer functions.
        self._mapper_setup(mapper)
        self._reducer_setup(reducer)

        # Setup reducer job indices for uncoordinated reduction.
        if self.client['backend'] == 'tree_reduce':
            self._binary_reducer(ranges)
        elif self.client['backend'] == 'tree_v2_reduce':
            pass
        elif self.client['backend'] == 'coord_reduce':
            self._coord_reducer()
        else:
            raise Exception('Specified backend not supported.')

        # Write mappers jobs that will trigger the funcions.
        self._launch_mappers(ranges)



        if self.client['backend'] == 'tree_reduce':
            # Wait for final result
            start = ranges[0].id
            end = ranges[-1].id

            target_name = f'{start}_{end}'
            print(f'Target Name: {target_name}')

            with self.client['mc'].listen_bucket_notification(
                    self.client['bucket_name'],
                    prefix='partial-results/',
                    events=['s3:ObjectCreated:*']) as events:

                for event in events:
                    file_name = event['Records'][0]['s3']['object']['key'].split('/')[1]
                    print(f'File {file_name} written to partial-results folder.')
                    if file_name == target_name:
                        target_name = 'partial-results/' + target_name
                        break

        if self.client['backend'] == 'coord_reduce':
            print('Waiting for coordinator to write final result.')
            with self.client['mc'].listen_bucket_notification(
                    self.client['bucket_name'],
                    prefix='final-result/',
                    events=['s3:ObjectCreated:*']) as events:

                for event in events:
                    target_name = event['Records'][0]['s3']['object']['key']
                    break


        # Once the final result has been generate fetch it from
        final_result = self.get_object(target_name)

        # Stop timer
        self.client['ttp'] = round(t.RealTime(), 2)
        print(f'Time to plot: {self.client["ttp"]}')

        # Generate benchmark reports if requested.
        if self.client['benchmarking']:
                self.benchmark_report()

        #sleep(20)
        # Cleanup bucket and asociated services.
        self._cleanup()

        return final_result


    def _mapper_setup(self, mapper):
        mapper_bytes = cloudpickle.dumps(mapper)
        mapper_stream = io.BytesIO(mapper_bytes)
        self.client['mc'].put_object(self.client['bucket_name'],
                                     'functions/mapper',
                                     mapper_stream,
                                     length = len(mapper_bytes))


    def _launch_mappers(self, ranges):
        for rang in ranges:
            rang_bytes = cloudpickle.dumps(rang)
            rang_stream = io.BytesIO(rang_bytes)
            file_name = f'{rang.id}_{rang.id}'
            self.client['mc'].put_object(self.client['bucket_name'],
                                         f'mapper-jobs/{file_name}',
                                         rang_stream,
                                         length = len(rang_bytes))


    def _reducer_setup(self, reducer):
        reducer_bytes = cloudpickle.dumps(reducer)
        reducer_stream = io.BytesIO(reducer_bytes)
        self.client['mc'].put_object(self.client['bucket_name'],
                                     'functions/reducer',
                                     reducer_stream,
                                     length = len(reducer_bytes))


    def _binary_reducer(self, ranges):

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
        #print(reduce_indices)

        # Write Reduction Jobs
        for reducer_job in reduce_indices:
            self.client['mc'].put_object(self.client['bucket_name'],
                                         f'reducer-jobs/{reducer_job}',
                                         io.BytesIO(b'\xff'),
                                         length=1)


    def _coord_reducer(self):
        # Write file containing needed data for the coordinator.

        # Compute the number of reductions needed.
        #reduction_count = int(self.client['mapper_count'] / self.client['reduce_size'])
        #reduction_count = 0
        #tmp_var = self.client['mapper_count']

        #while tmp_var > 1: #TODO: check this loop
        #    tmp_var = int(floor(tmp_var) / self.client['reduce_size'])
        #    reduction_count += tmp_var

        phases_bytes = cloudpickle.dumps(self.client['reduction_phases'])
        phases_stream = io.BytesIO(phases_bytes)
        self.client['mc'].put_object(self.client['bucket_name'],
                                     'coord-config/coord_setup',
                                     phases_stream,
                                     length = len(phases_bytes))
        #with open(f'{self.client["uuid"]}.coord_setup', 'w') as file:
        #    file.write(f'{self.client["reduce_size"]}\n')
        #    file.write(f'{reduction_count}')

        # Write file to MINIO triggering the reducer coordinator.
        #self.client['mc'].fput_object(self.client['bucket_name'],
        #self.client['mc'].put_object(self.client['bucket_name'],
        #                             'coord-config/coord_setup',
        #                             io.BytesIO(),
        #                             f'{self.client["uuid"]}.coord_setup')
        #

    def _serverless_coord_reducer(self):
        pass


    def get_object(self, name):
        response = self.client['mc'].get_object(self.client['bucket_name'],
                                     name)
        response.release_conn()
        result = cloudpickle.loads(response.data)
        return result


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


    def benchmark_report(self):
        """
        Generate perfomance report based on results of job.
        """
        import glob

        # TODO: Filter duplicate reducers.
        bench_results = self.client['mc'].list_objects(self.client['bucket_name'],
                                                        'benchmarks/',
                                                        recursive=True)
        results = {'mapper': {},
                   'reducer': {}}

        reducer_counts = {}

        # Get benchmark results from MINIO.
        for obj in bench_results:
            tmp = obj.object_name
            parts = tmp.split('/')[1].split('_')

            bench_response = self.client['mc'].get_object(self.client['bucket_name'],
                                                          tmp)
            bench_bytes = bench_response.data
            bench_response.release_conn()

            name = f'{parts[1]}_{parts[2]}'
            if name in results[parts[0]]:
                results[parts[0]][name] = results[parts[0]][name] | {parts[3]: cloudpickle.loads(bench_bytes)}
            else:
                results[parts[0]][name] = {parts[3]: cloudpickle.loads(bench_bytes)}

            # Add working node
            results[parts[0]][name] = results[parts[0]][name] | {'node': parts[5]}

            # Check for repeated reduced calls.
            # For each write to bucket we produce a reducer so every reducer
            # (i.e. 0_1) is called two times. Producing a total of 10 files:
            # [start, end, net_start, net_end, usage] * 2.
            if parts[0] == 'reducer':
                if name in reducer_counts:
                    reducer_counts[name] = reducer_counts[name] + 1
                else:
                    reducer_counts[name] = 1

        #for key in reducer_counts:
            #if (reducer_counts[key] /10) > 1:
            #    print(f'{int(reducer_counts[key]/5) - 2} extra reducers invoked for for ')
            #print(f'Reducer {key} count: {reducer_counts[key] / 5}')

        import json # For some reason this in necessary.
        self._report_to_csv(json.loads(json.dumps(results)))


    def _report_to_csv(self, data_dict):
        job_id = self.client['mapper_count']
        folder = '' if self.client['folder'] is None else f'{self.client["folder"]}/'
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
            # System Network
            'net_bytes_sent', 'net_bytes_recv', 'net_packets_sent', 'net_packets_recv',
            'net_errin', 'net_errout', 'net_dropin', 'net_dropout',
            # Create time (only taken on start)
            'create_time',
            # Cluster node in which the function has been executed.
            'node'
        ]

        headings_usage = ['function', 'id', 'time', 'cpu_percent', 'mem_percent', 'node']

        csv_row = headings_process

        csv_f_proc = open(f'{folder}{job_id}_process.csv' , 'w', newline='')
        proc_writer = csv.writer(csv_f_proc, delimiter='|')
        proc_writer.writerow(headings_process)
        print(f'{job_id}_process.csv')

        csv_f_usage = open(f'{folder}{job_id}_usage.csv', 'w', newline='')
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

        # Write time to plot.
        csv_ttp = open(f'{folder}{job_id}_ttp.csv', 'w', newline='')
        ttp_writer = csv.writer(csv_ttp, delimiter='|')
        ttp_writer.writerow(['ttp'])
        ttp_writer.writerow([str(self.client['ttp'])])
        csv_ttp.close()


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
