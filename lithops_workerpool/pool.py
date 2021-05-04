#
# (C) Copyright Cloudlab URV 2021
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import redis
import time
import copy
import msgpack
import threading
from concurrent.futures import ThreadPoolExecutor

import lithops
from lithops.config import extract_storage_config
from lithops.executors import FunctionExecutor
from lithops.constants import SERVERLESS
from lithops.future import ResponseFuture
from lithops.utils import uuid_str
from .job import create_map_job

logger = logging.getLogger(lithops.__name__)


class WorkerPool:
    def __init__(self,
                 workers,
                 init_function=None,
                 init_args=None,
                 config=None,
                 backend=None,
                 storage=None,
                 runtime=None,
                 runtime_memory=None,
                 runtime_timeout=None,
                 wait_workers=False,
                 worker_timeout=None,
                 worker_processes=None,
                 bootstrap_workers=False,
                 extra_env=None,
                 include_modules=None,
                 exclude_modules=None,
                 log_level=False):

        self.pool_id = uuid_str()[:8]
        self._closed = False
        self._max_workers = workers
        self._workers_up = 0
        self._jobs_queue = 'lithops-jobs-{}'.format(self.pool_id)
        self._host_queue = 'lithops-host-{}'.format(self.pool_id)
        self._ctl_queue = 'lithops-ctl-{}'.format(self.pool_id)
        self._worker_processes = worker_processes or 1
        self._worker_timeout = worker_timeout or 5
        self._wait_workers = wait_workers
        self._fexec = FunctionExecutor(mode=SERVERLESS,
                                       config=config,
                                       backend=backend,
                                       storage=storage,
                                       runtime=runtime,
                                       runtime_memory=runtime_memory,
                                       workers=workers,
                                       remote_invoker=None,
                                       log_level=log_level)
        self._storage_config = extract_storage_config(self._fexec.config)
        self._thread_pool = ThreadPoolExecutor(max_workers=1024)

        if self._fexec.config['lithops']['mode'] != SERVERLESS:
            raise Exception('Function Pool not available for mode {}'.format(self._fexec.config['lithops']['mode']))

        # Select worker runtime
        self._runtime_name = self._fexec.config[SERVERLESS]['runtime']
        self._runtime_memory = self._fexec.config[SERVERLESS]['runtime_memory']
        self._runtime_timeout = runtime_timeout or self._fexec.config[SERVERLESS]['runtime_timeout']
        self._runtime_meta = self._fexec.invoker.select_runtime(None, self._runtime_memory)

        # Setup init function
        self._init_function = init_function or (lambda: None)
        self._init_args = init_args or ()
        self._init_extra_env = extra_env
        self._init_include_mods = include_modules or []
        self._init_exclude_mods = exclude_modules or []

        # Create redis client
        if 'redis' not in self._fexec.config:
            raise Exception("'redis' parameters required in configuration.")
        self._redis_client = redis.Redis(**self._fexec.config['redis'])
        self._pubsub = self._redis_client.pubsub()
        self._worker_monitor_thread = threading.Thread(target=self._worker_monitor)
        self._worker_monitor_thread.start()
        self._monitoring = True

        # Start workers
        if bootstrap_workers:
            self.populate(workers)

    @property
    def workers_up(self):
        return self._workers_up

    def _worker_monitor(self):
        logger.debug('Pool monitor thread started')
        self._pubsub.subscribe(self._host_queue)
        while self._monitoring:
            msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=3)
            if msg is None:
                continue
            msg_data = msg['data'].decode('utf-8')
            logger.debug('Worker {} finished'.format(msg_data))
            self._workers_up -= 1
        logger.debug('Pool monitor thread finished')

    def _wait_worker_futures(self, worker_futures):
        self._fexec.wait(fs=worker_futures)

    def _init_worker(self, worker_id, payload):
        print('invoke')
        start = time.time()
        activation_id = self._fexec.invoker.compute_handler.invoke(runtime_name=self._runtime_name,
                                                                   memory=self._runtime_memory,
                                                                   payload=payload)
        resp_time = format(round(time.time() - start, 3), '.3f')

        logger.debug('ExecutorID {} | Worker {} invoked ({}s) - Activation ID: {}'.format(self._fexec.executor_id,
                                                                                          worker_id, resp_time,
                                                                                          activation_id))

    def _schedule_job(self, job):
        t0 = time.time()
        job_payload = self._fexec.invoker._create_payload(job)
        job_payload['func_hash'] = job.func_hash
        packed_payloads = []
        for call_id in range(job.total_calls):
            payload = copy.deepcopy(job_payload)
            payload['call_id'] = '{:05d}'.format(call_id)
            payload['data_byte_ranges'] = [job.data_byte_ranges[int(call_id)]]
            packed_payload = msgpack.packb(payload)
            packed_payloads.append(packed_payload)
        self._redis_client.lpush(self._jobs_queue, *packed_payloads)
        t1 = time.time()
        resp_time = format(round(t1 - t0, 3), '.3f')
        logger.debug('ExecutorID {} | JobID {} scheduled ({} total calls) in {}s'.format(job.executor_id, job.job_id,
                                                                                         job.total_calls, resp_time))

    def populate(self, workers):
        assert not self._closed

        new_workers = workers - self._workers_up

        if new_workers + self._workers_up > self._max_workers:
            new_workers = self._max_workers - self._workers_up

        if new_workers > 0 and self._workers_up < self._max_workers:
            logger.info('Populating worker pool -- starting {} new workers'.format(new_workers))
            job_id = self._fexec._create_job_id('I')
            self.last_call = 'init'
            init_job = create_map_job(config=self._fexec.config,
                                      internal_storage=self._fexec.internal_storage,
                                      executor_id=self._fexec.executor_id,
                                      job_id=job_id,
                                      map_function=self._init_function,
                                      iterdata=[self._init_args],
                                      runtime_meta=self._runtime_meta,
                                      runtime_memory=self._runtime_memory,
                                      extra_env=self._init_extra_env,
                                      include_modules=self._init_include_mods,
                                      exclude_modules=self._init_exclude_mods,
                                      execution_timeout=self._runtime_timeout,
                                      worker_processes=1)
            init_job.runtime_name = self._runtime_name
            init_job.total_calls = new_workers

            # Create all futures
            init_job_futures = []
            for i in range(init_job.total_calls):
                call_id = "{:05d}".format(i)
                fut = ResponseFuture(call_id, init_job, init_job.metadata.copy(), self._storage_config)
                fut._set_state(ResponseFuture.State.Invoked)
                init_job_futures.append(fut)

            # Create invocation payload
            job_payloads = []
            for _ in range(init_job.total_calls):
                job_payload = copy.deepcopy(self._fexec.invoker._create_payload(init_job))
                job_payload['func_hash'] = init_job.func_hash
                job_payloads.append(job_payload)
            call_ids = ["{:05d}".format(i) for i in range(init_job.total_calls)]
            data_byte_range = init_job.data_byte_ranges.pop()

            call_futures = []
            for job_payload, call_id in zip(job_payloads, call_ids):
                job_payload['call_id'] = call_id
                job_payload['data_byte_ranges'] = [data_byte_range]
                payload = {
                    'job_queue_worker': {
                        'pool_id': self.pool_id,
                        'job_queue': self._jobs_queue,
                        'host_queue': self._host_queue,
                        'ctl_queue': self._ctl_queue,
                        'worker_id': '{}-{}'.format(self.pool_id, call_id),
                        'worker_processes': self._worker_processes,
                        'worker_timeout': self._worker_timeout
                    },
                    'init_job': job_payload,
                    'log_level': self._fexec.config['lithops']['log_level'],
                    'config': self._fexec.config,
                    'runtime_name': self._runtime_name,
                    'runtime_memory': self._runtime_memory
                }
                future = self._thread_pool.submit(self._init_worker, call_id, payload)
                call_futures.append(future)

            [fut.result() for fut in call_futures]
            if self._wait_workers:
                self._fexec.wait(fs=init_job_futures)
            else:
                self._thread_pool.submit(self._fexec.wait, init_job_futures)
            self._workers_up += new_workers

    def apply_map(self,
                  map_function,
                  map_iterdata,
                  chunksize=None,
                  extra_args=None,
                  extra_env=None,
                  chunk_size=None,
                  chunk_n=None,
                  obj_chunk_size=None,
                  obj_chunk_number=None,
                  invoke_pool_threads=None,
                  include_modules=None,
                  exclude_modules=None):
        assert not self._closed

        job_id = self._fexec._create_job_id('M')
        self._fexec.last_call = 'map'

        job = create_map_job(self._fexec.config,
                             self._fexec.internal_storage,
                             self._fexec.executor_id,
                             job_id,
                             map_function=map_function,
                             iterdata=map_iterdata,
                             chunksize=chunksize,
                             worker_processes=self._worker_processes,
                             runtime_meta=self._runtime_meta,
                             runtime_memory=self._runtime_memory,
                             extra_env=extra_env,
                             include_modules=include_modules,
                             exclude_modules=exclude_modules,
                             execution_timeout=self._runtime_timeout,
                             extra_args=extra_args,
                             chunk_size=chunk_size,
                             chunk_n=chunk_n,
                             obj_chunk_size=obj_chunk_size,
                             obj_chunk_number=obj_chunk_number,
                             invoke_pool_threads=invoke_pool_threads)
        job.runtime_name = self._runtime_name

        self._schedule_job(job)

        effective_workers = (job.total_calls // self._worker_processes) + (job.total_calls % self._worker_processes)
        self.populate(effective_workers)

        futures = []
        for i in range(job.total_calls):
            call_id = "{:05d}".format(i)
            fut = ResponseFuture(call_id, job,
                                 job.metadata.copy(),
                                 self._storage_config)
            fut._set_state(ResponseFuture.State.Invoked)
            futures.append(fut)

        map_result = JobResult(executor=self._fexec, futures=futures)
        return map_result

    def close(self):
        assert not self._closed
        logger.debug('Stopping {} running workers'.format(self._workers_up))
        self._redis_client.publish(self._ctl_queue, 'stop plis')
        self._closed = True
        while self._workers_up > 0:
            time.sleep(0.5)
        self._monitoring = False
        logger.debug('Pool {} is now closed'.format(self.pool_id))

    def join(self):
        # self._host_channel.stop_consuming()
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.join()


class JobResult:
    def __init__(self, executor, futures):
        self._executor = executor
        self._futures = futures

    def wait(self):
        self._executor.wait(fs=self._futures)

    def result(self):
        return self._executor.get_result(fs=self._futures)

