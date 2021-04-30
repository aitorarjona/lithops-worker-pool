#
# (C) Copyright PyWren Team 2018
# (C) Copyright IBM Corp. 2020
# (C) Copyright Cloudlab URV 2020
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

import os
import sys
import zlib
import pika
import time
import json
import base64
import pickle
import logging
import traceback
import msgpack
import threading
from multiprocessing import Process, Pipe
from distutils.util import strtobool
from tblib import pickling_support
from types import SimpleNamespace

from .taskrunner import TaskRunner
from .utils import get_memory_usage, get_function_and_modules, get_function_data

from lithops.version import __version__
from lithops.config import extract_storage_config
from lithops.storage import InternalStorage
from lithops.constants import JOBS_PREFIX, LITHOPS_TEMP_DIR
from lithops.utils import sizeof_fmt, setup_lithops_logger, is_unix_system
from lithops.storage.utils import create_status_key, create_job_key, create_init_key

pickling_support.install()

logger = logging.getLogger(__name__)


def worker_handler(payload):
    worker_meta = SimpleNamespace(**payload['job_queue_worker'])
    config = payload['config']

    if 'user_agent' in config['redis']:
        del config['redis']['user_agent']
    redis_client = payload.Redis(**config['redis'])

    storage_config = extract_storage_config(config)
    internal_storage = InternalStorage(storage_config)

    os.environ['TASK_RUNNER_THREAD'] = 'True'

    # Run init task
    init_task = SimpleNamespace(**payload['init_job'])
    worker_run_task(init_task, internal_storage)

    worker_running = True
    running_tasks = 0

    def consumer_loop():
        nonlocal worker_running, running_tasks
        logger.debug('Consumer loop thread start')
        try:
            last_task_run_timestamp = time.time()
            while worker_running:
                logger.debug('Waiting for new task...')
                item = redis_client.blpop(worker_meta.job_queue, timeout=worker_meta.worker_timeout)
                if item:
                    _, packed_payload = item
                    task_dict = msgpack.unpackb(packed_payload)
                    task = SimpleNamespace(**task_dict)
                    logger.info('Going to execute task {}-{}'.format(task.job_key, task.call_id))
                    running_tasks += 1
                    worker_run_task(task, internal_storage)
                    running_tasks -= 1
                    last_task_run_timestamp = time.time()
                elapsed = time.time() - last_task_run_timestamp
                if elapsed > worker_meta.worker_timeout:
                    logger.info('No new tasks after {:.3f} seconds, shutting down worker...'.format(elapsed))
                    worker_running = False
        except Exception as e:
            exception = traceback.format_exc()
            logger.error(exception)
            worker_running = False
        logger.debug('Consumer loop thread finished')

    consumer_loop_thread = threading.Thread(target=consumer_loop)
    consumer_loop_thread.daemon = True
    consumer_loop_thread.start()

    pubsub = redis_client.pubsub()
    pubsub.subscribe(worker_meta.ctl_queue)
    while worker_running:
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if msg is not None:
            print(msg)
            worker_running = False

    redis_client.publish(worker_meta.host_queue, worker_meta.worker_id)
    logger.debug('Worker end')


def run_task(task, internal_storage):
    """
    Runs a single job within a separate process
    """
    start_tstamp = time.time()
    setup_lithops_logger(task.log_level)

    backend = os.environ.get('__LITHOPS_BACKEND', '')
    logger.info("Lithops v{} - Starting {} execution".format(__version__, backend))
    logger.info("Execution ID: {}/{}".format(task.job_key, task.id))

    if task.runtime_memory:
        logger.debug('Runtime: {} - Memory: {}MB - Timeout: {} seconds'
                     .format(task.runtime_name, task.runtime_memory, task.execution_timeout))
    else:
        logger.debug('Runtime: {} - Timeout: {} seconds'.format(task.runtime_name, task.execution_timeout))

    env = task.extra_env
    env['LITHOPS_WORKER'] = 'True'
    env['PYTHONUNBUFFERED'] = 'True'
    env['LITHOPS_CONFIG'] = json.dumps(task.config)
    env['__LITHOPS_SESSION_ID'] = '-'.join([task.job_key, task.id])
    os.environ.update(env)

    call_status = CallStatus(task.config, internal_storage)
    call_status.response['worker_start_tstamp'] = start_tstamp
    call_status.response['host_submit_tstamp'] = task.host_submit_tstamp
    call_status.response['call_id'] = task.id
    call_status.response['job_id'] = task.job_id
    call_status.response['executor_id'] = task.executor_id

    show_memory_peak = strtobool(os.environ.get('SHOW_MEMORY_PEAK', 'False'))

    try:
        # send init status event
        call_status.send('__init__')

        if show_memory_peak:
            mm_handler_conn, mm_conn = Pipe()
            memory_monitor = Thread(target=memory_monitor_worker, args=(mm_conn,))
            memory_monitor.start()

        task.stats_file = os.path.join(task.task_dir, 'task_stats.txt')
        handler_conn, jobrunner_conn = Pipe()
        taskrunner = TaskRunner(task, jobrunner_conn, internal_storage)
        logger.debug('Starting TaskRunner process')
        jrp
        if is_unix_system() or strtobool(os.environ.get('TASK_RUNNER_THREAD', "False")):
            jrp = Thread(target=taskrunner.run)
        else:
            jrp = Process(target=taskrunner.run)
        jrp.start()

        jrp.join(task.execution_timeout)
        logger.debug('TaskRunner process finished')

        if jrp.is_alive():
            # If process is still alive after jr.join(job_max_runtime), kill it
            try:
                jrp.terminate()
            except Exception:
                # thread does not have terminate method
                pass
            msg = ('Function exceeded maximum time of {} seconds and was '
                   'killed'.format(task.execution_timeout))
            raise TimeoutError('HANDLER', msg)

        if show_memory_peak:
            mm_handler_conn.send('STOP')
            memory_monitor.join()
            peak_memory_usage = int(mm_handler_conn.recv())
            logger.info("Peak memory usage: {}".format(sizeof_fmt(peak_memory_usage)))
            call_status.response['peak_memory_usage'] = peak_memory_usage

        if not handler_conn.poll():
            logger.error('No completion message received from JobRunner process')
            logger.debug('Assuming memory overflow...')
            # Only 1 message is returned by jobrunner when it finishes.
            # If no message, this means that the jobrunner process was killed.
            # 99% of times the jobrunner is killed due an OOM, so we assume here an OOM.
            msg = 'Function exceeded maximum memory and was killed'
            raise MemoryError('HANDLER', msg)

        if os.path.exists(task.stats_file):
            with open(task.stats_file, 'r') as fid:
                for l in fid.readlines():
                    key, value = l.strip().split(" ", 1)
                    try:
                        call_status.response[key] = float(value)
                    except Exception:
                        call_status.response[key] = value
                    if key in ['exception', 'exc_pickle_fail', 'result', 'new_futures']:
                        call_status.response[key] = eval(value)

    except Exception:
        # internal runtime exceptions
        print('----------------------- EXCEPTION !-----------------------')
        traceback.print_exc(file=sys.stdout)
        print('----------------------------------------------------------')
        call_status.response['exception'] = True

        pickled_exc = pickle.dumps(sys.exc_info())
        pickle.loads(pickled_exc)  # this is just to make sure they can be unpickled
        call_status.response['exc_info'] = str(pickled_exc)

    finally:
        call_status.response['worker_end_tstamp'] = time.time()

        # Flush log stream and save it to the call status
        task.log_stream.flush()
        with open(task.log_file, 'rb') as lf:
            log_str = base64.b64encode(zlib.compress(lf.read())).decode()
            call_status.response['logs'] = log_str

        call_status.send('__end__')

        # Unset specific env vars
        for key in task.extra_env:
            os.environ.pop(key, None)
        os.environ.pop('__LITHOPS_TOTAL_EXECUTORS', None)

        logger.info("Finished")


class CallStatus:

    def __init__(self, lithops_config, internal_storage):
        self.config = lithops_config
        self.rabbitmq_monitor = self.config['lithops'].get('rabbitmq_monitor', False)
        self.store_status = strtobool(os.environ.get('__LITHOPS_STORE_STATUS', 'True'))
        self.internal_storage = internal_storage
        self.response = {
            'exception': False,
            'activation_id': os.environ.get('__LITHOPS_ACTIVATION_ID'),
            'python_version': os.environ.get("PYTHON_VERSION")
        }

    def send(self, event_type):
        self.response['type'] = event_type
        if self.store_status:
            if self.rabbitmq_monitor:
                self._send_status_rabbitmq()
            if not self.rabbitmq_monitor or event_type == '__end__':
                self._send_status_os()

    def _send_status_os(self):
        """
        Send the status event to the Object Storage
        """
        executor_id = self.response['executor_id']
        job_id = self.response['job_id']
        call_id = self.response['call_id']
        act_id = self.response['activation_id']

        if self.response['type'] == '__init__':
            init_key = create_init_key(JOBS_PREFIX, executor_id, job_id, call_id, act_id)
            self.internal_storage.put_data(init_key, '')

        elif self.response['type'] == '__end__':
            status_key = create_status_key(JOBS_PREFIX, executor_id, job_id, call_id)
            dmpd_response_status = json.dumps(self.response)
            drs = sizeof_fmt(len(dmpd_response_status))
            logger.info("Storing execution stats - Size: {}".format(drs))
            self.internal_storage.put_data(status_key, dmpd_response_status)

    def _send_status_rabbitmq(self):
        """
        Send the status event to RabbitMQ
        """
        dmpd_response_status = json.dumps(self.response)
        drs = sizeof_fmt(len(dmpd_response_status))

        executor_id = self.response['executor_id']
        job_id = self.response['job_id']

        rabbit_amqp_url = self.config['rabbitmq'].get('amqp_url')
        status_sent = False
        output_query_count = 0
        params = pika.URLParameters(rabbit_amqp_url)
        job_key = create_job_key(executor_id, job_id)
        exchange = 'lithops-{}'.format(job_key)

        while not status_sent and output_query_count < 5:
            output_query_count = output_query_count + 1
            try:
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.exchange_declare(exchange=exchange, exchange_type='fanout', auto_delete=False)
                channel.basic_publish(exchange=exchange, routing_key='',
                                      body=dmpd_response_status)
                connection.close()
                logger.info("Execution status sent to rabbitmq - Size: {}".format(drs))
                status_sent = True
            except Exception as e:
                logger.error("Unable to send status to rabbitmq")
                logger.error(str(e))
                logger.info('Retrying to send status to rabbitmq')
                time.sleep(0.2)


def memory_monitor_worker(mm_conn, delay=0.01):
    peak = 0

    logger.debug("Starting memory monitor")

    def make_measurement(peak):
        mem = get_memory_usage(formatted=False) + 5 * 1024 ** 2
        if mem > peak:
            peak = mem
        return peak

    while not mm_conn.poll(delay):
        try:
            peak = make_measurement(peak)
        except Exception:
            break

    try:
        peak = make_measurement(peak)
    except Exception as e:
        logger.error('Memory monitor: {}'.format(e))
    mm_conn.send(peak)


def worker_run_task(task, internal_storage):
    bucket = task.config['lithops']['storage_bucket']
    task.task_dir = os.path.join(LITHOPS_TEMP_DIR, bucket, JOBS_PREFIX, task.job_key, task.call_id)
    task.log_file = os.path.join(task.task_dir, 'execution.log')
    os.makedirs(task.task_dir, exist_ok=True)
    open(task.log_file, 'a').close()
    task.log_stream = sys.stdout
    task.id = task.call_id
    task.func = get_function_and_modules(task, internal_storage)
    job_data = get_function_data(task, internal_storage)
    task.data = job_data.pop()
    run_job(task, internal_storage)


def pool_worker_handler(args):
    pass
