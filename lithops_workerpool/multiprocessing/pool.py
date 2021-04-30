#
# Module providing the `Pool` class for managing a process pool
#
# multiprocessing/pool.py
#
# Copyright (c) 2006-2008, R Oudkerk
# Licensed to PSF under a Contributor Agreement.
#
# Modifications Copyright (c) 2020 Cloudlab URV
#

#
# Imports
#
import logging

from lithops_workerpool import WorkerPool
from lithops.multiprocessing import pool
from lithops.multiprocessing import util
from lithops.multiprocessing import config as mp_config
from lithops.multiprocessing.process import CloudWorker

logger = logging.getLogger(__name__)


class StatefulPool:
    def __init__(self, processes=None, initializer=None, initargs=None, maxtasksperchild=None, context=None, **kwargs):
        self._state = pool.RUN
        extra_env = mp_config.get_parameter(mp_config.ENV_VARS)
        print(extra_env)
        self._pool_executor = WorkerPool(workers=processes,
                                         init_function=initializer,
                                         init_args=initargs,
                                         extra_env=extra_env,
                                         **kwargs)

    def apply(self, func, args=(), kwds={}):
        if kwds and not args:
            args = {}
        return self.apply_async(func, args, kwds).get()

    def map(self, func, iterable, chunksize=None):
        return self._map_async(func, iterable, chunksize).get()

    def starmap(self, func, iterable, chunksize=None):
        return self._map_async(func, iterable, chunksize=chunksize, starmap=True).get()

    def starmap_async(self, func, iterable, chunksize=None, callback=None, error_callback=None):
        return self._map_async(func, iterable, chunksize=chunksize, callback=callback, error_callback=error_callback,
                               starmap=True)

    def imap(self, func, iterable, chunksize=1):
        res = self.map(func, iterable, chunksize=chunksize)
        return pool.IMapIterator(res)

    def imap_unordered(self, func, iterable, chunksize=1):
        res = self.map(func, iterable, chunksize=chunksize)
        return pool.IMapIterator(res)

    def apply_async(self, func, args=(), kwds={}, callback=None, error_callback=None):
        raise NotImplementedError

    def map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None):
        return self._map_async(func, iterable, chunksize, callback, error_callback)

    def _map_async(self, func, iterable, chunksize=None, callback=None, error_callback=None, starmap=False):
        if self._state != pool.RUN:
            raise ValueError("Pool not running")
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        cloud_worker = CloudWorker(func=func)

        if not starmap:
            fmt_args = [{'args': (args,), 'kwargs': {}} for args in iterable]
        else:
            fmt_args = [{'args': args, 'kwargs': {}} for args in iterable]

        if mp_config.get_parameter(mp_config.STREAM_STDOUT):
            stream = self._pool_executor.pool_id
            logger.debug('Log streaming enabled, stream name: {}'.format(stream))
            self._remote_logger = util.RemoteLoggingFeed(stream)
            self._remote_logger.start()
            cloud_worker.log_stream = stream

        futures = self._pool_executor.apply_map(map_function=cloud_worker, map_iterdata=fmt_args)

        result = pool.MapResult(self._pool_executor, futures, callback, error_callback)

        return result

    def __reduce__(self):
        raise NotImplementedError('pool objects cannot be passed between processes or pickled')

    def close(self):
        logger.debug('closing pool')
        self._pool_executor.close()
        if self._state == pool.RUN:
            self._state = pool.CLOSE

    def terminate(self):
        logger.debug('terminating pool')
        self._state = pool.TERMINATE

    def join(self):
        logger.debug('joining pool')
        assert self._state in (pool.CLOSE, pool.TERMINATE)
        self._pool_executor.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.terminate()
