from time import perf_counter

import asyncio
import wrapt
from prometheus_client import Gauge, Histogram, Counter, Summary

REQ_IN_PROGRESS = Gauge('block_in_progress_requests',
                        'Number of requests that are in progress')

WAIT_FOR_AUTH = Histogram('block_wait_for_auth',
                          'Time spent waiting for answers from the auth resource')

TIME_IN_TRANSFER_STORE = Histogram('block_wait_for_transfer_store',
                                   'Time spent storing a file')
TIME_IN_TRANSFER_RETRIEVE = Histogram('block_wait_for_transfer_retrieve',
                                      'Time spent retrieving a file')
TIME_IN_TRANSFER_DELETE = Histogram('block_wait_for_transfer_delete',
                                    'Time spent deleting a file')

SUMMARY_S3_REQUESTS = Summary('block_s3_requests', 'Count ant time of requests to s3')


REQ_RESPONSE = Histogram('block_response_time',
                         'Time to respond to a request')

COUNT_ACCESS_DENIED = Counter('block_access_denied', 'Number of requests that received a 403')

COUNT_AUTH_CACHE_HITS = Counter('block_auth_cache_hits', 'Number of cache hits for auth requests')
COUNT_AUTH_CACHE_SETS = Counter('block_auth_cache_sets', 'Number of cache sets for auth requests')

TRAFFIC_RESPONSE = Counter('block_traffic_response', 'Download traffic')
TRAFFIC_REQUEST = Counter('block_traffic_request', 'Upload traffic')

DB_WAIT_FOR_CONNECTIONS = Counter('block_wait_database_connections',
                                  'Seconds waitet for getting a connection')


def time(metric):
    @asyncio.coroutine
    @wrapt.decorator
    def decorator(func, _, args, kw):
        def observe():
            metric.observe(perf_counter() - start_time)

        start_time = perf_counter()
        try:
            rv = yield from func(*args, **kw)
            return rv
        finally:
            observe()

    return decorator


def time_future(metric, future):
    @asyncio.coroutine
    @wrapt.decorator
    def measure():
        def observe():
            metric.observe(perf_counter() - start_time)

        try:
            rv = yield from future
            return rv
        finally:
            observe()

    start_time = perf_counter()
    return measure()
