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
TIME_IN_TRANSFER_META = Histogram('block_wait_for_transfer_meta',
                                  'Time spent retrieving meta-data (HEAD) of a file')
TIME_IN_TRANSFER_DELETE = Histogram('block_wait_for_transfer_delete',
                                    'Time spent deleting a file')

SUMMARY_S3_REQUESTS = Summary('block_s3_requests', 'Count and time of requests to s3')

REQ_RESPONSE = Histogram('block_response_time',
                         'Time to respond to a request')

HTTP_ERROR = Counter('block_access_denied', 'Number of requests that received a HTTP error response', ['reason'])
CONTENT_LENGTH_ERROR = Counter('content_length_error', 'Number of requests that were terminated due to too large '
                                                       'Content-Length')

COUNT_AUTH_CACHE_HITS = Counter('block_auth_cache_hits', 'Number of cache hits for auth requests')
COUNT_AUTH_CACHE_SETS = Counter('block_auth_cache_sets', 'Number of cache sets for auth requests')

TRAFFIC_RESPONSE = Counter('block_traffic_response', 'Download traffic')
TRAFFIC_REQUEST = Counter('block_traffic_request', 'Upload traffic')

DB_WAIT_FOR_CONNECTIONS = Counter('block_wait_database_connections',
                                  'Seconds waited for getting a connection')

TRAFFIC_BY_REQUEST = Summary('block_traffic_by_request',
                             'Traffic by individual request')

QUOTA_BY_REQUEST = Summary('block_quota_by_request',
                           'Quota change by request', ['type'])

WEBSOCKET_CONNECTIONS = Gauge('websocket_connections',
                              'Number of open WebSocket connections')

WEBSOCKET_CONNECTION_DURATION = Histogram('weboscket_connection_duration',
                                          'Time WebSocket connection is open')

WEBSOCKET_MESSAGES = Counter('websocket_messages',
                             'Number of sent WebSocket messages')

PUBSUB_PUBLISHED = Counter('pubsub_published',
                           'Messages published via pubsub')

PUBSUB_OPEN_CONNECTIONS = Gauge('pubsub_connections',
                                'Open connections to the pubsub broker (normally redis)')


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
