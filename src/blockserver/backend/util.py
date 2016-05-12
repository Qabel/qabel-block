from collections import namedtuple


User = namedtuple('User', ['user_id', 'is_active', 'quota', 'traffic_quota'])
