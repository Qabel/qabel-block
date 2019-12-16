from __future__ import annotations
import datetime
from collections import namedtuple


User = namedtuple('User', ['user_id', 'is_active', 'quota', 'traffic_quota'])


def this_month():
    """Return datetime.date for the current month (day=1)."""
    return datetime.date.today().replace(day=1)
