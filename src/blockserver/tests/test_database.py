import pytest

from blockserver.backend.database import AbstractUserDatabase, PostgresUserDatabase
import uuid
UID = 1


def test_create_prefix(pg_db: PostgresUserDatabase):
    prefix = pg_db.create_prefix(UID)
    assert pg_db.has_prefix(UID, prefix)
    another_prefix = str(uuid.uuid4())
    assert not pg_db.has_prefix(UID, another_prefix)
    second_prefix = pg_db.create_prefix(UID)
    assert pg_db.has_prefix(UID, second_prefix)


def test_create_multiple_prefixes(pg_db: PostgresUserDatabase):
    prefix = pg_db.create_prefix(UID)
    prefix_2 = pg_db.create_prefix(UID)
    assert prefix != prefix_2


def test_assert_user_exists(pg_db):
    pg_db.assert_user_exists(UID)
    pg_db.assert_user_exists(UID)


def test_retrieve_prefixes(pg_db):
    p1 = pg_db.create_prefix(UID)
    p2 = pg_db.create_prefix(UID)
    prefixes = pg_db.get_prefixes(UID)
    assert set(prefixes) == {p1, p2}


def test_nonexistent_prefixes(pg_db):
    assert pg_db.get_prefixes(UID) == []


def test_used_space_inc(pg_db, user_id, prefix):
    size = 500
    second_prefix = pg_db.create_prefix(user_id)
    pg_db.update_size(prefix, size)
    assert pg_db.get_size(user_id)[1] == size

    pg_db.update_size(second_prefix, size)
    assert pg_db.get_size(user_id)[1]  == size * 2


def test_used_space_dec(pg_db, user_id, prefix):
    size = 500
    second_prefix = pg_db.create_prefix(user_id)
    pg_db.update_size(prefix, size)
    assert pg_db.get_size(user_id)[1] == size

    pg_db.update_size(second_prefix, -size)
    assert pg_db.get_size(user_id)[1] == 0


def test_traffic_for_prefix(pg_db, user_id, prefix):
    assert pg_db.get_traffic(user_id) == 0
    amount = 500
    pg_db.update_traffic(prefix, amount)
    assert pg_db.get_traffic(user_id) == amount


def test_quota(pg_db, user_id):
    assert pg_db.get_quota(user_id) == 2*1024**3
    pg_db.set_quota(user_id, 10)
    assert pg_db.get_quota(user_id) == 10


def test_set_quota(pg_db, user_id):
    pg_db.set_quota(user_id, 10)
    assert pg_db.get_quota(user_id) == 10


def test_quota_reached(pg_db, user_id, prefix):
    size = 10
    assert not pg_db.quota_reached(user_id, size)
    pg_db.set_quota(user_id, 10)
    assert not pg_db.quota_reached(user_id, size-1)
    assert pg_db.quota_reached(user_id, size)
    pg_db.update_size(prefix, 10)
    assert pg_db.quota_reached(user_id, 0)
    assert pg_db.quota_reached(user_id, size)


def test_traffic_by_prefix(pg_db, prefix):
    assert pg_db.get_traffic_by_prefix(prefix) == 0
    amount = 500
    pg_db.update_traffic(prefix, amount)
    assert pg_db.get_traffic_by_prefix(prefix) == amount


def test_quota_reached_by_large_file(pg_db, user_id, prefix):
    size = 3*1024**3
    assert pg_db.quota_reached(user_id, size)


def test_traffic_default(pg_db):
    assert pg_db.get_traffic_by_prefix("non existing prefix") == 0
