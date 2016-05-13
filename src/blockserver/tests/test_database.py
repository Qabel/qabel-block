import datetime

import psycopg2
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
    assert pg_db.get_size(user_id) == size

    pg_db.update_size(second_prefix, size)
    assert pg_db.get_size(user_id) == size * 2


def test_used_space_dec(pg_db, user_id, prefix):
    size = 500
    second_prefix = pg_db.create_prefix(user_id)
    pg_db.update_size(prefix, size)
    assert pg_db.get_size(user_id) == size

    pg_db.update_size(second_prefix, -size)
    assert pg_db.get_size(user_id) == 0


def test_traffic_for_prefix(pg_db, user_id, prefix):
    assert pg_db.get_traffic(user_id) == 0
    amount = 500
    pg_db.update_traffic(prefix, amount)
    assert pg_db.get_traffic(user_id) == amount
    different_amount = 123456
    pg_db.update_traffic(prefix, different_amount)
    assert pg_db.get_traffic(user_id) == amount + different_amount


def test_traffic_by_prefix(pg_db, prefix):
    assert pg_db.get_traffic_by_prefix(prefix) == 0
    amount = 500
    pg_db.update_traffic(prefix, amount)
    assert pg_db.get_traffic_by_prefix(prefix) == amount
    different_amount = 123456
    pg_db.update_traffic(prefix, different_amount)
    assert pg_db.get_traffic_by_prefix(prefix) == amount + different_amount


def test_traffic_by_prefix_and_month(pg_db, prefix, mocker):
    def normal_cycle():
        assert pg_db.get_traffic_by_prefix(prefix) == 0
        amount = 500
        pg_db.update_traffic(prefix, amount)
        assert pg_db.get_traffic_by_prefix(prefix) == amount
    normal_cycle()
    this_month = mocker.patch('blockserver.backend.util.this_month')
    today = datetime.date.today()
    this_month.return_value = today.replace(day=1, month=today.month + 1)
    normal_cycle()


def test_traffic_by_month(pg_db, prefix, mocker, user_id):
    def normal_cycle():
        assert pg_db.get_traffic(user_id) == 0
        amount = 500
        pg_db.update_traffic(prefix, amount)
        assert pg_db.get_traffic(user_id) == amount
    normal_cycle()
    this_month = mocker.patch('blockserver.backend.util.this_month')
    today = datetime.date.today()
    this_month.return_value = today.replace(day=1, month=today.month + 1)
    normal_cycle()


def test_traffic_integrity(pg_db, prefix, mocker):
    this_month = mocker.patch('blockserver.backend.util.this_month')
    this_month.return_value = datetime.date.today().replace(day=2)
    with pytest.raises(psycopg2.IntegrityError):
        pg_db.update_traffic(prefix, 123)


def test_traffic_default(pg_db):
    assert pg_db.get_traffic_by_prefix("non existing prefix") == 0
