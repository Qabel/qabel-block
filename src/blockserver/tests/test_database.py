from blockserver.backend.database import AbstractUserDatabase, PostgresUserDatabase
from blockserver import server
import uuid
UID = 1


def test_default_quota():
    assert AbstractUserDatabase.DEFAULT_QUOTA > 0
    assert str(AbstractUserDatabase.DEFAULT_QUOTA) in PostgresUserDatabase.SCHEMA


def test_create_prefix(pg_db: PostgresUserDatabase):
    prefix = pg_db.create_prefix(UID)
    assert pg_db.has_prefix(UID, prefix)
    another_prefix = uuid.uuid4()
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
    assert isinstance(prefixes[0], uuid.UUID)
    assert set(prefixes) == {p1, p2}


def test_nonexistent_prefixes(pg_db):
    assert pg_db.get_prefixes(UID) == []


def test_idempotent_init_db(pg_db):
    pg_db.init_db()
    pg_db.init_db()
