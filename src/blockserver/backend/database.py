from typing import List
import psycopg2
import psycopg2.extensions
from abc import abstractmethod, ABC
from uuid import uuid4
import psycopg2.extras
psycopg2.extras.register_uuid()
from contextlib import contextmanager


class AbstractUserDatabase(ABC):

    DEFAULT_QUOTA = 2*1024*1024*8

    @abstractmethod
    def init_db(self):
        pass

    @abstractmethod
    def drop_db(self):
        pass

    @abstractmethod
    def create_prefix(self, user_id: int) -> str:
        pass

    @abstractmethod
    def has_prefix(self, user_id: int, prefix: str) -> bool:
        pass

    @abstractmethod
    def get_prefixes(self, user_id: int) -> List[str]:
        pass


class PostgresUserDatabase(AbstractUserDatabase):

    VERSION = 1

    BASE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS version (
    id integer PRIMARY KEY
    )"""

    SCHEMA = """
    CREATE TABLE users (
    id integer PRIMARY  KEY,
    max_quota integer DEFAULT {0},
    download_traffic bigint DEFAULT 0,
    size bigint DEFAULT 0,
    prefixes CHARACTER(36)[]
    );
    CREATE INDEX prefix_idx ON users USING GIN (prefixes);
    """.format(AbstractUserDatabase.DEFAULT_QUOTA)

    def __init__(self, connection: psycopg2.extensions.connection):
        self.connection = connection

    @contextmanager
    def _cur(self):
        with self.connection:
            yield self.connection.cursor()  # type: psycopg2.extensions.cursor

    def init_db(self):
        with self.connection:
            cur = self.connection.cursor()
            cur.execute(self.BASE_SCHEMA)
            cur.execute('SELECT MAX(id) FROM version')
            result = cur.fetchone()
            version = result[0]
            if version is None:
                version = 0
            if self.VERSION > version:
                self._migrate(cur, result[0], self.VERSION)

    def drop_db(self):
        with self._cur() as cur:
            cur.execute('DROP TABLE IF EXISTS users, version')

    def create_prefix(self, user_id: int) -> str:
        self.assert_user_exists(user_id)
        with self._cur() as cur:
            prefix = str(uuid4())
            cur.execute(
                'UPDATE users SET prefixes = prefixes || %s::CHARACTER(36) WHERE id=%s',
                (prefix, user_id))
            return prefix

    def assert_user_exists(self, user_id):
        with self._cur() as cur:
            try:
                cur.execute(
                        'INSERT INTO users (id) VALUES (%s)',
                        (user_id,))
            except psycopg2.IntegrityError:
                pass

    def has_prefix(self, user_id: int, prefix: str) -> bool:
        with self._cur() as cur:
            cur.execute(
                'SELECT 1 FROM users WHERE id=%s AND ARRAY[%s::CHARACTER(36)] <@ prefixes',
                (user_id, prefix))
            return cur.rowcount == 1

    def get_prefixes(self, user_id: int) -> List[str]:
        with self._cur() as cur:
            cur.execute(
                'SELECT prefixes FROM users WHERE id=%s',
                (user_id,))
            result = cur.fetchone()
            if result is None:
                return []
            else:
                return result[0]

    def update_size(self, prefix: str, change: int):
        with self._cur() as cur:
            cur.execute(
                'UPDATE users SET size = size + %s WHERE %s = ANY (prefixes)',
                (change, prefix))

    def get_size(self, prefix: str) -> int:
        with self._cur() as cur:
            cur.execute(
                'SELECT size FROM users WHERE %s = ANY (prefixes)',
                (prefix,))
            result = cur.fetchone()
            if result is None:
                return 0
            else:
                return result[0]

    def _migrate(self, cur, from_version, to_version):
        cur.execute(self.SCHEMA)
        cur.execute('INSERT INTO version (id) VALUES (%s)', (to_version,))

