from typing import List
import psycopg2
import psycopg2.extensions
from abc import abstractmethod, ABC
from uuid import UUID, uuid4
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
    def create_prefix(self, user_id: int) -> UUID:
        pass

    @abstractmethod
    def has_prefix(self, user_id: int, prefix: UUID) -> bool:
        pass

    @abstractmethod
    def get_prefixes(self, user_id: int) -> List[UUID]:
        pass


class PostgresUserDatabase(AbstractUserDatabase):

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS users (
    id integer PRIMARY  KEY,
    max_quota integer DEFAULT {0},
    download_traffic bigint DEFAULT 0,
    size bigint DEFAULT 0,
    prefixes uuid[]
    )
    """.format(AbstractUserDatabase.DEFAULT_QUOTA)

    def __init__(self, connection: psycopg2.extensions.connection):
        self.connection = connection

    @contextmanager
    def cur(self):
        with self.connection:
            yield self.connection.cursor()  # type: psycopg2.extensions.cursor

    def init_db(self):
        with self.connection:
            cur = self.connection.cursor()
            cur.execute(self.SCHEMA)

    def drop_db(self):
        with self.cur() as cur:
            cur.execute('DROP TABLE IF EXISTS users')

    def create_prefix(self, user_id: int) -> UUID:
        self.assert_user_exists(user_id)
        with self.cur() as cur:
            prefix = uuid4()
            cur.execute(
                'UPDATE users SET prefixes = prefixes || %s WHERE id=%s',
                (prefix, user_id))
            return prefix

    def assert_user_exists(self, user_id):
        with self.cur() as cur:
            try:
                cur.execute(
                        'INSERT INTO users (id) VALUES (%s)',
                        (user_id,))
            except psycopg2.IntegrityError:
                pass

    def has_prefix(self, user_id: int, prefix: UUID) -> bool:
        with self.cur() as cur:
            cur.execute(
                'SELECT 1 FROM users WHERE id=%s AND %s = ANY (prefixes)',
                (user_id, prefix))
            return cur.rowcount == 1

    def get_prefixes(self, user_id: int) -> List[UUID]:
        with self.cur() as cur:
            cur.execute(
                'SELECT prefixes FROM users WHERE id=%s',
                (user_id,))
            result = cur.fetchone()
            if result is None:
                return []
            else:
                return result[0]
