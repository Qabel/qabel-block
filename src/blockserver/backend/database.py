from typing import List
import psycopg2
import psycopg2.extensions
from abc import abstractmethod, ABC
from uuid import uuid4
from contextlib import contextmanager


class AbstractUserDatabase(ABC):

    @abstractmethod
    def create_prefix(self, user_id: int) -> str:
        pass

    @abstractmethod
    def has_prefix(self, user_id: int, prefix: str) -> bool:
        pass

    @abstractmethod
    def get_prefixes(self, user_id: int) -> List[str]:
        pass

    @abstractmethod
    def get_size(self, user_id: int) -> int:
        pass

    @abstractmethod
    def get_traffic(self, user_id: int) -> int:
        pass

    @abstractmethod
    def update_size(self, prefix: str, size: int):
        pass

    @abstractmethod
    def update_traffic(self, prefix: str, traffic: int):
        pass


class PostgresUserDatabase(AbstractUserDatabase):

    def __init__(self, connection: psycopg2.extensions.connection):
        self.connection = connection

    @contextmanager
    def _cur(self):
        with self.connection:
            yield self.connection.cursor()  # type: psycopg2.extensions.cursor

    def create_prefix(self, user_id: int) -> str:
        self.assert_user_exists(user_id)
        with self._cur() as cur:
            prefix = str(uuid4())
            cur.execute(
                'INSERT INTO prefixes (user_id, name) VALUES(%s, %s)',
                (user_id, prefix))
            return prefix

    def assert_user_exists(self, user_id):
        with self._cur() as cur:
            try:
                cur.execute(
                        'INSERT INTO users (user_id) VALUES (%s)',
                        (user_id,))
            except psycopg2.IntegrityError:
                pass

    def has_prefix(self, user_id: int, prefix: str) -> bool:
        with self._cur() as cur:
            cur.execute(
                'SELECT 1 FROM prefixes WHERE user_id=%s AND name=%s',
                (user_id, prefix))
            return cur.rowcount == 1

    def get_prefixes(self, user_id: int) -> List[str]:
        with self._cur() as cur:
            cur.execute(
                'SELECT name FROM prefixes WHERE user_id=%s',
                (user_id,))
            result = cur.fetchall()
            return [row[0] for row in result]

    def update_size(self, prefix: str, change: int):
        with self._cur() as cur:
            cur.execute(
                'UPDATE users u SET size = u.size + %s FROM prefixes p '
                'WHERE p.name=%s AND u.user_id = p.user_id',
                (change, prefix))

    def get_size(self, user_id: int) -> int:
        with self._cur() as cur:
            cur.execute(
                'SELECT max_quota, size FROM users WHERE user_id = %s',
                (user_id,))
            result = cur.fetchone()
            if result is None:
                return self.DEFAULT_QUOTA, 0
            else:
                return result

    def update_traffic(self, prefix: str, amount: int):
        with self._cur() as cur:
            cur.execute(
                'UPDATE users u SET download_traffic = download_traffic + %s '
                'FROM prefixes p '
                'WHERE p.name=%s AND u.user_id = p.user_id',
                (amount, prefix))

    def get_traffic(self, user_id: int) -> int:
        with self._cur() as cur:
            cur.execute('SELECT download_traffic FROM users WHERE user_id = %s', (user_id,))
            traffic, = cur.fetchone()
            if traffic is None:
                traffic = 0
            return traffic

    def _flush_all(self):
        with self._cur() as cur:
            cur.execute('DELETE FROM users')
            cur.execute('DELETE FROM prefixes')

