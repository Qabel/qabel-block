from typing import List
import psycopg2
import psycopg2.extensions
from abc import abstractmethod, ABC
from uuid import uuid4
from contextlib import contextmanager

from . import util


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

    def get_prefix_owner(self, prefix: str) -> int:
        with self._cur() as cur:
            cur.execute(
                'SELECT user_id FROM users NATURAL JOIN prefixes WHERE prefixes.name = %s',
                (prefix,))
            result = cur.fetchone()
            return result[0] if result else None

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
                'SELECT size FROM users WHERE user_id = %s',
                (user_id,))
            result = cur.fetchone()
            if result is None:
                self.assert_user_exists(user_id)
                return self.get_size(user_id)
            else:
                return result[0]

    def update_traffic(self, prefix: str, amount: int):
        with self._cur() as cur:
            cur.execute(
                'INSERT INTO traffic (traffic, traffic_month, user_id) '
                'SELECT %s, %s, user_id FROM prefixes WHERE name = %s '
                'ON CONFLICT (user_id, traffic_month) '
                'DO UPDATE '
                'SET traffic = traffic.traffic + EXCLUDED.traffic',
                (amount, util.this_month(), prefix))

    def get_traffic(self, user_id: int) -> int:
        with self._cur() as cur:
            cur.execute('SELECT traffic FROM traffic WHERE user_id = %s AND traffic_month = %s',
                        (user_id, util.this_month()))
            traffic = cur.fetchone()
            if traffic is None:
                traffic = 0,
            return traffic[0]

    def get_traffic_by_prefix(self, prefix: str) -> int:
        with self._cur() as cur:
            cur.execute('SELECT traffic FROM traffic JOIN prefixes USING (user_id)'
                        'WHERE name = %s AND traffic_month = %s',
                        (prefix, util.this_month()))
            result = cur.fetchone()
            if result is None:
                traffic = 0
            else:
                traffic, = result
            return traffic

    def _flush_all(self):
        with self._cur() as cur:
            cur.execute('DELETE FROM users')
            cur.execute('DELETE FROM prefixes')
            cur.execute('DELETE FROM traffic')
