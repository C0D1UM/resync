from asyncio import Queue, QueueEmpty
from logging import getLogger
from typing import Tuple
from weakref import WeakSet

import rethinkdb as r
from rethinkdb.net import DefaultConnection

l = getLogger('resync.connection')

r.set_loop_type('asyncio')

DatabaseQuery = Tuple[str, tuple, dict]


class ConnectionPool:

    def __init__(self):
        self._config_dict = None
        self._queue = Queue()
        self._outstanding_connections = WeakSet()

    async def get_conn(self):
        self._check_config()
        try:
            while True:
                conn = self._queue.get_nowait()
                if conn.is_open():
                    break
                try:
                    await conn.close()
                except Exception:
                    l.debug('Exception in close rethink connection', exc_info=True)
        except QueueEmpty:
            conn = await r.connect(**self._config_dict)
        self._outstanding_connections.add(conn)
        return conn

    async def put_conn(self, conn):
        self._queue.put_nowait(conn)
        self._outstanding_connections.remove(conn)

    def set_config(self, config):
        self._config_dict = config

    def get_config(self):
        self._check_config()
        return self._config_dict

    async def teardown(self):
        while True:
            try:
                conn = self._queue.get_nowait()
            except QueueEmpty:
                break
            self._outstanding_connections.add(conn)
        for conn in self._outstanding_connections:
            try:
                await conn.close()
            except Exception:
                l.debug('Exception in close rethink connection', exc_info=True)

    def _check_config(self):
        assert self._config_dict is not None, "Did you remember to run resync.setup()?"

connection_pool = ConnectionPool()


class RethinkConnection:
    """
    A context manager helper to get a connection from the pool and return it to the pool when
    it is finished.
    """

    async def __aenter__(self):
        self._conn = await connection_pool.get_conn()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            l.debug('Unhandled exception in RethinkConnection block', exc_info=(exc_type, exc_val, exc_tb))
            try:
                self._conn.close()
            except Exception:
                pass
            return False
        await connection_pool.put_conn(self._conn)


def get_sync_connection(timeout=20):
    """
    Convenience method for testing.
    :return:  Synchronous (blocking) connection to rethinkdb
    """
    conn = DefaultConnection(
        **connection_pool.get_config(),
        auth_key='',
        timeout=timeout,
        ssl=dict(),
    )
    return conn.reconnect(timeout=timeout)
