import logging
from typing import Mapping

import asyncio

from resync import models
from resync.connection import connection_pool

l = logging.getLogger('resync')
l.addHandler(logging.NullHandler())


def setup(config: Mapping[str, str]):
    connection_pool.set_config(config)
    models.setup()


async def teardown():
    await connection_pool.teardown()


class ResyncConfiguration:
    """
    Contextmanager helper to ensure proper cleanup of resources.
    """

    def __init__(self, config: Mapping[str, str]):
        self.config = config

    def __enter__(self):
        setup(self.config)

    def __exit__(self, exc_type, exc_val, exc_tb):
        loop = asyncio.get_event_loop()
        fut = asyncio.ensure_future(teardown())
        if not loop.is_running():
            loop.run_until_complete(fut)

    async def __aenter__(self):
        setup(self.config)

    async def __aexit__(self, exc_type, exc_value, traceback):
        await teardown()
        if exc_type is not None:
            l.debug('Unhandled exception in RethinkConfiguration block', exc_info=(exc_type, exc_value, traceback))
            return False
