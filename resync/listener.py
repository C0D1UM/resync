import asyncio
from logging import getLogger
from typing import Callable

from rethinkdb import ReqlDriverError, ReqlRuntimeError, ReqlQueryLogicError

from resync.diff import Diff
from resync.models import Model
from resync.queryset import Queryset

l = getLogger('resync.listener')


class ChangeListener:
    """
    Generic change listener, listens to a given Queryset for changes and calls provided callback with the results.
    Recovers from connection being dropped.
    """

    def __init__(self, queryset: Queryset, callback: Callable[[Model, Diff], None], timeout: asyncio.Future):
        """
        Args:
            queryset: The queryset on which to listen for changes.
            callback: The callback to call with each change.  Should have the signature (Model, Diff) -> None
            timeout: A future that when cancelled will stop the listener at next error (which will be raised when the
                     database connection is closed.)
        """
        self.queryset = queryset
        self.callback = callback
        self.timeout = timeout

    async def listen(self):
        while not self.timeout.cancelled():
            try:
                async for obj, diff in self.queryset.changes():
                    await self.callback(obj, diff)
            except ReqlQueryLogicError:
                l.exception('This query is fundamentally stupid.')
                raise KeyboardInterrupt  # This kills the recon. (but allows finalizers to run)
            except (ReqlDriverError, ReqlRuntimeError):
                l.debug('ReqlError in listener coroutine for {}'.format(self.queryset.model), exc_info=True)
                await asyncio.sleep(1)  # Prevent runaway CPU and memory usage if database fails terminally.
                # TODO: Implement a limit on the number of retries
        l.debug('Shutting down {}'.format(self))

    def __str__(self):
        return 'ChangeListener for {}'.format(self.queryset)
