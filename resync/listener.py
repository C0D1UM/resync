import asyncio
from collections import Awaitable
from logging import getLogger
from typing import Callable

from rethinkdb import ReqlTimeoutError, ReqlAvailabilityError

from resync.diff import Diff
from resync.models import Model
from resync.queryset import Queryset

l = getLogger('resync.listener')


class ChangeListener:
    """
    Generic change listener, listens to a given Queryset for changes and calls provided callback with the results.
    Recovers from connection being dropped.
    """

    def __init__(self, queryset: Queryset, callback: Callable[[Model, Diff], Awaitable]):
        """
        Args:
            queryset: The queryset on which to listen for changes.
            callback: The callback to call with each change.  Should have the signature (Model, Diff) -> Awaitable
        """
        self.queryset = queryset
        self.callback = callback

    async def listen(self):
        try:
            while True:
                try:
                    async for obj, diff in self.queryset.changes():
                        await self.callback(obj, diff)
                except (ReqlTimeoutError, ReqlAvailabilityError):
                    l.debug('ReqlError in listener {}'.format(self), exc_info=True)
                    await asyncio.sleep(1)  # Prevent busy looping if database fails terminally.
                    # TODO: Implement some kind of backoff strategy
                    # TODO: Implement a limit on the number of retries
        finally:
            l.debug('Shutting down {}'.format(self))

    def __str__(self):
        return 'ChangeListener for {}'.format(self.queryset)
