import logging
import operator
from typing import List, Any, Tuple, Mapping, Callable

import rethinkdb as r

from resync.connection import DatabaseQuery, QueryRunner
from resync.diff import get_diff_from_changeset, Diff, delete

l = logging.getLogger('resync.queryset')

ALLOWED_COMPARATORS = frozenset(['eq', 'ne', 'gt', 'lt', 'ge', 'le'])


class BaseQueryset:

    def __init__(self, model, queries=tuple()):
        self.model = model
        self._queries = queries
        self._query = None

    @property
    def queries(self) -> Tuple[DatabaseQuery]:
        """
        We really don't want anybody to mutate this.
        """
        return self._queries

    async def __aiter__(self):
        self._query = QueryRunner(self.model.table, self.queries)
        self.cursor = await self._query.run()
        return self

    async def __anext__(self):
        if await self.cursor.fetch_next():
            value = await self.cursor.next()
        else:
            await self._query.close()
            raise StopAsyncIteration
        return self.transform_query_result(value)

    def transform_query_result(self, value):
        return value


class Queryset(BaseQueryset):

    UPDATE_ERROR_MSG = '{n_errors} errors in update query. \n First error message: {error_msg}\n Query: {query}'

    def __await__(self):
        """
        Allow awaiting the queryset to return it as a list.
        """
        return self._consume().__await__()

    async def _consume(self):
        """
        Consume the cursor into a list and return it.
        """
        result = []
        async for item in self:
            result.append(item)
        return result

    def all(self):
        """
        Just return a new copy of this queryset.  Currently makes duck-typing model Manager and Queryset easier.
        Might be useful one day to re-evaluate a cached queryset once I implement queryset caching.
        :return: A new Queryset with the same query
        """
        return self.__class__(self.model, self.queries)

    def transform_query_result(self, result):
        return self.model.from_db(result)

    def filter(self, **filter_kwargs: Mapping[str, Any]):
        extra_queries = []
        for key, value in filter_kwargs.items():
            query_key_parts = key.split('__')
            if len(query_key_parts) == 1:
                query = {key: value}
            elif len(query_key_parts) == 2:
                field, comparator = query_key_parts
                query = _build_filter_query(field, comparator, value)
            else:
                raise ValueError('This is not a valid key for filtering: {}'.format(key))
            extra_queries.append(('filter', (query,), {}))
        return self.__class__(self.model, self.queries + tuple(extra_queries))

    def order_by(self, field_name: str):
        if field_name.startswith('-'):
            field_name = field_name[1:]
            order = r.desc
        else:
            order = r.asc
        query = ('order_by', (order(field_name),), {})
        return OrderedQueryset(self.model, self.queries + (query,))

    def limit(self, num: int):
        query = ('limit', (num,), {})
        return self.__class__(self.model, self.queries + (query,))

    async def get(self, **kwargs):
        if kwargs:
            self.filter(**kwargs)
        value = None
        async with QueryRunner(self.model.table, self.queries) as query:
            cursor = await query.run()
            if not await cursor.fetch_next():
                raise self.model.DoesNotExist()
            value = await cursor.next()
            if await cursor.fetch_next():
                raise TooManyResults(self.queries)

        return self.transform_query_result(value)

    async def update(self, **fields_to_update) -> List[Tuple[Any, List[Diff]]]:
        """
        Update a queryset with new values for the fields passed as kwargs.  Returns a list of the changed objects.
        ### NOTE: Only the changed objects are returned, unchanged objects are ignored ###
        Args:
            **fields_to_update: specify as kwargs the fields to update on the model
        Returns:
            A list of objects updated and their list of changes, as returned by dictdiffer.diff. e.g.:
            [
                <Hardware object>, [ ('change', 'status'. (0, 1)), ]
            ]  # One object changed status from 0 to 1

        """
        serialized_data = self.model.serialize_fields(fields_to_update)
        query_kwargs = {'return_changes': True}
        queries = self.queries + (('update', (serialized_data,), query_kwargs),)
        async with QueryRunner(self.model.table, queries) as query:
            result = await query.run()
        if result['errors']:
            msg = self.UPDATE_ERROR_MSG.format(
                n_errors=result['errors'], error_msg=result['first_error'], query=self.queries)
            l.debug(msg)
            raise DBUpdateError(msg)

        changes = []
        for changeset in result['changes']:
            diff = get_diff_from_changeset(changeset)
            instance = self.model.from_db(changeset['new_val'])
            changes.append((instance, diff))
        return changes

    def changes(self):
        """
        Subscribes to a change feed of the filtered queryset.
        Returns: A new AsyncChangeFeed similar to a Queryset in that it can be iterated over with `async for`,
        but without the chainable methods like `filter`.  You can make `filter` calls ahead of `changes`.
        """
        query = ('changes', tuple(), {})
        return AsyncChangeFeed(self.model, self.queries + (query,))


class OrderedQueryset(Queryset):
    """
    A separate class is required because an order_by query returns an array instead of a cursor.
    """

    def __init__(self, *args, **kwargs):
        super(OrderedQueryset, self).__init__(*args, **kwargs)
        self._index = 0

    async def __anext__(self):
        try:
            value = self.cursor[self._index]
        except IndexError:
            await self._query.close()
            raise StopAsyncIteration
        self._index += 1
        return self.transform_query_result(value)


def _build_filter_query(field: str, comparator: str, value: Any) -> Callable[[Any], bool]:
    """
    Returns a function that returns a boolean, for use in a ReQL query as in the examples here:
        http://rethinkdb.com/api/python/filter/
    Args:
        field: Name of the field to filter on
        comparator: Name of the operator to use for the comparison, e.g. 'ne', 'lt', 'ge', etc
                    see ALLOWED_COMPARATORS for the full set
        value: Value to use in the comparison
    """
    if comparator not in ALLOWED_COMPARATORS:
        raise KeyError('Comparator "{}" is not recognized'.format(comparator))
    compare = getattr(operator, comparator)  # type: Callable[[Any, Any], bool]
    return lambda row: compare(row[field], value)


class AsyncChangeFeed(BaseQueryset):

    def transform_query_result(self, change_obj):
        diff = get_diff_from_changeset(change_obj)
        model_data = change_obj['old_val'] if diff is delete else change_obj['new_val']
        instance = self.model.from_db(model_data)
        return instance, diff


class TooManyResults(Exception):
    pass


class DBUpdateError(Exception):
    pass
