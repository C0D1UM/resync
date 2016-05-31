import logging
import operator
from typing import List, Any, Tuple, Mapping, Callable, Iterable

import rethinkdb as r

from resync.connection import DatabaseQuery, connection_pool, RethinkConnection
from resync.diff import get_diff_from_changeset, Diff, delete

l = logging.getLogger('resync.queryset')

ALLOWED_COMPARATORS = frozenset(['eq', 'ne', 'gt', 'lt', 'ge', 'le'])


class BaseQueryset:

    def __init__(self, model, queries=None):
        self.model = model
        self.queries = queries or tuple()  # type: Tuple[DatabaseQuery]
        self._conn = None

    async def __aiter__(self):
        self._conn = await connection_pool.get_conn()
        self.cursor = await self._run_query(self._conn)
        return self

    async def __anext__(self):
        if await self.cursor.fetch_next():
            value = await self.cursor.next()
        else:
            await connection_pool.put_conn(self._conn)
            self._conn = None
            raise StopAsyncIteration
        return self.transform_query_result(value)

    def transform_query_result(self, value):
        return value

    async def _run_query(self, conn):
        """
            Run a query against the database and return a cursor or query result as appropriate.
            Returns:
                Result dictionary or cursor, depending on the type of the final query.
            """
        query_to_run = self._build_query(self.model.table, self.queries)
        result = await query_to_run.run(conn)
        return result

    @staticmethod
    def _build_query(table: str, queries: Iterable[DatabaseQuery]):
        """
        Build a query object from a list of DatabaseQuery tuples.  Might be useful in the future for building
        nested queries.
        """
        final_query = r.table(table)
        for query_type, args, kwargs in queries:
            query_func = getattr(final_query, query_type)
            final_query = query_func(*args, **kwargs)
        return final_query


class Queryset(BaseQueryset):

    UPDATE_ERROR_MSG = '{n_errors} errors in update query. \n First error message: {error_msg}\n Query: {query}'
    INSERT_ERROR_MSG = '{n_errors} errors in insert query. \n First error message: {error_msg}\n Query: {query}'

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
        async with RethinkConnection() as conn:
            cursor = await self._run_query(conn)
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
        self.queries += (('update', (serialized_data,), query_kwargs),)
        async with RethinkConnection() as conn:
            result = await self._run_query(conn)
        if result['errors']:
            msg = self.UPDATE_ERROR_MSG.format(
                n_errors=result['errors'], error_msg=result['first_error'], query=self.queries)
            l.debug(msg)
            raise DBUpdateError(msg)

        raw_changes = result['changes']
        changes = []
        for changeset in raw_changes:
            diff = get_diff_from_changeset(changeset)
            instance = self.model.from_db(changeset['new_val'])
            changes.append((instance, diff))
        return changes

    async def insert(self, **field_data):
        """
        Inserts a new record into the database
        :param field_data: Attributes to set on the model
        :return: Created instance
        """
        assert not self.queries, 'It doesn\'t make sense to `insert` into a filtered queryset.'
        unsaved_instance = self.model(**field_data)
        serialized_data = unsaved_instance.to_db()
        query_kwargs = {'return_changes': True}
        self.queries += (('insert', (serialized_data,), query_kwargs),)
        async with RethinkConnection() as conn:
            result = await self._run_query(conn)

        if result['errors']:
            msg = self.INSERT_ERROR_MSG.format(
                n_errors=result['errors'], error_msg=result['first_error'], query=self.queries)
            l.debug(msg)
            raise DBInsertError(msg)

        new_object_data = result['changes'][0]['new_val']
        return self.model.from_db(new_object_data)

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
            await connection_pool.put_conn(self._conn)
            self._conn = None
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
    compare = getattr(operator, comparator)  # type: Callable[..., bool]
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


class DBInsertError(Exception):
    pass
