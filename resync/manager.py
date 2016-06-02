from logging import getLogger
from typing import Tuple

import rethinkdb as r

from resync.connection import connection_pool, QueryRunner
from resync.queryset import Queryset

l = getLogger('resync.manager')


class BaseManager:

    INSERT_ERROR_MSG = '{n_errors} errors in insert query. \n First error message: {error_msg}\n Query: {query}'

    def attach_model(self, model):
        self.model = model

    class DBInsertError(Exception):
        pass


class Manager(BaseManager):
    """
    This class is intended to encapsulate the logic for interacting with the database.
    """

    def get(self, **kwargs):
        """
        Query the database for a single instance.
        :param kwargs: Parameters to use for filtering
        :return: Instance of the model
        """
        return self.filter(**kwargs).get()

    async def update(self, instance, **kwargs) -> Tuple[str, str, tuple]:
        """
        Update an instance in the database with the passed kwargs.
        """
        changes_list = await self.filter(id=instance.id).update(**kwargs)
        if changes_list:
            instance, changes = changes_list[0]
        else:
            changes = []
        return changes

    def filter(self, **kwargs) -> Queryset:
        """
        Returns a Queryset filtered on the given arguments.
        :param kwargs: Filters to apply. See rethink docs
        """
        return self.all().filter(**kwargs)

    def changes(self) -> Queryset:
        """
        Returns a change feed of this model's table.
        """
        return self.all().changes()

    async def create(self, **field_data):
        """
        Inserts a new record into the database
        :param field_data: Attributes to set on the model
        :return: Created instance
        """
        unsaved_instance = self.model(**field_data)
        serialized_data = unsaved_instance.to_db()
        query_kwargs = {'return_changes': True}
        queries = (('insert', (serialized_data,), query_kwargs),)
        async with QueryRunner(self.model.table, queries) as query:
            result = await query.run()

        if result['errors']:
            msg = self.INSERT_ERROR_MSG.format(
                n_errors=result['errors'], error_msg=result['first_error'], query=queries)
            l.debug(msg)
            raise self.DBInsertError(msg)

        new_object_data = result['changes'][0]['new_val']
        return self.model.from_db(new_object_data)

    # TODO: Fix or remove this.
    # def create_sync(self, conn, **kwargs):
    #     """
    #     Synchronously inserts a new record into the database.
    #     :param conn: Synchronous RethinkDB connection
    #     :param kwargs: Attributes to set on the model
    #     :return: Created instance
    #     """
    #     inserted = r.table(self.model.table).insert(kwargs).run(conn)
    #     info = r.table(self.model.table).get(inserted['generated_keys'][0]).run(conn)
    #     return self.model.from_db(info)

    async def delete(self, instance):
        """
        Deletes a record from the database. Returns True if the object was deleted, otherwise it
        probably throws an Exception of some kind tbh I'm not really sure
        :param instance: Model instance
        :return: bool
        """
        conn = await connection_pool.get_conn()
        query = await r.table(self.model.table).get(instance.id).delete().run(conn)
        await connection_pool.put_conn(conn)
        return bool(query['deleted'])

    def delete_sync(self, conn, instance):
        """
        Synchronously deletes a record from the database. Returns True if the object was deleted, otherwise it
        probably throws an Exception of some kind tbh I'm not really sure
        :param conn: Synchronous RethinkDB connection
        :param instance: Model instance
        :return: bool
        """
        query = r.table(self.model.table).get(instance.id).delete().run(conn)
        return bool(query['deleted'])

    def all(self) -> Queryset:
        """
        Returns an async iterator of all the objects in this table.
        :return: Queryset of all documents in this model's table
        """
        return Queryset(self.model)
