from typing import Tuple

import rethinkdb as r

from resync.connection import connection_pool
from resync.queryset import Queryset


class BaseManager:

    def attach_model(self, model):
        self.model = model


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

    def create(self, **kwargs):
        """
        Inserts a new record into the database
        :param kwargs: Attributes to set on the model
        :return: Coroutine that returns created instance
        """
        return self.all().insert(**kwargs)

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
