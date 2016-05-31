from collections import MutableSequence, MutableMapping

import arrow


class Field:
    """
    I experimented with making this class a data-descriptor, to be attached to instances of the model.  However, it
    felt like misdirection with no real upside.
    In the end, instances of this class are limited to only dealing with setting the default value on new Model
    instances and converting between db and python representations, which means they only need to exist as class-level
    attributes in `Model._meta.fields`.
    """

    MUTABLE_DEFAULT_ERR = 'Try not to use mutable default arguments. You probably want a NestedDocument or ListField'

    def __init__(self, default=None):
        assert not isinstance(default, (MutableSequence, MutableMapping)), self.MUTABLE_DEFAULT_ERR
        self._default = default
        self.name = None

    @property
    def default(self):
        try:
            return self._default()
        except TypeError:
            return self._default

    @staticmethod
    def to_db(value):
        return value

    @staticmethod
    def from_db(value):
        return value


class ForeignKeyField(Field):
    """
    Used to reference objects in another table.
    """

    def __init__(self, model, related_name=None):
        super(ForeignKeyField, self).__init__()
        self.model = model
        self.related_name = related_name

    @staticmethod
    def to_db(value):
        return value.id if value is not None else None

    def from_db(self, value):
        return RelatedObjectProxy(self.model, value)


class RelatedObjectProxy:
    """
    With this proxy, users can get the id of the object synchronously without a db query, or they can await it to get
    the whole object.
    """

    def __init__(self, model, id):
        self.model = model
        self.id = id
        self._cache = None

    async def _get_instance(self):
        if self.id is None:
            return None
        if self._cache is None:
            self._cache = await self.model.objects.get(id=self.id)
        return self._cache

    def __await__(self):
        return self._get_instance().__await__()

    def __str__(self):
        return '{} object, id: {}'.format(self.model, self.id)


class NestedDocumentField(Field):
    """
    Used to nest data structures in documents.  'inner' should be an subclass of NestedDocument
    """

    def __init__(self, inner):
        super(NestedDocumentField, self).__init__()
        self.inner = inner

    def to_db(self, value):
        """
        Cover the None case here to avoid complicating the NestedDocument to_db implementation.
        """
        return self.inner.to_db(value) if value is not None else None

    def from_db(self, value):
        return self.inner.from_db(value) if value is not None else None


class ListField(Field):
    """
    Represents a collection of 'inner' fields.
    """

    def __init__(self, inner):
        super(ListField, self).__init__(default=list)
        self.inner = inner

    def to_db(self, value):
        return [self.inner.to_db(inner_obj) for inner_obj in value]

    def from_db(self, value):
        return [self.inner.from_db(inner_obj) for inner_obj in value]


class DictField(Field):
    """
    Used for arbitrary unstructured data.
    """

    def __init__(self):
        super(DictField, self).__init__(default=dict)


def field_factory(name, to_db, from_db):

    field_class = type(name, (Field,), {})
    field_class.to_db = staticmethod(lambda x: to_db(x) if x is not None else None)
    field_class.from_db = staticmethod(lambda x: from_db(x) if x is not None else None)
    return field_class

StrField = field_factory('StrField', str, str)
IntField = field_factory('IntField', int, int)
FloatField = field_factory('FloatField', float, float)
BooleanField = field_factory('BooleanField', bool, bool)


class DateTimeField(Field):

    @staticmethod
    def to_db(value):
        return value.isoformat() if value is not None else None

    @staticmethod
    def from_db(value):
        return arrow.get(value) if value is not None else None


class IntEnumField(Field):

    def __init__(self, enum_class, **kwargs):
        self.enum_class = enum_class
        super(IntEnumField, self).__init__(**kwargs)

    @staticmethod
    def to_db(value):
        try:
            value = value.value
        except AttributeError:
            if not isinstance(value, (int, type(None))):  # Allow null values until I add a `required` kwarg to fields
                raise TypeError('Only `IntEnum`s and `int`s can be saved to an IntEnumField, got {}'.format(value))
        return value

    def from_db(self, value):
        return self.enum_class(int(value)) if value is not None else None


class ReverseForeignKeyField:
    """
    Created automatically as a counterpart to ForeignKeyField on the related model.  Should not be instantiated by
    user code.
    """

    def __init__(self, target_model, field_name):
        self.target_model = target_model
        self.field_name = field_name

    def get_queryset(self, id):
        return self.target_model.objects.filter(**{self.field_name: id})
