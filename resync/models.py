from typing import NamedTuple, Mapping, Dict, Any, List, Optional

from resync.fields import Field, ForeignKeyField, ReverseForeignKeyField
from resync.manager import Manager
from resync.utils import RegistryPatternMetaclass
from resync.diff import DiffObject

ModelMeta = NamedTuple(
    'Meta',
    [('table', str), ('fields', Mapping['str', Field]), ('reverse_relations', Mapping[str, ReverseForeignKeyField])]
)


class DocumentBase(type):

    def __new__(mcs, name, bases, attrs):
        fields = {}
        for base in bases:
            fields.update(base._meta.fields)
        non_field_attrs = {}
        for key, value in attrs.items():
            if isinstance(value, Field):
                value.name = key
                fields[key] = value
            else:
                non_field_attrs[key] = value
        new_class = super(DocumentBase, mcs).__new__(mcs, name, bases, non_field_attrs)
        new_class._meta = ModelMeta(None, fields, {})
        return new_class


class ModelBase(DocumentBase, RegistryPatternMetaclass):

    def __new__(mcs, name, bases, attrs):
        table_name = attrs.pop('table', name.lower())
        foreign_key_fields = {}
        for key, value in attrs.items():
            if isinstance(value, ForeignKeyField):
                foreign_key_fields[key] = value
        new_class = super(ModelBase, mcs).__new__(mcs, name, bases, attrs)
        new_class._meta = ModelMeta(table_name, new_class._meta.fields, {})
        for foreign_key_field_name, field in foreign_key_fields.items():
            related_model = field.model
            reverse_relation_name = field.related_name or name.lower() + '_set'
            related_model._meta.reverse_relations[reverse_relation_name] = ReverseForeignKeyField(new_class, foreign_key_field_name)
        return new_class

    @property
    def table(cls):
        return cls._meta.table


class NestedDocument(metaclass=DocumentBase):

    def __init__(self, **kwargs):
        fields = frozenset(self._meta.fields.keys())
        for field_name, value in kwargs.items():
            if field_name not in fields:
                raise AttributeError(
                    '{} received unexpected keyword argument {}.'.format(self.__class__.__name__, field_name))
            setattr(self, field_name, value)
        for field_name in fields.difference(frozenset(kwargs.keys())):
            setattr(self, field_name, self._meta.fields[field_name].default)

    def to_db(self) -> Dict[str, Any]:
        """
        Converts itself into a plain Python dictionary of values serialized into a form suitable for the database.
        This method is called by parent/container models when they are serialized.
        """
        field_data = self._get_field_data()
        return self.serialize_fields(field_data)

    @classmethod
    def from_db(cls, data_dict: Mapping[str, Any]):
        """
        Deserializes the data from its db representation into Python values and returns a
        """
        transformed_data = {}
        for field_name, field in cls._meta.fields.items():
            value = data_dict.get(field_name, field.default)
            transformed_data[field_name] = field.from_db(value)
        return cls(**transformed_data)

    @classmethod
    def serialize_fields(cls, data_dict: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Converts a dictionary with the Python values of this model's fields into their db forms.  Throws KeyError if
        any keys in the dictionary are not fields on this model.  Return value doesn't include fields missing from
        the input dictionary.
        """
        transformed_data = {}
        for field_name, value in data_dict.items():
            field = cls._meta.fields[field_name]
            transformed_data[field_name] = field.to_db(value)
        return transformed_data

    def _get_field_data(self):
        """
        Get the instance's field data as a plain Python dictionary.
        """
        return {field_name: getattr(self, field_name) for field_name in self._meta.fields.keys()}


class Model(NestedDocument, metaclass=ModelBase):

    class DoesNotExist(Exception):
        pass

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)
        if self.id is not None:
            for related_name, field in self._meta.reverse_relations.items():
                setattr(self, related_name, field.get_queryset(self.id))

    async def save(self) -> Optional[List[DiffObject]]:
        field_data = self._get_field_data()
        create = self.id is None
        if create:
            field_data.pop('id')
            new_obj = await self.objects.create(**field_data)
            self.id = new_obj.id
            changes = None
        else:
            changes = await self.objects.update(self, **field_data)
        return changes

    def to_db(self):
        serialized_data = super(Model, self).to_db()
        if self.id is None:
            serialized_data.pop('id')
        return serialized_data


def setup():
    for subclass in RegistryPatternMetaclass.REGISTRY:
        if subclass is Model:
            continue
        if not hasattr(subclass, 'objects'):
            subclass.objects = Manager()
        subclass.objects.attach_model(subclass)
