class RegistryPatternMetaclass(type):
    """
    We use a metaclass to register all Model subclasses as they are defined, then we will initialize the manager
    attribute on each class at runtime.
    Registry pattern: see https://github.com/faif/python-patterns/blob/master/registry.py
    """
    REGISTRY = set()

    def __new__(mcs, name, bases, attrs):
        new_cls = type.__new__(mcs, name, bases, attrs)
        mcs.REGISTRY.add(new_cls)
        return new_cls
