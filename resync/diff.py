from typing import NamedTuple, Union, List, Tuple, Any

import dictdiffer

DiffObject = NamedTuple('DiffObject', [
        ('change_type', str),
        ('key_path', Union[str, List[Union[str, int]]]),
        ('value_tuple', Tuple[Any, Any])
    ])
Diff = Union[object, DiffObject]


def get_diff_from_changeset(changeset) -> Diff:
    """
    Change a change object from rethink (dictionary with 2 keys, 'old_val' and 'new_val' into our internal
    representation.
    """
    old, new = changeset['old_val'], changeset['new_val']
    if old is None:
        diff = create
    elif new is None:
        diff = delete
    else:
        diff = [DiffObject(*x) for x in dictdiffer.diff(old, new)]
    return diff


# These singletons are returned instead of a list of DiffObjects when an object is created or deleted
create = object()
delete = object()
