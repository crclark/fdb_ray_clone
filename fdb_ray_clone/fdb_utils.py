import fdb
from dataclasses import fields
from typing import TypeVar, Any
import pickle

fdb.api_version(710)


T = TypeVar("T")

# TODO: find a protocol for dataclasses so we don't need to use T
@fdb.transactional
def write_dataclass(
    tr: fdb.Transaction, subspace: fdb.Subspace, key: bytes, dataclass_object: T
) -> None:
    for field in fields(dataclass_object):
        tr[subspace.pack((key, field.name))] = pickle.dumps(
            getattr(dataclass_object, field.name), protocol=5
        )


@fdb.transactional
def read_dataclass(
    tr: fdb.Transaction, subspace: fdb.Subspace, key: bytes, dataclass_type: T
) -> T:
    return dataclass_type(  # type: ignore
        **{
            field.name: pickle.loads(tr[subspace.pack((key, field.name))])
            for field in fields(dataclass_type)
        }
    )
