from dataclasses import dataclass
import datetime
import uuid


@dataclass
class Object:
    uuid: uuid.UUID
    namespace: str
    name: str
    type_: str
    creation_time: datetime.datetime
    hidden: bool
    username: str
    json_data: dict
    binary_data: bytes

    def __getitem__(self, key):
        return self.json_data[key]


@dataclass
class Blob:
    uuid: uuid.UUID
    parent: uuid.UUID
    name: str
    type_: str
    size: int
    creation_time: datetime.datetime
    hidden: bool
    username: str
    serialization: str
    json_data: dict


@dataclass
class Relationship:
    uuid: uuid.UUID
    first: uuid.UUID
    second: uuid.UUID
    type_: str
    hidden: bool