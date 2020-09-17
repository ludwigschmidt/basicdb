from dataclasses import dataclass
import datetime


@dataclass
class Object:
    uuid: str
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
    uuid: str
    parent: str
    name: str
    type_: str
    size: int
    creation_time: datetime.datetime
    hidden: bool
    username: str
    json_data: dict


@dataclass
class Relationship:
    uuid: str
    first: str
    second: str
    type_: str
