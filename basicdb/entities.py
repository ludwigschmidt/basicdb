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
    modification_time: datetime.datetime
    hidden: bool
    username: str
    extra: dict

    def __getitem__(self, key):
        return self.extra[key]

    def __getattr__(self, key):
        return self.extra[key]


@dataclass
class Blob:
    uuid: uuid.UUID
    parent: uuid.UUID
    name: str
    type_: str
    size: int
    creation_time: datetime.datetime
    modification_time: datetime.datetime
    hidden: bool
    username: str
    serialization: str
    extra: dict
    
    def __getitem__(self, key):
        return self.extra[key]
    
    def __getattr__(self, key):
        return self.extra[key]


@dataclass
class Relationship:
    uuid: uuid.UUID
    first: uuid.UUID
    second: uuid.UUID
    type_: str
    hidden: bool