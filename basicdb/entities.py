from dataclasses import dataclass
import datetime
import uuid


@dataclass
class Object:
    uuid: uuid.UUID
    namespace: str
    name: str
    type_: str
    subtype: str
    creation_time: datetime.datetime
    modification_time: datetime.datetime
    hidden: bool
    username: str
    extra: dict

    _initialized: bool = False

    def __post_init__(self):
        self._initialized = True

    def __getitem__(self, key):
        return self.extra[key]
    
    def __setitem__(self, key, value):
        self.extra[key] = value

    def __getattr__(self, key):
        return self.extra[key]
    
    def __setattr__(self, key, value):
        if not self._initialized or key in self.__dict__:
            super().__setattr__(key, value)
        else:
            self.extra[key] = value


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
    
    _initialized: bool = False

    def __post_init__(self):
        self._initialized = True

    def __getitem__(self, key):
        return self.extra[key]
    
    def __setitem__(self, key, value):
        self.extra[key] = value

    def __getattr__(self, key):
        return self.extra[key]
    
    def __setattr__(self, key, value):
        if not self._initialized or key in self.__dict__:
            super().__setattr__(key, value)
        else:
            self.extra[key] = value


@dataclass
class Relationship:
    uuid: uuid.UUID
    first: uuid.UUID
    second: uuid.UUID
    type_: str
    hidden: bool