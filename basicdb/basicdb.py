from dataclasses import dataclass
import datetime


@dataclass
class Object:
    uuid: str
    namespace: str
    type_: str
    time_created: datetime.datetime
    hidden: bool
    username: str
    json_data: dict
    binary_data: bytes
    blobs: dict


@dataclass
class Blob:
    uuid: str
    parent: str
    name: str
    type_: str
    size: int
    time_created: datetime.datetime
    hidden: bool
    username: str
    json_data: dict


@dataclass
class Relationship:
    uuid: str
    first: str
    second: str
    type_: str


class BasicDB:
    def __init__(self,
                 db_string,
                 stash_string,
                 namespace=None,
                 db_kwargs=None,
                 stash_kwargs=None,
                 db_adapter=None,
                 object_stash=None):
        pass
    
    def insert(self,
               namespace=None,
               type_=None,
               username=None,
               json_data=None,
               binary_data=None,
               blobs=None):
        pass

    # Retrieves objects, arguments are filters
    def get(self,
            uuid_or_uuids=None,
            namespace=None,
            type_=None,
            include_hidden=False,
            return_blobs=False,
            first_object=None,
            second_object=None):
        pass
    
    def update(self,
               uuid_or_object,
               namespace=None,
               type_=None,
               hidden=None,
               username=None,
               json_data=None,
               binary_data=None):
        pass

    def delete(self, uuid_uuids):
        pass
    
    # Create a new blob
    def insert_blob(self,
                    object_or_uuid,
                    name,
                    type_=None,
                    username=None,
                    json_data=None,
                    data=None):
        pass

    # Upload a list of files as blobs
    def insert_files_as_blobs(self,
                              object_or_uuid,
                              filenames,
                              name_prefix=None,
                              username=None):
        pass
    
    # Upload a files in a directory as blobs
    def insert_dir_as_blobs(self,
                            object_or_uuid,
                            dir,
                            name_prefix=None,
                            username=None):
        pass

    # Load all blobs for a given object
    def get_blobs(self, object_or_uuid):
        pass

    # Load blob data either by uuid or by object, name
    def load_blob_data(self,
                       obj=None,
                       name=None,
                       names=None,
                       uuid=None,
                       uuids=None):
        pass
    
    def update_blob(self,
                    obj=None,
                    name=None,
                    uuid=None,
                    type_=None,
                    json_data=None,
                    username=None,
                    hidden=None,
                    data=None):
        pass
    
    def delete_blob(self,
                    obj=None,
                    name=None,
                    uuid=None,
                    full_deletion=False):
        pass
    
    def insert_relationship(self, first, second, type_=None):
        pass
    
    def get_relationship(self, first=None, second=None, type_=None):
        pass
    
    def delete_relationship(self, first, second, type_=None):
        pass