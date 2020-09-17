import getpass
import socket

from .entities import *
from .sql_adapter import SQLAdapter


class BasicDB:
    def __init__(self,
                 sql_string=None,
                 stash_rootdir=None,
                 namespace=None,
                 db_kwargs={},
                 stash_kwargs={},
                 db_adapter=None,
                 object_stash=None,
                 username = None,
                 include_fqdn_in_username=True):
        if username is None:
            if include_fqdn_in_username:
                self.username = getpass.getuser() + '@' + socket.getfqdn()
            else:
                self.username = getpass.getuser()
        else:
            self.username = username
        self.namespace = namespace
        if sql_string is not None:
            self.db_adapter = SQLAdapter(sql_string, **db_kwargs)
    
    def insert(self,
               namespace=None,
               name=None,
               type_=None,
               username=None,
               json_data=None,
               binary_data=None,
               blobs=None,
               return_result=False):
        if blobs is not None:
            raise NotImplementedError
        if username is None:
            username = self.username
        if namespace is None:
            namespace = self.namespace
        return self.db_adapter.insert_object(namespace=namespace,
                                             name=name,
                                             type_=type_,
                                             username=username,
                                             json_data=json_data,
                                             binary_data=binary_data,
                                             return_result=return_result)

    # Retrieves objects, arguments are filters
    def get(self,
            object_=None,
            uuid=None,
            uuids=None,
            namespace=None,
            name=None,
            names=None,
            type_=None,
            include_hidden=False,
            return_blobs=False,
            first_object=None,
            second_object=None,
            assert_exists=False):
        if namespace is None:
            namespace = self.namespace
        if object_ is not None:
            assert uuid is None
            uuid = object_.uuid
        if uuid is not None:
            assert uuids is None
            assert name is None
        if name is not None:
            assert names is None
        if names is not None:
            assert name is None
        return self.db_adapter.get_object(uuid=uuid,
                                          uuids=uuids,
                                          namespace=namespace,
                                          name=name,
                                          names=names,
                                          type_=type_,
                                          include_hidden=include_hidden,
                                          return_blobs=return_blobs,
                                          first_object=first_object,
                                          second_object=second_object,
                                          assert_exists=assert_exists)
    
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
    
    def exists(self, uuid_or_object):
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