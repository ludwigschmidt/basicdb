from collections import Iterable
import getpass
import pathlib
import pickle
import socket

import objectstash

from .entities import *
from .sql_adapter import SQLAdapter


def uuid_if_object(x):
    if isinstance(x, Object):
        return x.uuid
    assert isinstance(x, str) or x is None
    return x


def uuid_if_blob(x):
    if isinstance(x, Blob):
        return x.uuid
    assert isinstance(x, str) or x is None
    return x


def uuid_if_relationship(x):
    if isinstance(x, Relationship):
        return x.uuid
    assert isinstance(x, str) or x is None
    return x


class BasicDB:
    def __init__(self,
                 sql_string=None,
                 stash_rootdir=None,
                 namespace=None,
                 db_kwargs={},
                 stash_kwargs={},
                 db_adapter=None,
                 object_stash=None,
                 username=None,
                 include_fqdn_in_username=True,
                 stash_blob_prefix=''):
        if username is None:
            if include_fqdn_in_username:
                self.username = getpass.getuser() + '@' + socket.getfqdn()
            else:
                self.username = getpass.getuser()
        else:
            self.username = username
        self.namespace = namespace
        # Database initialization
        if sql_string is not None:
            self.db_adapter = SQLAdapter(sql_string, **db_kwargs)
        # Object stash initialization
        if object_stash is not None:
            self.stash = object_stash
            assert stash_rootdir is None
        else:
            if stash_rootdir is not None:
                self.stash = objectstash.ObjectStash(rootdir=stash_rootdir, **stash_kwargs)
            else:
                raise ValueError('No known ObjectStash implementation identified.')
        self.stash_blob_prefix = pathlib.Path(stash_blob_prefix)
        self.serialization = 'pickle'
    
    def serialize(self, data):
        return pickle.dumps(data)
    
    def deserialize(self, bytes, serialization):
        if serialization is None:
            return bytes
        else:
            assert serialization == 'pickle'
            return pickle.loads(bytes)
    
    def get_blob_key(self, blob_uuid):
        return str(self.stash_blob_prefix / blob_uuid)
    
    def insert(self,
               namespace=None,
               name=None,
               type_=None,
               username=None,
               json_data=None,
               binary_data=None,
               blob=None,
               blobs=None,
               return_result=True):
        if blob is not None:
            assert blobs is None
        if blobs is not None:
            assert blob is None
        also_insert_blobs = blob is not None or blobs is not None
        if username is None:
            username = self.username
        if namespace is None:
            namespace = self.namespace
        else:
            assert self.namespace is None or namespace == self.namespace
        if not also_insert_blobs:
            return self.db_adapter.insert_object(namespace=namespace,
                                                 name=name,
                                                 type_=type_,
                                                 username=username,
                                                 json_data=json_data,
                                                 binary_data=binary_data,
                                                 return_result=return_result)
        else:
            new_obj = self.db_adapter.insert_object(namespace=namespace,
                                                    name=name,
                                                    type_=type_,
                                                    username=username,
                                                    json_data=json_data,
                                                    binary_data=binary_data,
                                                    return_result=True)
            if blobs is None:
                blobs = {None: blob}
            for blob_name, blob_data in blobs.items():
                self.insert_blob(new_obj.uuid,
                                 name=blob_name,
                                 data=blob_data)
            if return_result:
                return new_obj

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
            rel_first=None,
            rel_second=None,
            rel_type=None,
            assert_exists=False):
        if namespace is None:
            namespace = self.namespace
        else:
            assert self.namespace is None or namespace == self.namespace
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
                                          relationship_first=uuid_if_object(rel_first),
                                          relationship_second=uuid_if_object(rel_second),
                                          relationship_type=rel_type,
                                          assert_exists=assert_exists)
    
    def update(self,
               uuid_or_object,
               namespace=None,
               type_=None,
               hidden=None,
               username=None,
               json_data=None,
               binary_data=None):
        raise NotImplementedError

    def delete(self, to_delete, hide_only=True):
        uuid_list = []
        if isinstance(to_delete, list):
            for x in to_delete:
                uuid_list.append(uuid_if_object(x))
        else:
            uuid_list.append(uuid_if_object(to_delete))
        return self.db_adapter.delete_objects(uuids=uuid_list,
                                              hide_only=hide_only,
                                              check_namespace=self.namespace is not None,
                                              namespace_to_check=self.namespace)
    
    def exists(self, uuid_or_object):
        return self.get(uuid=uuid_if_object(uuid_or_object),
                        include_hidden=False) is not None
    
    # Create a new blob
    def insert_blob(self,
                    object_or_uuid,
                    name=None,
                    type_=None,
                    username=None,
                    json_data=None,
                    data=None):
        object_identifier = uuid_if_object(object_or_uuid)
        if not isinstance(data, bytes):
            data = self.serialize(data)
            serialization = self.serialization
        else:
            serialization = None
        size = len(data)
        if username is None:
            username = self.username
        new_blob = self.db_adapter.insert_blob(
                object_identifier=object_identifier,
                name=name,
                type_=type_,
                username=username,
                json_data=json_data,
                serialization=serialization,
                size=size,
                return_result=True,
                check_namespace=self.namespace is not None,
                namespace_to_check=self.namespace)
        new_key = self.get_blob_key(new_blob.uuid)
        self.stash.put(new_key, data)
        return new_blob

    # Upload a list of files as blobs
    def insert_files_as_blobs(self,
                              object_or_uuid,
                              filenames,
                              name_prefix=None,
                              username=None):
        raise NotImplementedError
    
    # Upload a files in a directory as blobs
    def insert_dir_as_blobs(self,
                            object_or_uuid,
                            dir,
                            name_prefix=None,
                            username=None):
        raise NotImplementedError

    # Load all blobs for a given object
    def get_blobs(self,
                  obj_identifier,
                  return_by='name',
                  include_hidden=False):
        uuid_or_name = uuid_if_object(obj_identifier)
        tmp_res = self.db_adapter.get_blobs(object_identifier=uuid_or_name,
                                            include_hidden=include_hidden,
                                            check_namespace=self.namespace is not None,
                                            namespace_to_check=self.namespace)
        if return_by == 'name':
            res = {}
            for blob in tmp_res:
                res[blob.name] = blob
        elif return_by == 'uuid':
            res = {}
            for blob in tmp_res:
                res[blob.uuid] = blob
        elif return_by is None:
            res = tmp_res
        else:
            raise ValueError
        return res

    # Load blob data either by uuid or by object, name
    def load_blob(self,
                  object_identifier_or_blob=None,
                  name=None,
                  *,
                  uuid=None,
                  stash_kwargs={}):
        cur_blob = None
        if isinstance(object_identifier_or_blob, Blob):
            uuid = object_identifier_or_blob.uuid
            cur_blob = object_identifier_or_blob
            object_identifier_or_blob = None
        else:
            object_identifier_or_blob = uuid_if_object(object_identifier_or_blob)

        if uuid is None:
            assert object_identifier_or_blob is not None
            cur_blob = self.db_adapter.get_blobs(object_identifier=object_identifier_or_blob,
                                                 match_name=True,
                                                 name=name,
                                                 check_namespace=self.namespace is not None,
                                                 namespace_to_check=self.namespace,
                                                 include_hidden=False,
                                                 assert_exists=True)
        else:
            assert object_identifier_or_blob is None
            assert name is None
            if cur_blob is None:
                cur_blob = self.db_adapter.get_blobs(uuid=uuid,
                                                     include_hidden=False,
                                                     check_namespace=self.namespace is not None,
                                                     namespace_to_check=self.namespace,
                                                     assert_exists=True)
        tmp_res = self.stash.get(self.get_blob_key(cur_blob.uuid), **stash_kwargs)
        return self.deserialize(tmp_res, cur_blob.serialization)
    

    def load_blobs(self,
                   obj_identifier=None,
                   names=None,
                   *,
                   uuids=None,
                   skip_database=False,
                   serialization=None,
                   also_return_blob_objects=False,
                   stash_kwargs={}):
        if skip_database:
            assert obj_identifier is None
            assert names is None
            assert uuids is not None
            assert serialization is not None
            tmp_res = self.stash.get([self.get_blob_key(x.uuid) for x in uuids], **stash_kwargs)
            res = {}
            for k, v in tmp_res.items():
                res[k] = self.deserialize(v, serialization)
            assert len(res) == len(uuids)
            return res
        else:
            by = 'name'
            if uuids is not None:
                assert names is None
                by = 'uuid'
            blobs = self.get_blobs(obj_identifier, return_by=by)

            keys_to_get = {}
            if by == 'name':
                if names is None:
                    names = list(blobs.keys())
                for x in names:
                    keys_to_get[self.get_blob_key(blobs[x].uuid)] = blobs[x]
            else:
                for x in uuids:
                    keys_to_get[self.get_blob_key(blobs[x].uuid)] = blobs[x]
            tmp_res = self.stash.get(keys_to_get, **stash_kwargs)

            res = {}
            if by == 'name':
                for key, blob in keys_to_get.items():
                    res[blob.name] = self.deserialize(tmp_res[key], blob.serialization)
            else:
                for key, blob in keys_to_get.items():
                    res[blob.uuid] = self.deserialize(tmp_res[key], blob.serialization)
            
            if uuids is not None:
                target_len = len(uuids)
            else:
                target_len = len(names)
            assert len(res) == target_len
            if also_return_blob_objects:
                blobs_res = {}
                if by == 'name':
                    for name in res:
                        blobs_res[name] = blobs[name]
                else:
                    for uuid in res:
                        blobs_res[uuid] = blobs[uuid]
                return res, blobs
            else:
                return res
    
    def update_blob(self,
                    obj_identifier=None,
                    name=None,
                    uuid=None,
                    type_=None,
                    json_data=None,
                    username=None,
                    hidden=None,
                    data=None):
        raise NotImplementedError
    
    def delete_blob(self,
                    to_delete,
                    hide_only=True):
        if not hide_only:
            raise NotImplementedError
        uuid_list = []
        if isinstance(to_delete, list):
            for x in to_delete:
                uuid_list.append(uuid_if_blob(x))
        else:
            uuid_list.append(uuid_if_blob(to_delete))
        return self.db_adapter.delete_blobs(uuids=uuid_list,
                                            hide_only=hide_only,
                                            check_namespace=self.namespace is not None,
                                            namespace_to_check=self.namespace)
    
    def insert_relationship(self,
                            first,
                            second,
                            type_=None,
                            return_result=True):
            assert first is not None and second is not None
            return self.db_adapter.insert_relationship(first=uuid_if_object(first),
                                                       second=uuid_if_object(second),
                                                       type_=type_,
                                                       return_result=return_result,
                                                       check_namespace=self.namespace is not None,
                                                       namespace_to_check=self.namespace)
    
    def get_relationship(self,
                         first=None,
                         second=None,
                         type_=None,
                         uuid=None,
                         uuids=None,
                         include_hidden=False):
        if isinstance(type_, str):
            type_ = [type_]
        return self.db_adapter.get_relationships(first=uuid_if_object(first),
                                                 second=uuid_if_object(second),
                                                 type_=type_,
                                                 uuid=uuid,
                                                 uuids=uuids,
                                                 filter_namespace=self.namespace is not None,
                                                 namespace=self.namespace,
                                                 include_hidden=include_hidden)
    
    def delete_relationship(self,
                            to_delete,
                            hide_only=True):
        if not hide_only:
            raise NotImplementedError
        uuid_list = []
        if isinstance(to_delete, list):
            for x in to_delete:
                uuid_list.append(uuid_if_relationship(x))
        else:
            uuid_list.append(uuid_if_relationship(to_delete))
        return self.db_adapter.delete_relationships(uuids=uuid_list,
                                                    hide_only=hide_only,
                                                    check_namespace=self.namespace is not None,
                                                    namespace_to_check=self.namespace)