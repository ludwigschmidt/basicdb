import getpass
import pathlib
import pickle
import socket

import msgpack
import objectstash

from .entities import *
from .exceptions import *
from .sql_adapter import SQLAdapter
from . import utils


def uuid_if_object(x):
    if isinstance(x, Object):
        return x.uuid
    return x


def uuid_if_blob(x):
    if isinstance(x, Blob):
        return x.uuid
    return x


def uuid_if_relationship(x):
    if isinstance(x, Relationship):
        return x.uuid
    return x


class BasicDB:
    def __init__(self,
                 sql_string=None,
                 stash_rootdir=None,
                 stash_s3_bucket=None,
                 namespace=None,
                 db_kwargs={},
                 stash_kwargs={},
                 db_adapter=None,
                 object_stash=None,
                 username=None,
                 include_fqdn_in_username=True,
                 stash_blob_prefix='basicdb_stash',
                 max_extra_fields_size=1024,
                 allow_hard_delete=False):
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
            assert db_adapter is None
            self.db_adapter = SQLAdapter(sql_string, **db_kwargs)
        else:
            assert db_adapter is not None
            self.db_adapter = db_adapter
        # Object stash initialization
        if object_stash is not None:
            self.stash = object_stash
            assert stash_rootdir is None
            assert stash_s3_bucket is None
        else:
            if stash_rootdir is not None:
                assert stash_s3_bucket is None
                self.stash = objectstash.ObjectStash(rootdir=stash_rootdir, **stash_kwargs)
            elif stash_s3_bucket is not None:
                assert stash_rootdir is None
                self.stash = objectstash.ObjectStash(s3_bucket=stash_s3_bucket, **stash_kwargs)
            else:
                raise ValueError('No known ObjectStash implementation identified.')
        self.stash_blob_prefix = pathlib.Path(stash_blob_prefix)
        self.serialization = 'pickle'
        self.max_extra_fields_size = max_extra_fields_size
        self.allow_hard_delete = allow_hard_delete
    
    def serialize(self, data):
        return pickle.dumps(data)
    
    def deserialize(self, bytes, serialization):
        if serialization is None:
            return bytes
        else:
            assert serialization == 'pickle'
            return pickle.loads(bytes)
    
    def get_blob_key(self, blob_uuid):
        return str(self.stash_blob_prefix / blob_uuid.hex)
    
    def insert(self,
               namespace=None,
               name=None,
               type_=None,
               subtype=None,
               username=None,
               extra={},
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
            if self.namespace is not None and namespace != self.namespace:
                raise NamespaceError
        assert isinstance(extra, dict)
        extra_data = utils.pack_extra(extra)
        if self.max_extra_fields_size is not None:
            assert len(extra_data) <= self.max_extra_fields_size
        if not also_insert_blobs:
            return self.db_adapter.insert_object(namespace=namespace,
                                                 name=name,
                                                 type_=type_,
                                                 subtype=subtype,
                                                 username=username,
                                                 extra_data=extra_data,
                                                 return_result=return_result)
        else:
            new_obj = self.db_adapter.insert_object(namespace=namespace,
                                                    name=name,
                                                    type_=type_,
                                                    subtype=subtype,
                                                    username=username,
                                                    extra_data=extra_data,
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
    # TODO: make this accept an object identifier as first argument?
    def get(self,
            object_=None,
            uuid=None,
            uuids=None,
            namespace=None,
            name=None,
            names=None,
            type_=None,
            subtype=None,
            include_hidden=False,
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
        if subtype is not None:
            assert type_ is not None
        return self.db_adapter.get_object(uuid=uuid,
                                          uuids=uuids,
                                          namespace=namespace,
                                          name=name,
                                          names=names,
                                          type_=type_,
                                          subtype=subtype,
                                          include_hidden=include_hidden,
                                          relationship_first=uuid_if_object(rel_first),
                                          relationship_second=uuid_if_object(rel_second),
                                          relationship_type=rel_type,
                                          assert_exists=assert_exists)
    
    def update(self, object_identifier, **kwargs):
        for keyword in kwargs:
            assert keyword in ['new_name', 'type_', 'subtype', 'namespace', 'hidden', 'username', 'extra']
        if isinstance(object_identifier, Object):
            if 'new_name' not in kwargs:
                kwargs['new_name'] = object_identifier.name
            if 'type_' not in kwargs:
                kwargs['type_'] = object_identifier.type_
            if 'subtype' not in kwargs:
                kwargs['subtype'] = object_identifier.subtype
            if 'namespace' not in kwargs:
                kwargs['namespace'] = object_identifier.namespace
            if 'hidden' not in kwargs:
                kwargs['hidden'] = object_identifier.hidden
            if 'username' not in kwargs:
                kwargs['username'] = object_identifier.username
            if 'extra' not in kwargs:
                kwargs['extra'] = object_identifier.extra
        if 'namespace' in kwargs:
            if self.namespace is not None:
                if self.namespace != kwargs['namespace']:
                    raise NamespaceError
        if 'extra' in kwargs:
            assert isinstance(kwargs['extra'], dict)
            kwargs['extra_data'] = utils.pack_extra(kwargs['extra'])
            del kwargs['extra']
            if self.max_extra_fields_size is not None:
                assert len(kwargs['extra_data']) <= self.max_extra_fields_size
        self.db_adapter.update_object(object_identifier=uuid_if_object(object_identifier),
                                      update_kwargs=kwargs,
                                      namespace=self.namespace)

    def delete(self, to_delete, hide_only=True):
        if not hide_only:
            assert self.allow_hard_delete
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
                    *,
                    type_=None,
                    username=None,
                    extra={},
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
        assert isinstance(extra, dict)
        extra_data = utils.pack_extra(extra)
        if self.max_extra_fields_size is not None:
            assert len(extra_data) <= self.max_extra_fields_size
        new_blob = self.db_adapter.insert_blob(
                object_identifier=object_identifier,
                name=name,
                type_=type_,
                username=username,
                extra_data=extra_data,
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

    # Load all blobs for a given object or multiple objects
    def get_blobs(self,
                  obj_identifier,
                  return_by='name',
                  include_hidden=False):
        if isinstance(obj_identifier, list):
            for obj in obj_identifier:
                assert isinstance(obj, Object) or isinstance(obj, uuid.UUID)
            obj_uuids = [uuid_if_object(x) for x in obj_identifier]
            tmp_res = self.db_adapter.get_blobs(object_identifier=obj_uuids,
                                                include_hidden=include_hidden,
                                                check_namespace=self.namespace is not None,
                                                namespace_to_check=self.namespace)
            if return_by == 'name':
                res = {}
                for k, v in tmp_res.items():
                    res[k] = {x.name: x for x in v}
            elif return_by == 'uuid':
                res = {}
                for k, v in tmp_res.items():
                    res[k] = {x.uuid : x for x in v}
            elif return_by is None:
                res = tmp_res
            else:
                raise ValueError
            return res
        else:
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
                    object_or_blob_identifier=None,
                    name=None,
                    uuid=None,
                    **kwargs):
        for keyword in kwargs:
            assert keyword in ['new_name', 'type_', 'extra', 'username', 'hidden', 'data']
        if isinstance(object_or_blob_identifier, Blob):
            uuid = object_or_blob_identifier.uuid
            object_identifier = None
            assert name is None
        else:
            object_identifier = object_or_blob_identifier
        if uuid is None:
            assert object_identifier is not None
        else:
            assert object_identifier is None
            assert name is None
        update_data = False
        if 'data' in kwargs:
            update_data = True
            new_data = kwargs['data']
            del kwargs['data']
            if not isinstance(new_data, bytes):
                new_data = self.serialize(new_data)
                kwargs['serialization'] = self.serialization
            else:
                kwargs['serialization'] = None
            kwargs['size'] = len(new_data)
        if 'extra' in kwargs:
            assert isinstance(kwargs['extra'], dict)
            kwargs['extra_data'] = utils.pack_extra(kwargs['extra'])
            del kwargs['extra']
            if self.max_extra_fields_size is not None:
                assert len(kwargs['extra_data']) <= self.max_extra_fields_size
        cur_uuid = self.db_adapter.update_blob(object_identifier=uuid_if_object(object_identifier),
                                               name=name,
                                               uuid=uuid,
                                               update_kwargs=kwargs,
                                               check_namespace=self.namespace is not None,
                                               namespace_to_check=self.namespace)
        if update_data:
            self.stash.put(self.get_blob_key(cur_uuid), new_data)
    
    def delete_blob(self,
                    to_delete,
                    hide_only=True):
        if not hide_only:
            assert self.allow_hard_delete
        uuid_list = []
        if isinstance(to_delete, list):
            for x in to_delete:
                uuid_list.append(uuid_if_blob(x))
        else:
            uuid_list.append(uuid_if_blob(to_delete))
        self.db_adapter.delete_blobs(uuids=uuid_list,
                                     hide_only=hide_only,
                                     check_namespace=self.namespace is not None,
                                     namespace_to_check=self.namespace)
        if not hide_only:
            for uuid in uuid_list:
                self.stash.delete(self.get_blob_key(uuid))
    
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

    def update_relationship(self,
                            first=None,
                            second=None,
                            type_=None,
                            uuid=None,
                            new_type=None,
                            hidden=None):
        if isinstance(first, Relationship):
            assert second is None
            assert type_ is None
            assert uuid is None
            uuid = first.uuid
            first = None
        return self.db_adapter.update_relationship(first=uuid_if_object(first),
                                                   second=uuid_if_object(second),
                                                   type_=type_,
                                                   uuid=uuid,
                                                   new_type=new_type,
                                                   hidden=hidden,
                                                   check_namespace=self.namespace is not None,
                                                   namespace_to_check=self.namespace)
    
    def delete_relationship(self,
                            to_delete,
                            hide_only=True):
        if not hide_only:
            assert self.allow_hard_delete
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