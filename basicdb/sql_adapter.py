import contextlib
import datetime
import uuid

import sqlalchemy as sqla
import sqlalchemy_utils

from . import basicdb
from .db_adapter import DBAdapter
from . import entities
from . import exceptions
from . import utils


sqlalchemy_base = sqla.ext.declarative.declarative_base()

# From https://gist.github.com/gmolveau/7caeeefe637679005a7bb9ae1b5e421e
class GenericUUID(sqla.types.TypeDecorator):
    """Platform-independent UUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    """
    impl = sqla.types.CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(sqla.dialects.postgresql.UUID())
        else:
            return dialect.type_descriptor(sqla.types.CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


class Object(sqlalchemy_base):
    __tablename__ = 'objects'
    uuid = sqla.Column(GenericUUID, primary_key=True)
    namespace = sqla.Column(sqla.String)
    name = sqla.Column(sqla.String, nullable=False)
    type_ = sqla.Column(sqla.String)
    subtype = sqla.Column(sqla.String)
    creation_time = sqla.Column(sqla.DateTime(timezone=False), server_default=sqla.sql.func.now())
    modification_time = sqla.Column(sqla.DateTime(timezone=False))
    hidden = sqla.Column(sqla.Boolean, nullable=False)
    username = sqla.Column(sqla.String)
    extra_data = sqla.Column(sqla.LargeBinary)
    __table_args__ = (sqla.Index('object_unique1', 'namespace', 'name', unique=True, postgresql_where=namespace.isnot(None), sqlite_where=namespace.isnot(None)),
                      sqla.Index('object_unique2', 'name',              unique=True, postgresql_where=namespace.is_(None),   sqlite_where=namespace.is_(None)))


    def __repr__(self):
        return f'<Object(uuid="{self.uuid.hex}", namespace="{self.namespace}", name="{self.name}")>'
    
    def to_public(self):
        return entities.Object(uuid=self.uuid,
                               namespace=self.namespace,
                               name=self.name,
                               type_=self.type_,
                               subtype=self.subtype,
                               creation_time=self.creation_time.replace(tzinfo=datetime.timezone.utc),
                               modification_time=self.modification_time.replace(tzinfo=datetime.timezone.utc) if self.modification_time is not None else None,
                               hidden=self.hidden,
                               username=self.username,
                               extra=utils.unpack_extra(self.extra_data))


class Blob(sqlalchemy_base):
    __tablename__ = 'blobs'
    uuid = sqla.Column(GenericUUID, primary_key=True)
    # TODO: add foreign key constraint ?
    parent = sqla.Column(GenericUUID, nullable=False)
    name = sqla.Column(sqla.String)
    type_ = sqla.Column(sqla.String)
    size = sqla.Column(sqla.Integer)
    creation_time = sqla.Column(sqla.DateTime(timezone=False), server_default=sqla.sql.func.now())
    modification_time = sqla.Column(sqla.DateTime(timezone=False))
    hidden = sqla.Column(sqla.Boolean, nullable=False)
    username = sqla.Column(sqla.String)
    serialization = sqla.Column(sqla.String)
    extra_data = sqla.Column(sqla.LargeBinary)
    __table_args__ = (sqla.Index('blob_unique1', 'parent', 'name', unique=True, postgresql_where=name.isnot(None), sqlite_where=name.isnot(None)),
                      sqla.Index('blob_unique2', 'parent',         unique=True, postgresql_where=name.is_(None),   sqlite_where=name.is_(None)))

    def __repr__(self):
        return f'<Blob(uuid="{self.uuid.hex}", parent="{self.parent}", name="{self.name}")>'
    
    def to_public(self):
        return entities.Blob(uuid=self.uuid,
                             parent=self.parent,
                             name=self.name,
                             type_=self.type_,
                             size=self.size,
                             creation_time=self.creation_time.replace(tzinfo=datetime.timezone.utc),
                             modification_time=self.modification_time.replace(tzinfo=datetime.timezone.utc) if self.modification_time is not None else None,
                             hidden=self.hidden,
                             username=self.username,
                             serialization=self.serialization,
                             extra=utils.unpack_extra(self.extra_data))


class Relationship(sqlalchemy_base):
    __tablename__ = 'relationships'
    uuid = sqla.Column(GenericUUID, primary_key=True)
    # TODO: add foreign key constraint ?
    first = sqla.Column(GenericUUID, nullable=False)
    second = sqla.Column(GenericUUID, nullable=False)
    type_ = sqla.Column(sqla.String)
    hidden = sqla.Column(sqla.Boolean, nullable=False)
    __table_args__ = (sqla.Index('relationship_unique1', 'first', 'second', 'type_', unique=True, postgresql_where=type_.isnot(None), sqlite_where=type_.isnot(None)),
                      sqla.Index('relationship_unique2', 'first', 'second',          unique=True, postgresql_where=type_.is_(None),   sqlite_where=type_.is_(None)))

    def __repr__(self):
        return f'<Relationship(uuid="{self.uuid.hex}", first="{self.first.hex}", second="{self.second.hex}")>'
    
    def to_public(self):
        return entities.Relationship(uuid=self.uuid,
                                     first=self.first,
                                     second=self.second,
                                     type_=self.type_,
                                     hidden=self.hidden)


class SQLAdapter(DBAdapter):
    def __init__(self,
                 connection_string,
                 namespace=None,
                 create_db=False,
                 echo_sql=False,
                 pool_pre_ping=True):
        self.connection_string = connection_string
        self.namespace = namespace
        if create_db:
            if not sqlalchemy_utils.functions.database_exists(self.connection_string):
                sqlalchemy_utils.functions.create_database(self.connection_string)
        elif not sqlalchemy_utils.functions.database_exists(self.connection_string):
            raise ValueError(f'The database "{connection_string}" does not exist')
        self.engine = sqla.create_engine(self.connection_string,
                                         echo=echo_sql,
                                         pool_pre_ping=pool_pre_ping)
        sqlalchemy_base.metadata.create_all(self.engine)
        self.sessionmaker = sqla.orm.sessionmaker(bind=self.engine,
                                                  expire_on_commit=True)
    
    @contextlib.contextmanager
    def session_scope(self):
        session = self.sessionmaker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    
    def run_query_with_optional_session(self, query, session=None):
        if session is None:
            with self.session_scope() as sess:
                return query(sess)
        else:
            return query(session)

    def insert_object(self,
                      namespace,
                      name=None,
                      type_=None,
                      subtype=None,
                      username=None,
                      extra_data=None,
                      return_result=False):
        new_uuid = uuid.uuid4()
        if name is None:
            name = str(new_uuid)
        try:
            with self.session_scope() as session:
                new_obj = Object(uuid=new_uuid,
                                 namespace=namespace,
                                 name=name,
                                 type_=type_,
                                 subtype=subtype,
                                 hidden=False,
                                 username=username,
                                 modification_time=None,
                                 extra_data=extra_data)
                session.add(new_obj)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e
        if return_result:
            return self.get_object(uuid=new_uuid,
                                   assert_exists=True,
                                   filter_namespace=False,
                                   convert_to_public_class=True)
    
    def get_object(self,
                   uuid=None,
                   uuids=None,
                   namespace=None,
                   name=None,
                   names=None,
                   type_=None,
                   subtype=None,
                   include_hidden=False,
                   relationship_first=None,
                   relationship_second=None,
                   relationship_type=None,
                   assert_exists=False,
                   filter_namespace=True,
                   convert_to_public_class=True,
                   session=None):
        if relationship_type is not None:
            assert relationship_first is not None or relationship_second is not None
        assert not (relationship_first is not None and relationship_second is not None)
        singular_query = False
        if uuid is not None or (name is not None and filter_namespace):
            singular_query = True

        options = []
        filters = []
        if not include_hidden:
            filters.append(Object.hidden == False)
        if uuid is not None:
            filters.append(Object.uuid == uuid)
        if uuids is not None:
            filters.append(Object.uuid.in_(uuids))
        if name is not None:
            filters.append(Object.name == name)
        if names is not None:
            filters.append(Object.name.in_(names))
        if filter_namespace or namespace is not None:
            filters.append(Object.namespace == namespace)
        if type_ is not None:
            filters.append(Object.type_ == type_)
        if subtype is not None:
            filters.append(Object.subtype == subtype)

        def query(sess):
            if relationship_first is not None or relationship_second is not None:
                rels = self.get_relationships(first=relationship_first,
                                              second=relationship_second,
                                              type_=relationship_type,
                                              include_hidden=include_hidden,
                                              namespace=namespace,
                                              filter_namespace=False,
                                              convert_to_public_class=False,
                                              assert_exists=False,
                                              session=sess)
                if relationship_first is not None:
                    rel_other_uuids = list(set([x.second for x in rels]))
                else:
                    rel_other_uuids = list(set([x.first for x in rels]))
                filters.append(Object.uuid.in_(rel_other_uuids))
            inner_res = sess.query(Object).options(options).filter(*filters).all()
            if convert_to_public_class:
                inner_res = [x.to_public() for x in inner_res]
            return inner_res
        res = self.run_query_with_optional_session(query, session=session)

        if assert_exists:
            assert len(res) >= 1
        if singular_query:
            assert len(res) <= 1
            if len(res) == 1:
                return res[0]
            else:
                return None
        else:
            return res
    
    def update_object(self,
                      object_identifier,
                      update_kwargs,
                      namespace,
                      session=None):
        def query(sess):
            cur_obj = self.get_object_from_identifier(object_identifier,
                                                      check_namespace=namespace is not None,
                                                      namespace=namespace,
                                                      assert_exists=True,
                                                      include_hidden=True,
                                                      session=sess)
            for keyword, new_value in update_kwargs.items():
                if keyword == 'new_name':
                    cur_obj.name = new_value
                elif keyword == 'hidden':
                    cur_obj.hidden = new_value
                elif keyword == 'username':
                    cur_obj.username = new_value
                elif keyword == 'extra_data':
                    cur_obj.extra_data = new_value
                elif keyword == 'type_':
                    cur_obj.type_ = new_value
                elif keyword == 'subtype':
                    cur_obj.subtype = new_value
                elif keyword == 'namespace':
                    cur_obj.namespace = new_value
                else:
                    raise ValueError
            cur_obj.modification_time = datetime.datetime.now(datetime.timezone.utc)
        try:
            return self.run_query_with_optional_session(query, session=session)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e
    
    def delete_objects(self,
                       uuids,
                       hide_only,
                       check_namespace,
                       namespace_to_check):
        with self.session_scope() as session:
            objs = self.get_object(uuids=uuids,
                                   session=session,
                                   include_hidden=True,
                                   filter_namespace=check_namespace,
                                   namespace=namespace_to_check,
                                   convert_to_public_class=False)
            assert len(objs) == len(uuids)
            for obj in objs:
                if hide_only:
                    obj.hidden = True
                    obj.modification_time = datetime.datetime.now(datetime.timezone.utc)
                else:
                    # TODO: can probably make this faster by using key constraints instead
                    if len(self.get_blobs(obj.uuid, include_hidden=True)) != 0:
                        raise exceptions.DeletionError(f'Object {obj.uuid} still has {len(self.get_blobs(obj.uuid))} blobs')
                    num_first_relationships = len(self.get_relationships(first=obj.uuid, include_hidden=True))
                    if num_first_relationships != 0:
                        raise exceptions.DeletionError(f'Object {obj.uuid} still is the first element in {num_first_relationships} relationships')
                    num_second_relationships = len(self.get_relationships(second=obj.uuid, include_hidden=True))
                    if num_second_relationships != 0:
                        raise exceptions.DeletionError(f'Object {obj.uuid} still is the second element in {num_second_relationships} relationships')
                    session.delete(obj)
   
    # TODO: merge this into get ?
    def get_object_from_identifier(self,
                                   uuid_or_name,
                                   check_namespace,
                                   namespace,
                                   session,
                                   include_hidden=False,
                                   assert_exists=False,
                                   convert_to_public_class=False):
        if isinstance(uuid_or_name, uuid.UUID):
            return self.get_object(uuid=uuid_or_name,
                                   filter_namespace=check_namespace,
                                   namespace=namespace,
                                   session=session,
                                   include_hidden=include_hidden,
                                   assert_exists=assert_exists,
                                   convert_to_public_class=convert_to_public_class)
        else:
            return self.get_object(name=uuid_or_name,
                                   filter_namespace=True,
                                   namespace=namespace,
                                   session=session,
                                   include_hidden=include_hidden,
                                   assert_exists=assert_exists,
                                   convert_to_public_class=convert_to_public_class)

    def insert_blob(self,
                    object_identifier,
                    name,
                    type_,
                    username,
                    extra_data,
                    serialization,
                    size,
                    return_result,
                    check_namespace,
                    namespace_to_check):
        new_uuid = uuid.uuid4()
        try:
            with self.session_scope() as session:
                obj = self.get_object_from_identifier(object_identifier,
                                                      check_namespace,
                                                      namespace_to_check,
                                                      session)
                assert obj is not None
                new_blob = Blob(uuid=new_uuid,
                                parent=obj.uuid,
                                name=name,
                                type_=type_,
                                size=size,
                                hidden=False,
                                username=username,
                                modification_time=None,
                                serialization=serialization,
                                extra_data=extra_data)
                session.add(new_blob)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e
        if return_result:
            return self.get_blobs(uuid=new_uuid,
                                  assert_exists=True,
                                  check_namespace=False,
                                  convert_to_public_class=True)
    
    def get_blobs(self,
                  object_identifier=None,
                  match_name=False,
                  name=None,
                  uuid=None,
                  uuids=None,
                  include_hidden=False,
                  check_namespace=False,
                  namespace_to_check=None,
                  convert_to_public_class=True,
                  assert_exists=False,
                  session=None):
        if object_identifier is not None:
            assert uuid is None and uuids is None
            if isinstance(object_identifier, list):
                assert not assert_exists
                assert not match_name
                assert name is None
                def query(sess):
                    cur_objs = self.get_object(uuids=object_identifier,
                                               filter_namespace=check_namespace,
                                               namespace=namespace_to_check,
                                               convert_to_public_class=False,
                                               session=sess,
                                               include_hidden=include_hidden)
                    assert len(cur_objs) == len(object_identifier)
                    options = []
                    filters = [Blob.parent.in_(object_identifier)]
                    if not include_hidden:
                        filters.append(Blob.hidden == False)
                    inner_res = sess.query(Blob).options(options).filter(*filters).all()
                    if convert_to_public_class:
                        inner_res = [x.to_public() for x in inner_res] 
                    res_by_parent_uuid = {}
                    for cr in inner_res:
                        if cr.parent not in res_by_parent_uuid:
                            res_by_parent_uuid[cr.parent] = []
                        res_by_parent_uuid[cr.parent].append(cr)
                    return res_by_parent_uuid
                return self.run_query_with_optional_session(query, session=session)
            else:
                def query(sess):
                    cur_obj = self.get_object_from_identifier(object_identifier,
                                                              check_namespace,
                                                              namespace_to_check,
                                                              sess,
                                                              include_hidden=include_hidden)
                    if cur_obj is None:
                        raise exceptions.ObjectNotFoundError(object_identifier)
                    options = []
                    filters = [Blob.parent == cur_obj.uuid]
                    if not include_hidden:
                        filters.append(Blob.hidden == False)
                    if match_name:
                        filters.append(Blob.name == name)
                    inner_res = sess.query(Blob).options(options).filter(*filters).all()
                    if convert_to_public_class:
                        inner_res = [x.to_public() for x in inner_res] 
                    return inner_res
                res = self.run_query_with_optional_session(query, session=session)
                if match_name:
                    assert len(res) <= 1
                    if assert_exists:
                        assert len(res) == 1
                    if len(res) == 1:
                        return res[0]
                    else:
                        return None
                else:
                    return res
        else:
            assert uuid is not None or uuids is not None
            assert uuid is None or uuids is None
            options = []
            if uuid is not None:
                filters = [Blob.uuid == uuid]
            else:
                filters = [Blob.uuid.in_(uuids)]
            if not include_hidden:
                filters.append(Blob.hidden == False)
            def query(sess):
                cur_res = sess.query(Blob).options(options).filter(*filters).all()
                if check_namespace:
                    objs_uuids = list(set([x.parent for x in cur_res]))
                    tmp_objs = self.get_object(uuids=objs_uuids,
                                               include_hidden=include_hidden,
                                               filter_namespace=True,
                                               namespace=namespace_to_check,
                                               session=sess)
                    assert len(objs_uuids) == len(tmp_objs)
                if convert_to_public_class:
                    cur_res = [x.to_public() for x in cur_res]
                return cur_res
            res = self.run_query_with_optional_session(query, session=session)
            if uuid is not None:
                if assert_exists:
                    assert len(res) == 1
                if len(res) == 1:
                    return res[0]
                else:
                    return None
            else:
                if assert_exists:
                    assert len(res) >= 1
                return res
    
    def update_blob(self,
                    object_identifier,
                    name,
                    uuid,
                    update_kwargs,
                    check_namespace,
                    namespace_to_check,
                    session=None):
        def query(sess):
            cur_blob = self.get_blobs(object_identifier=object_identifier,
                                      name=name,
                                      match_name=True,
                                      uuid=uuid,
                                      include_hidden=True,
                                      check_namespace=check_namespace,
                                      namespace_to_check=namespace_to_check,
                                      assert_exists=True,
                                      convert_to_public_class=False,
                                      session=sess)
            for keyword, new_value in update_kwargs.items():
                if keyword == 'new_name':
                    cur_blob.name = new_value
                elif keyword == 'hidden':
                    cur_blob.hidden = new_value
                elif keyword == 'serialization':
                    cur_blob.serialization = new_value
                elif keyword == 'size':
                    cur_blob.size = new_value
                elif keyword == 'username':
                    cur_blob.username = new_value
                elif keyword == 'extra_data':
                    cur_blob.extra_data = new_value
                elif keyword == 'type_':
                    cur_blob.type_ = new_value
                else:
                    raise ValueError
            cur_blob.modification_time = datetime.datetime.now(datetime.timezone.utc)
            return cur_blob.uuid
        try:
            return self.run_query_with_optional_session(query, session=session)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e

    def delete_blobs(self,
                     uuids,
                     hide_only,
                     check_namespace,
                     namespace_to_check):
        with self.session_scope() as session:
            blobs = self.get_blobs(uuids=uuids,
                                   include_hidden=True,
                                   check_namespace=check_namespace,
                                   namespace_to_check=namespace_to_check,
                                   convert_to_public_class=False,
                                   session=session)
            assert len(blobs) == len(uuids)
            for blob in blobs:
                if hide_only:
                    blob.hidden = True
                    blob.modification_time = datetime.datetime.now(datetime.timezone.utc)
                else:
                    session.delete(blob)

    def insert_relationship(self,
                            *,
                            first,
                            second,
                            type_,
                            return_result,
                            check_namespace,
                            namespace_to_check):
        new_uuid = uuid.uuid4()
        try:
            with self.session_scope() as session:
                first = self.get_object_from_identifier(first,
                                                        check_namespace,
                                                        namespace_to_check,
                                                        session)
                assert first is not None
                second = self.get_object_from_identifier(second,
                                                        check_namespace,
                                                        namespace_to_check,
                                                        session)
                assert second is not None
                new_rel = Relationship(uuid=new_uuid,
                                    first=first.uuid,
                                    second=second.uuid,
                                    type_=type_,
                                    hidden=False)
                session.add(new_rel)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e
        if return_result:
            return self.get_relationships(uuid=new_uuid,
                                          assert_exists=True,
                                          filter_namespace=False)

    def get_relationships(self,
                          *,
                          uuid=None,
                          uuids=None,
                          first=None,
                          second=None,
                          type_=None,
                          namespace=None,
                          filter_namespace=False,
                          include_hidden=False,
                          assert_exists=False,
                          convert_to_public_class=True,
                          session=None):
        # TODO: make the namespace filtering faster with a real JOIN?
        if uuid is None and uuids is None:
            options = []
            filters = []
            if not include_hidden:
                filters.append(Relationship.hidden == False)
            if type_ is not None:
                if isinstance(type_, list):
                    filters.append(Relationship.type_.in_(type_))
                else:
                    assert isinstance(type_, str)
                    filters.append(Relationship.type_ == type_)
            def query(sess):
                if first is not None:
                    first_obj = self.get_object_from_identifier(first,
                                                                filter_namespace,
                                                                namespace,
                                                                sess,
                                                                include_hidden=include_hidden)
                    if first_obj is None:
                        return []
                    filters.append(Relationship.first == first_obj.uuid)
                if second is not None:
                    second_obj = self.get_object_from_identifier(second,
                                                                 filter_namespace,
                                                                 namespace,
                                                                 sess,
                                                                 include_hidden=include_hidden)
                    if second_obj is None:
                        return []
                    filters.append(Relationship.second == second_obj.uuid)
                cur_res = sess.query(Relationship).options(options).filter(*filters).all()
                if first is None and second is None and filter_namespace:
                    objs_uuids = list(set([x.first for x in cur_res]))
                    tmp_objs = self.get_object(uuids=objs_uuids,
                                               include_hidden=include_hidden,
                                               filter_namespace=True,
                                               namespace=namespace,
                                               session=sess)
                    namespace_by_first_uuid = {x.uuid: x.namespace for x in tmp_objs}
                    cur_res = [x for x in cur_res if namespace_by_first_uuid[x.first] == namespace]
                if convert_to_public_class:
                    cur_res = [x.to_public() for x in cur_res]
                return cur_res
            res = self.run_query_with_optional_session(query, session=session)
            if first is not None and second is not None and type_ is not None:
                if assert_exists:
                    assert len(res) == 1
                if len(res) == 1:
                    return res[0]
                else:
                    return None
            else:
                if assert_exists:
                    assert len(res) >= 1
                return res
        else:
            assert uuid is None or uuids is None
            assert first is None and second is None
            assert type_ is None
            options = []
            if uuid is not None:
                filters = [Relationship.uuid == uuid]
            else:
                filters = [Relationship.uuid.in_(uuids)]
            if not include_hidden:
                filters.append(Relationship.hidden == False)
            def query(sess):
                cur_res = sess.query(Relationship).options(options).filter(*filters).all()
                if filter_namespace:
                    objs_uuids = list(set([x.first for x in cur_res]))
                    tmp_objs = self.get_object(uuids=objs_uuids,
                                               include_hidden=include_hidden,
                                               filter_namespace=True,
                                               namespace=namespace,
                                               session=sess)
                    namespace_by_first_uuid = {x.uuid: x.namespace for x in tmp_objs}
                    cur_res2 = [x for x in cur_res if namespace_by_first_uuid[x.first] == namespace]
                else:
                    cur_res2 = cur_res
                if convert_to_public_class:
                    cur_res2 = [x.to_public() for x in cur_res2]
                return cur_res2
            res = self.run_query_with_optional_session(query, session=session)
            if uuid is None:
                if assert_exists:
                    assert len(res) >= 1
                return res
            else:
                if assert_exists:
                    assert len(res) == 1
                if len(res) == 1:
                    return res[0]
                else:
                    return None
    
    def update_relationship(self,
                            first,
                            second,
                            type_,
                            uuid,
                            new_type,
                            hidden,
                            check_namespace,
                            namespace_to_check,
                            session=None):
        def query(sess):
            rel = self.get_relationships(first=first,
                                         second=second,
                                         type_=type_,
                                         uuid=uuid,
                                         filter_namespace=check_namespace,
                                         namespace=namespace_to_check,
                                         include_hidden=True,
                                         assert_exists=True,
                                         convert_to_public_class=False,
                                         session=sess)
            if isinstance(rel, list):
                assert len(rel) == 1
                rel = rel[0]
            if new_type is not None:
                rel.type_ = new_type
            if hidden is not None:
                rel.hidden = hidden
        try:
            return self.run_query_with_optional_session(query, session=session)
        except sqla.exc.IntegrityError as e:
            raise exceptions.IntegrityError from e

    def delete_relationships(self,
                             uuids,
                             hide_only,
                             check_namespace,
                             namespace_to_check):
        with self.session_scope() as session:
            rels = self.get_relationships(
                    uuids=uuids,
                    include_hidden=True,
                    filter_namespace=check_namespace,
                    namespace=namespace_to_check,
                    convert_to_public_class=False,
                    session=session)
            assert len(rels) == len(uuids)
            for rel in rels:
                if hide_only:
                    rel.hidden = True
                else:
                    session.delete(rel)