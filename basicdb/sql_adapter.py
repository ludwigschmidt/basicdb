import contextlib
import datetime

import sqlalchemy as sqla
import sqlalchemy_utils

from . import basicdb
from .db_adapter import DBAdapter
from . import entities
from . import utils


sqlalchemy_base = sqla.ext.declarative.declarative_base()


class Object(sqlalchemy_base):
    __tablename__ = 'objects'
    uuid = sqla.Column(sqla.String, primary_key=True)
    namespace = sqla.Column(sqla.String)
    name = sqla.Column(sqla.String)
    type_ = sqla.Column(sqla.String)
    creation_time = sqla.Column(sqla.DateTime(timezone=False), server_default=sqla.sql.func.now())
    hidden = sqla.Column(sqla.Boolean)
    username = sqla.Column(sqla.String)
    json_data = sqla.Column(sqla.JSON)
    binary_data = sqla.Column(sqla.LargeBinary)

    sqla.UniqueConstraint('namespace', 'name')

    def __repr__(self):
        return f'<Object(uuid="{self.uuid}", namespace="{self.namespace}", name="{self.name}")>'
    
    def to_public(self):
        return entities.Object(uuid=self.uuid,
                               namespace=self.namespace,
                               name=self.name,
                               type_=self.type_,
                               creation_time=self.creation_time.replace(tzinfo=datetime.timezone.utc),
                               hidden=self.hidden,
                               username=self.username,
                               json_data=self.json_data,
                               binary_data=self.binary_data)


class Blob(sqlalchemy_base):
    __tablename__ = 'blobs'
    uuid = sqla.Column(sqla.String, primary_key=True)
    # TODO: add foreign key constraint
    parent = sqla.Column(sqla.String)
    name = sqla.Column(sqla.String)
    type_ = sqla.Column(sqla.String)
    size = sqla.Column(sqla.Integer)
    creation_time = sqla.Column(sqla.DateTime(timezone=False), server_default=sqla.sql.func.now())
    hidden = sqla.Column(sqla.Boolean)
    username = sqla.Column(sqla.String)
    serialization = sqla.Column(sqla.String)
    json_data = sqla.Column(sqla.JSON)

    sqla.UniqueConstraint('parent', 'name')

    def __repr__(self):
        return f'<Blob(uuid="{self.uuid}", parent="{self.parent}", name="{self.name}")>'
    
    def to_public(self):
        return entities.Blob(uuid=self.uuid,
                             parent=self.parent,
                             name=self.name,
                             type_=self.type_,
                             size=self.size,
                             creation_time=self.creation_time.replace(tzinfo=datetime.timezone.utc),
                             hidden=self.hidden,
                             username=self.username,
                             serialization=self.serialization,
                             json_data=self.json_data)


class Relationship(sqlalchemy_base):
    __tablename__ = 'relationships'
    uuid = sqla.Column(sqla.String, primary_key=True)
    # TODO: add foreign key constraint
    first = sqla.Column(sqla.String)
    second = sqla.Column(sqla.String)
    type_ = sqla.Column(sqla.String)
    hidden = sqla.Column(sqla.Boolean)

    sqla.UniqueConstraint('first', 'second', 'type_')

    def __repr__(self):
        return f'<Relationship(uuid="{self.uuid}", first="{self.first}", second="{self.second}")>'
    
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
                                                  expire_on_commit=False)
    
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
                      username=None,
                      json_data=None,
                      binary_data=None,
                      return_result=False):
        new_uuid = utils.gen_uuid_string()
        if name is None:
            name = new_uuid
        with self.session_scope() as session:
            new_obj = Object(uuid=new_uuid,
                             namespace=namespace,
                             name=name,
                             type_=type_,
                             hidden=False,
                             username=username,
                             json_data=json_data,
                             binary_data=binary_data)
            session.add(new_obj)
        if return_result:
            return self.get_object(uuid=new_uuid, assert_exists=True)
    
    def get_object(self,
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
                   assert_exists=False,
                   filter_namespace=True,
                   convert_to_public_class=True,
                   session=None):
        if return_blobs or first_object is not None or second_object is not None:
            raise NotImplementedError()
        singular_query = False
        if uuid is not None or name is not None:
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

        def query(sess):
            return sess.query(Object).options(options).filter(*filters).all()
        res = self.run_query_with_optional_session(query, session=session)

        if assert_exists:
            assert len(res) >= 1
        if convert_to_public_class:
            res = [x.to_public() for x in res] 
        if singular_query:
            assert len(res) <= 1
            if len(res) == 1:
                return res[0]
            else:
                return None
        else:
            return res
    
    def delete_object(self,
                      uuids,
                      hide_only,
                      check_namespace,
                      namespace_to_check):
        if not hide_only:
            raise NotImplementedError
        with self.session_scope() as session:
            objs = self.get_object(uuids=uuids,
                                   session=session,
                                   include_hidden=False,
                                   filter_namespace=check_namespace,
                                   namespace=namespace_to_check,
                                   convert_to_public_class=False)
            assert len(objs) == len(uuids)
            for obj in objs:
                obj.hidden = True
   
    # TODO: merge this into get ?
    def get_object_from_identifier(self,
                                   uuid_or_name,
                                   check_namespace,
                                   namespace,
                                   session):
        uuid_obj = self.get_object(uuid=uuid_or_name, session=session)
        if uuid_obj is None:
            name_obj = self.get_object(name=uuid_or_name, namespace=namespace, session=session)
            return name_obj
        else:
            if check_namespace:
                assert uuid_obj.namespace == namespace
            return uuid_obj

    def insert_blob(self,
                    object_identifier,
                    name,
                    type_,
                    username,
                    json_data,
                    serialization,
                    size,
                    return_result,
                    check_namespace,
                    namespace_to_check):
        new_uuid = utils.gen_uuid_string()
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
                            serialization=serialization,
                            json_data=json_data)
            session.add(new_blob)
        if return_result:
            return self.get_blobs(uuid=new_uuid, assert_exists=True)
    
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
            def query(sess):
                cur_obj = self.get_object_from_identifier(object_identifier,
                                                          check_namespace,
                                                          namespace_to_check,
                                                          sess)
                assert cur_obj is not None
                options = []
                filters = [Blob.parent == cur_obj.uuid]
                if not include_hidden:
                    filters.append(Blob.hidden == False)
                if match_name:
                    filters.append(Blob.name == name)
                return sess.query(Blob).options(options).filter(*filters).all()
            res = self.run_query_with_optional_session(query, session=session)
            if convert_to_public_class:
                res = [x.to_public() for x in res] 
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
                                               include_hidden=False,
                                               filter_namespace=True,
                                               namespace=namespace_to_check,
                                               session=sess)
                    assert len(objs_uuids) == tmp_objs
                return cur_res
            res = self.run_query_with_optional_session(query, session=session)
            if convert_to_public_class:
                res = [x.to_public() for x in res] 
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

    def delete_blobs(self,
                     uuids,
                     hide_only,
                     check_namespace,
                     namespace_to_check):
        if not hide_only:
            raise NotImplementedError
        with self.session_scope() as session:
            blobs = self.get_blobs(uuids=uuids,
                                   include_hidden=False,
                                   check_namespace=check_namespace,
                                   namespace_to_check=namespace_to_check,
                                   convert_to_public_class=False,
                                   session=session)
            assert len(blobs) == len(uuids)
            for blob in blobs:
                blob.hidden = True

    def insert_relationship(self,
                            *,
                            first,
                            second,
                            type_,
                            resturn_result,
                            check_namespace,
                            namespace_to_check):
        raise NotImplementedError

    def get_relationships(self,
                          *,
                          uuid=None,
                          first=None,
                          second=None,
                          type_=None,
                          include_hidden=False,
                          assert_exists=False):
        raise NotImplementedError

    def delete_relationships(self,
                             uuids,
                             hide_only,
                             check_namespace,
                             namespace_to_check):
        raise NotImplementedError