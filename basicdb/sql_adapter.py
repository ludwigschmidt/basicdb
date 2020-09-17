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
                print('Creating DB')
                sqlalchemy_utils.functions.create_database(self.connection_string)
            else:
                print('DB exists')
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
                   assert_exists=False):
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
            filters.append(Object.uuid == uuids)
        if uuids is not None:
            filters.append(Object.uuid.in_(uuids))
        if name is not None:
            filters.append(Object.name == name)
        if names is not None:
            filters.append(Object.name.in_(names))
        if namespace is not None:
            filters.append(Object.namespace == namespace)
        if type_ is not None:
            filters.append(Object.type_ == type_)

        def query(sess):
            return sess.query(Object).options(options).filter(*filters).all()
        res = self.run_query_with_optional_session(query)

        if assert_exists:
            assert len(res) >= 1
        res = [x.to_public() for x in res] 
        if singular_query:
            assert len(res) <= 1
            if len(res) == 1:
                return res[0]
            else:
                return None
        else:
            return res