import datetime
import os
import uuid

import boto3
from moto import mock_s3
import pytest

import basicdb
from basicdb import __version__, BasicDB, IntegrityError, NamespaceError


def test_version():
    assert __version__ == '0.0.6'


def simple_test(db):
    db.insert(name='test1',
              extra={'foo': 'bar'})
    res = db.get(name='test1')
    test1 = res
    assert res.name == 'test1'
    assert isinstance(res.uuid, uuid.UUID)
    assert res.extra['foo'] == 'bar'
    assert res['foo'] == 'bar'
    assert res.foo == 'bar'
    with pytest.raises(KeyError):
        assert res.foo2 == 'bar'
    assert res.namespace == db.namespace
    assert res.type_ is None
    assert res.subtype is None
    assert not res.hidden
    assert res.username is not None
    assert (res.creation_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds() <= 1
    res = db.get()
    assert len(res) == 1
    assert res[0].name == 'test1'
    
    db.insert(name='test2',
              extra={'key': 5},
              username='testuser')
    res = db.get(name='test1')
    assert res.name == 'test1'
    assert res.extra['foo'] == 'bar'
    res = db.get()
    assert len(res) == 2
    assert set([x.name for x in res]) == set(['test1', 'test2'])
    res = db.get(names=['test2'])
    assert len(res) == 1
    res = res[0]
    assert res.name == 'test2'
    assert res['key'] == 5
    assert res.username == 'testuser'

    db.insert(name='test3') 
    assert set([x.name for x in db.get()]) == set(['test1', 'test2', 'test3'])
    db.delete(db.get(name='test2'))
    assert set([x.name for x in db.get()]) == set(['test1', 'test3'])
    res = db.get(names=['test2'])
    assert len(res) == 0
    res = db.get(names=['test2'], include_hidden=True)
    assert len(res) == 1
    res = res[0]
    assert res.name == 'test2'
    assert res['key'] == 5
    assert res.username == 'testuser'

    db.update('test2', extra={2:3}, username='someone_else', hidden=False)
    res = db.get(name='test2')
    assert res.username == 'someone_else'
    assert res[2] == 3
    assert len(res.extra) == 1

    res[2] = 4
    db.update(res)
    assert db.get(name='test2')[2] == 4

    test2 = db.get(name='test2') 
    test2.username = 'third_person'
    test2.not_a_field = 'hello setattr'
    db.update(test2)
    res = db.get(name='test2')
    assert res.username == 'third_person'
    assert res.not_a_field == 'hello setattr'

    with pytest.raises(IntegrityError):
        db.insert(name='test1')
    
    with pytest.raises(IntegrityError):
        db.update('test1', new_name='test3')

    if db.namespace is None: 
        test1_space1 = db.insert(name='test1', namespace='space1')
        assert db.get(name='test1').uuid == test1.uuid
        assert db.get(name='test1', namespace='space1').uuid == test1_space1.uuid
        db.update('test1', namespace='space2')
        assert db.get(name='test1') is None
        assert db.get(name='test1', namespace='space2').uuid == test1.uuid
        with pytest.raises(IntegrityError):
            db.update(test1, namespace='space1')
        db.update(test1, namespace=None)
        assert db.get(name='test1').uuid == test1.uuid
    else:
        assert db.get(name='test1', namespace=db.namespace).uuid == test1.uuid
        with pytest.raises(NamespaceError):
            test1_space1 = db.insert(name='test1', namespace='space1')
        with pytest.raises(NamespaceError):
            db.update('test1', namespace='space1')


def simple_noname_test(db):
    db.insert(name='test1', extra={'foo': 'bar'})
    assert set([x.name for x in db.get()]) == set(['test1'])

    unnamed = db.insert(extra={'name': 'unnamed'})
    assert set([x.name for x in db.get()]) == set(['test1', str(unnamed.uuid)])

    res = db.get(name=str(unnamed.uuid))
    assert res.uuid == unnamed.uuid

    db.update(unnamed, new_name='test2')
    
    assert set([x.name for x in db.get()]) == set(['test1', 'test2'])
    assert db.get(name=str(unnamed.uuid)) == None


def simple_type_test(db):
    db.insert(name='test1', type_='a', subtype='a1')
    db.insert(name='test2', type_='b', subtype='b1')
    db.insert(name='test3', type_='b', subtype='b1')
    db.insert(name='test4', type_='a')
    db.insert(name='test5', type_='a', subtype='a2')

    assert set([x.name for x in db.get(type_='a')]) == set(['test1', 'test4', 'test5'])
    assert set([x.name for x in db.get(type_='b')]) == set(['test2', 'test3'])
    assert set([x.name for x in db.get(type_='b', subtype='b1')]) == set(['test2', 'test3'])
    assert set([x.name for x in db.get(type_='a', subtype='a1')]) == set(['test1'])

    db.update('test5', subtype='a1')
    assert set([x.name for x in db.get(type_='a', subtype='a1')]) == set(['test1', 'test5'])
    
    db.update('test5', type_='c')
    assert set([x.name for x in db.get(type_='a', subtype='a1')]) == set(['test1'])


def simple_blob_test(db):
    blob_data = [2, 3, 5, 7, 11]
    db.insert(name='test1', blob=blob_data)
    loaded_blob = db.load_blob('test1')
    assert loaded_blob == blob_data
    
    with pytest.raises(IntegrityError):
        db.insert_blob('test1', name=None, data=b'')

    test_obj2 = db.insert(name='test2')
    blob_data = {'a': 0,
                 'b': b'12345',
                 'c': 100}
    for k, v in blob_data.items():
        db.insert_blob('test2', name=k, data=v)
    blobs = db.get_blobs('test2')
    for k, v in blobs.items():
        assert v.name == k
        assert v.modification_time is None
    assert set(blobs.keys()) == set(blob_data.keys())
    for k, v in blob_data.items():
        assert db.load_blob('test2', k) == v
    for k, v in blob_data.items():
        assert db.load_blob(test_obj2.uuid, k) == v
    for k, v in blob_data.items():
        assert db.load_blob(test_obj2, k) == v
    for blob in blobs.values():
        assert db.load_blob(uuid=blob.uuid) == blob_data[blob.name]
    for blob in blobs.values():
        assert db.load_blob(blob) == blob_data[blob.name]
    obj2_blob_data = db.load_blobs('test2', names=blobs.keys())
    assert len(obj2_blob_data) == len(blob_data)
    for k, v in blob_data.items():
        assert obj2_blob_data[k] == v
    obj2_blob_data = db.load_blobs('test2')
    assert len(obj2_blob_data) == len(blob_data)
    for k, v in blob_data.items():
        assert obj2_blob_data[k] == v
    
    if db.namespace is None:
        db.insert(name='test2', namespace='other_space')
        assert len(db.get_blobs(db.get(name='test2'))) == 3
        assert len(db.get_blobs(db.get(name='test2', namespace='other_space'))) == 0
        for k, v in blob_data.items():
            assert db.load_blob('test2', k) == v

    db.delete_blob(db.get_blobs('test2')['b'])
    del blob_data['b']
    assert len(blob_data) == 2
    blobs = db.get_blobs('test2')
    assert set(blobs.keys()) == set(blob_data.keys())
    for k, v in blob_data.items():
        assert db.load_blob('test2', k) == v
    
    db.update_blob('test2', 'a', data=1)
    db.update_blob('test2', 'c', new_name='d', data=200)
    blob_data['a'] = 1
    del blob_data['c']
    blob_data['d'] = 200
    assert len(blob_data) == 2
    assert blob_data['a'] == 1
    assert blob_data['d'] == 200
    blobs = db.get_blobs('test2')
    assert set(blobs.keys()) == set(blob_data.keys())
    for k, v in blob_data.items():
        assert db.load_blob('test2', k) == v
    assert blobs['a'].modification_time is not None
    assert blobs['d'].modification_time is not None
    assert (blobs['a'].modification_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds() <= 1
    assert (blobs['d'].modification_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds() <= 1

    db.insert_blob('test1', 'test1', extra={1: 2}, data=b'0000')
    blobs = db.get_blobs('test1')
    assert len(blobs) == 2
    assert set([x for x in blobs.keys()]) == set([None, 'test1'])
    assert set([x.name for x in blobs.values()]) == set([None, 'test1'])
    assert blobs['test1'][1] == 2

    with pytest.raises(IntegrityError):
        db.update_blob('test1', 'test1', new_name=None)


def simple_relationship_test(db):
    test1 = db.insert(name='test1')
    test2 = db.insert(name='test2')
    test3 = db.insert(name='test3')
    db.insert_relationship(first='test1', second='test2', type_='child')
    db.insert_relationship(first='test3', second='test2', type_='child2')
    
    with pytest.raises(IntegrityError):
        db.insert_relationship(first='test3', second='test2', type_='child2')

    tmp = db.get_relationship(first='test1')
    assert len(tmp) == 1
    tmp = tmp[0]
    assert tmp.first == test1.uuid
    assert tmp.second == test2.uuid
    assert tmp.type_ == 'child'
    
    tmp = db.get_relationship(first=test1)
    assert len(tmp) == 1
    tmp = tmp[0]
    assert tmp.first == test1.uuid
    assert tmp.second == test2.uuid
    assert tmp.type_ == 'child'
    
    tmp = db.get_relationship(second='test2')
    assert len(tmp) == 2
    assert set([x.first for x in tmp]) == set([test1.uuid, test3.uuid])
    assert set([x.second for x in tmp]) == set([test2.uuid])
    assert set([x.type_ for x in tmp]) == set(['child', 'child2'])
    
    tmp = db.get_relationship(second='test2', type_='child')
    assert len(tmp) == 1
    assert set([x.first for x in tmp]) == set([test1.uuid])
    assert set([x.second for x in tmp]) == set([test2.uuid])
    assert set([x.type_ for x in tmp]) == set(['child'])

    db.update_relationship(first=test3, second=test2, type_='child2', new_type='child')
    tmp = db.get_relationship(second='test2', type_='child')
    assert len(tmp) == 2
    assert set([x.first for x in tmp]) == set([test1.uuid, test3.uuid])
    assert set([x.second for x in tmp]) == set([test2.uuid])
    assert set([x.type_ for x in tmp]) == set(['child'])
    db.update_relationship(db.get_relationship(first=test3, second=test2, type_='child'), new_type='child2')
    
    tmp = db.get_relationship(second='test1')
    assert len(tmp) == 0

    db.delete_relationship(db.get_relationship(first='test1', second='test2'))
    tmp = db.get_relationship(first='test1')
    assert len(tmp) == 0
    
    tmp = db.get_relationship(first='test1', include_hidden=True)
    assert len(tmp) == 1
    tmp = tmp[0]
    assert tmp.first == test1.uuid
    assert tmp.second == test2.uuid
    assert tmp.type_ == 'child'
    
    tmp = db.get_relationship(second='test2')
    assert len(tmp) == 1
    assert set([x.first for x in tmp]) == set([test3.uuid])
    assert set([x.second for x in tmp]) == set([test2.uuid])
    assert set([x.type_ for x in tmp]) == set(['child2'])
    
    db.insert_relationship(first='test3', second='test2')
    with pytest.raises(IntegrityError):
        db.insert_relationship(first='test3', second='test2')


def simple_relationship_query_test(db):
    parent1 = db.insert(name='parent1')
    parent2 = db.insert(name='parent2')
    parent3 = db.insert(name='parent3')
    child1 = db.insert(name='child1')
    child2 = db.insert(name='child2')
    db.insert_relationship(first=parent1, second=child1)
    db.insert_relationship(first=parent1, second=child2, type_='special')
    db.insert_relationship(first=parent2, second=child2)

    res = db.get(rel_first=parent1)
    assert set([x.name for x in res]) == set(['child1', 'child2'])
    
    res = db.get(rel_first=parent1,
                 rel_type='special')
    assert set([x.name for x in res]) == set(['child2'])
    
    res = db.get(rel_first=parent2)
    assert set([x.name for x in res]) == set(['child2'])
    
    res = db.get(rel_first=parent3)
    assert set([x.name for x in res]) == set([])
    
    res = db.get(rel_second=child2)
    assert set([x.name for x in res]) == set(['parent1', 'parent2'])
    
    res = db.get(rel_second=child1)
    assert set([x.name for x in res]) == set(['parent1'])


def generic_s3_setup(bucket_name='test_bucket'):
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket_name)


def test_simple_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_test(db)


def test_simple_namespace_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_test(db)


def test_simple_noname_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_noname_test(db)


def test_simple_noname_namespace_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_noname_test(db)


def test_simple_type_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_type_test(db)


def test_simple_type_namespace_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_type_test(db)


def test_simple_blobs_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_blob_test(db)


@mock_s3
def test_simple_blobs_sqlite_s3(tmp_path):
    generic_s3_setup('test_bucket')
    db = BasicDB(sql_string='sqlite:///:memory:', stash_s3_bucket='test_bucket')
    simple_blob_test(db)


def test_simple_blobs_namespace_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_blob_test(db)


def test_simple_relationships_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=stash_rootdir)
    simple_relationship_test(db)


def test_simple_relationships_namespace_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_relationship_test(db)


def test_simple_relationship_queries_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=stash_rootdir)
    simple_relationship_query_test(db)


def test_simple_relationship_queries_namespace_sqlite_fs(tmp_path):
    stash_rootdir = tmp_path / 'stash_rootdir'
    stash_rootdir.mkdir()
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_relationship_query_test(db)