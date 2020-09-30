import datetime

from basicdb import __version__, BasicDB


def test_version():
    assert __version__ == '0.1.0'


def simple_test(db):
    db.insert(name='test1',
              json_data={'foo': 'bar'})
    res = db.get(name='test1')
    assert res.name == 'test1'
    assert res.json_data['foo'] == 'bar'
    assert res['foo'] == 'bar'
    assert res.namespace == db.namespace
    assert res.type_ is None
    assert not res.hidden
    assert res.binary_data is None
    assert res.username is not None
    assert (res.creation_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds() <= 1
    res = db.get()
    assert len(res) == 1
    assert res[0].name == 'test1'
    
    db.insert(name='test2',
              json_data={'key': 5},
              binary_data=b'0123',
              username='testuser')
    res = db.get(name='test1')
    assert res.name == 'test1'
    assert res.json_data['foo'] == 'bar'
    res = db.get()
    assert len(res) == 2
    assert set([x.name for x in res]) == set(['test1', 'test2'])
    res = db.get(names=['test2'])
    assert len(res) == 1
    res = res[0]
    assert res.name == 'test2'
    assert res['key'] == 5
    assert res.binary_data == b'0123'
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
    assert res.binary_data == b'0123'
    assert res.username == 'testuser'


def simple_blob_test(db):
    blob_data = [2, 3, 5, 7, 11]
    db.insert(name='test1', blob=blob_data)
    loaded_blob = db.load_blob('test1')
    assert loaded_blob == blob_data

    test_obj2 = db.insert(name='test2')
    blob_data = {'a': 0,
                 'b': b'12345',
                 'c': 100}
    for k, v in blob_data.items():
        db.insert_blob('test2', name=k, data=v)
    blobs = db.get_blobs('test2')
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

    db.delete_blob(db.get_blobs('test2')['b'])
    del blob_data['b']
    assert len(blob_data) == 2
    blobs = db.get_blobs('test2')
    assert set(blobs.keys()) == set(blob_data.keys())
    for k, v in blob_data.items():
        assert db.load_blob('test2', k) == v


def simple_relationship_test(db):
    test1 = db.insert(name='test1')
    test2 = db.insert(name='test2')
    test3 = db.insert(name='test3')
    db.insert_relationship(first='test1', second='test2', type_='child')
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


def test_simple_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_test(db)


def test_simple_namespace_sqlite_fs(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:',
                 stash_rootdir=tmp_path,
                 namespace='test_namespace')
    simple_test(db)


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