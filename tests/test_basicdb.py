import datetime

from basicdb import __version__, BasicDB


def test_version():
    assert __version__ == '0.1.0'


def simple_test(db):
    db.insert(name='test1', json_data={'foo': 'bar'})
    res = db.get(name='test1')
    assert res.name == 'test1'
    assert res.json_data['foo'] == 'bar'
    assert res['foo'] == 'bar'
    assert res.namespace is None
    assert res.type_ is None
    assert not res.hidden
    assert res.binary_data is None
    assert res.username is not None
    assert (res.creation_time - datetime.datetime.now(datetime.timezone.utc)).total_seconds() <= 1
    res = db.get()
    assert len(res) == 1
    assert res[0].name == 'test1'
    
    db.insert(name='test2', json_data={'key': 5}, binary_data=b'0123', username='testuser')
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


def test_fs_adapter(tmp_path):
    db = BasicDB(sql_string='sqlite:///:memory:', stash_rootdir=tmp_path)
    simple_test(db)