from collections import namedtuple
import dataclasses

import pytest

import basicdb
from basicdb import pack_extra, unpack_extra


def identity(x):
    return unpack_extra(pack_extra(x))


def test_packing_simple():
    inputs = [0,
              1,
              3.14,
              -1,
              (0,),
              [0,],
              {0: '0', '0':0},
              [2, 3, '4'],
              b'531',
              None,
              [None, None],
              {'foo': 'bar',
               'foo2': 5,
               'ttt': 4.2,
               'xyz': (4, 3),
               'zzz': [0, -1.0]},
              (0, 1, [2, 3, (-1, -5)])]
    for inp in inputs:
        assert inp == identity(inp)


def test_tuple():
    inp = (0, 2, 3)
    out = identity(inp)
    assert type(out) is tuple
    assert len(out) == 3
    assert inp[0] == out[0] and inp[1] == out[1] and inp[2] == out[2]


def test_list():
    inp = [0, 2, 3]
    out = identity(inp)
    assert type(out) is list
    assert len(out) == 3
    assert inp[0] == out[0] and inp[1] == out[1] and inp[2] == out[2]


@dataclasses.dataclass
class Tmp:
    a: int
    b: float


def test_dataclass():
    inp = Tmp(a=0, b=1.0)
    with pytest.raises(TypeError):
        out = identity(inp)
        assert inp == out


def test_namedtuple():
    Tmp2 = namedtuple('Tmp2', ['x', 'y'])
    inp = Tmp2(x=0, y=1)
    with pytest.raises(TypeError):
        out = identity(inp)
        assert inp == out