import uuid

import msgpack
import pytest


def default(obj):
    if type(obj) is tuple:
        return msgpack.ExtType(10, pack_extra(list(obj)))
    else:
        raise TypeError(f'Unknown type: "{type(obj)}"')


def pack_extra(extra):
    return msgpack.packb(extra,
                         default=default,
                         strict_types=True,
                         use_bin_type=True)


def ext_hook(code, data):
    if code == 10:
        return tuple(unpack_extra(data))
    else:
        return msgpack.ExtType(code, data)


def unpack_extra(extra_bytes):
    return msgpack.unpackb(extra_bytes,
                           ext_hook=ext_hook,
                           strict_map_key=False)
