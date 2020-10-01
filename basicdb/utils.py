import uuid


def gen_short_uuid_string(cur_uuid):
    num = cur_uuid.int
    alphabet = '23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    res = []
    while num > 0:
        num, digit = divmod(num, len(alphabet))
        res.append(alphabet[digit])
    return ''.join(reversed(res))
