# -*- coding: utf-8 -*-

import hashlib


def hash(s):
    h = hashlib.md5(s.encode())
    return int(h.hexdigest(), base=16)
