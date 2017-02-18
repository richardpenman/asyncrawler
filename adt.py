# -*- coding: utf-8 -*-

from collections import defaultdict
import hashlib
import common



class HashDict:
    """For storing large quantities of keys where don't need the original value of the key
    Instead each key is hashed and hashes are compared for equality

    >>> hd = HashDict()
    >>> url = 'http://webscraping.com'
    >>> hd[url] = True
    >>> url in hd
    True
    >>> 'other url' in hd
    False
    >>> len(hd)
    1
    """
    def __init__(self, default_factory=str):
        self.d = defaultdict(default_factory)

    def __len__(self):
        """How many keys are stored in the HashDict
        """
        return self.d.__len__()

    def __contains__(self, name):
        return self.d.__contains__(self.to_hash(name))

    def __getitem__(self, name):
        return self.d.__getitem__(self.to_hash(name))

    def __setitem__(self, name, value):
        return self.d.__setitem__(self.to_hash(name), value)

    def get(self, name, default=None):
        """Get the value at this key

        Returns default if key does not exist
        """
        return self.d.get(self.to_hash(name), default)

    def to_hash(self, value):
        return common.hash(str(value))


class FakeDict:
    """Class with dict interface that does not store data

    >>> d = FakeDict()
    >>> d['a'] = 'b'
    >>> 'a' in d
    False
    >>> d.get(1, 2)
    2
    >>> len(d)
    0
    """
    def __len__(self):
        return 0

    def __contains__(self, _):
        return False

    def __getitem__(self, key):
        {}[key]

    def __setitem__(self, key, value):
        pass

    def get(self, key, default=None):
        return default
