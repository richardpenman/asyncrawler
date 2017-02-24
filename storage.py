# -*- coding: utf-8 -*-

import collections, os, datetime, time, sqlite3, zlib, pickle
from . import common



class PersistentDict:
    """
    PersistentDict has a dictionary like interface and a sqlite backend
    It uses pickle to store Python objects and strings, which are then compressed
    Multithreading is supported

    filename: 
        where to store sqlite database
    compress_level: 
        between 1-9 (in my test levels 1-3 produced a 1300kb file in ~7 seconds while 4-9 a 288kb file in ~9 seconds)
    expires: 
        a timedelta object of how old data can be before expires. By default is set to None to disable.
    timeout: 
        how long should a thread wait for sqlite to be ready (in ms)

    >>> cache = PersistentDict()
    >>> url = 'http://webscraping.com/blog'
    >>> html = '<html>abc</html>'
    >>>
    >>> url in cache
    False
    >>> len(cache)
    0
    >>> cache[url] = html
    >>> url in cache
    True
    >>> len(cache)
    1
    >>> cache[url] == html
    True
    >>> cache.get(url)['value'] == html
    True
    >>> cache.meta(url)
    {}
    >>> cache.meta(url, 'meta')
    >>> cache.meta(url)
    'meta'
    >>> del cache[url]
    >>> url in cache
    False
    >>> os.remove(cache.filename)
    """
    def __init__(self, filename, compress_level=6, expires=None, timeout=10000, max_operations=1000):
        """initialize a new PersistentDict with the specified database file.
        """
        self.filename = filename
        self.compress_level, self.expires, self.timeout = compress_level, expires, timeout
        self.conn = sqlite3.connect(filename, timeout=timeout, isolation_level='DEFERRED', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES, check_same_thread=False)
        self.conn.text_factory = lambda x: unicode(x, 'utf-8', 'replace')
        sql = """
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT NOT NULL PRIMARY KEY UNIQUE,
            value BLOB,
            updated timestamp DEFAULT (datetime('now', 'localtime'))
        );
        """
        self.conn.execute(sql)
        self.operations = 0
        self.max_operations = max_operations


    def __del__(self):
        self.conn.commit()


    def __contains__(self, key):
        """check the database to see if a key exists
        """
        row = self.conn.execute("SELECT updated FROM cache WHERE key=?;", (key,)).fetchone()
        return row and self.is_fresh(row[0])


    def __iter__(self):
        """iterate each key in the database
        """
        c = self.conn.cursor()
        c.execute("SELECT key FROM cache;")
        for row in c:
            yield row[0]

    
    def __nonzero__(self):
        return True


    def __len__(self):
        """Return the number of entries in the cache
        """
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM cache;")
        return c.fetchone()[0]


    def __getitem__(self, key):
        """return the value of the specified key or raise KeyError if not found
        """
        row = self.conn.execute("SELECT value, updated FROM cache WHERE key=?;", (key,)).fetchone()
        if row:
            if self.is_fresh(row[1]):
                value = row[0]
                return self.deserialize(value)
            else:
                raise KeyError("Key `%s' is stale" % key)
        else:
            raise KeyError("Key `%s' does not exist" % key)


    def __delitem__(self, key):
        """remove the specifed value from the database
        """
        self.conn.execute("DELETE FROM cache WHERE key=?;", (key,))
        self.commit()


    def __setitem__(self, key, value):
        """set the value of the specified key
        """
        updated = datetime.datetime.now()
        self.conn.execute("INSERT OR REPLACE INTO cache (key, value, updated) VALUES(?, ?, ?);", (
            key, self.serialize(value), updated)
        )
        self.commit()


    def commit(self):
        self.operations += 1
        if self.operations % self.max_operations == 0:
            self.conn.commit()


    def serialize(self, value):
        """convert object to a compressed pickled string to save in the db
        """
        return sqlite3.Binary(zlib.compress(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), self.compress_level))
    
    def deserialize(self, value):
        """convert compressed pickled string from database back into an object
        """
        if value:
            return pickle.loads(zlib.decompress(value))


    def is_fresh(self, t):
        """returns whether this datetime has expired
        """
        return self.expires is None or datetime.datetime.now() - t < self.expires


    def clear(self):
        """Clear all cached data
        """
        self.conn.execute("DELETE FROM cache;")


    def vacuum(self):
        self.conn.execute('VACUUM')



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
        self.d = collections.defaultdict(default_factory)

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
