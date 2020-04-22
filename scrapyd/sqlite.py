import sqlite3
import json
try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping
import six

from bson import json_util

from ._deprecate import deprecate_class


class LogStatsSqliteData(object):

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (id integer primary key autoincrement, \
                                            spider text not null, \
                                            create_time integer not null, \
                                            log_count integer not null, \
                                            items integer not null, \
                                            pages integer not null \
                                            ); " % table
        q += "create index if not exists create_time_index on %s (create_time); " % table
        q += "create index if not exists spider_index on %s (spider); " % table
        # q += "create unique index create_time_spider_index on %s (create_time, spider) " %table

        self.conn.execute(q)
        self.insert_log_q = "insert into %s (spider, create_time, log_count, items, pages) \
                                        values (?, ?, ?, ?, ?); "%table

    def insert_log(self, create_time, spider, log_count, pages, items):
        self.conn.execute(q, (spider, create_time, log_count, items, pages))
        self.conn.commit()

    def get_all_stats(self, st, et):
        pass
    def get_stats(self, st, et, spider):
        pass

json_options = json_util.JSONOptions(tz_aware=False,
                                datetime_representation=json_util.DatetimeRepresentation.ISO8601)


def encode(obj, json_options=json_options):
    return sqlite3.Binary(json_util.dumps(obj, json_options=json_options).encode('ascii'))

def decode(obj, json_options=json_options):
    return json_util.loads(bytes(obj).decode('ascii'), json_options=json_options)


class JsonSqliteList(object):
    """SQLite-backed list"""

    def __init__(self, database=None, table="list"):
        self.database = database or ':memory:'
        self.table = table
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (key blob primary key, value blob)" \
            % table
        self.conn.execute(q)

    def __getitem__(self, key):
        if isinstance(key, slice):
            q = "select value from %s where key>=? and key<?" % self.table
            start = self.encode(key.start or 0)
            stop = self.encode(key.stop or 0)
            values = self.conn.execute(q, (start, stop)).fetchall()
            if values:
                return [self.decode(v[0]) for v in values]
        else:
            key = self.encode(key)
            q = "select value from %s where key=?" % self.table
            value = self.conn.execute(q, (key,)).fetchone()
            if value:
                return self.decode(value[0])
        raise IndexError(key)

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __delitem__(self, key):
        if isinstance(key, slice):
            q = "delete from %s where key>=? and key<?" % self.table
            start = self.encode(key.start or 0)
            stop = self.encode(key.stop or 0)
            self.conn.execute(q, (start, stop))
            self.conn.commit()
        else:
            key = self.encode(key)
            q = "delete from %s where key=?" % self.table
            self.conn.execute(q, (key,))
            self.conn.commit()

    def __iter__(self):
        q = "select value from %s" % self.table
        return (self.decode(x[0]) for x in self.conn.execute(q))

    def append(self, obj):
        q = "select count(*) from %s" % self.table
        key = self.conn.execute(q).fetchone()[0]
        key, value = self.encode(key), self.encode(obj)
        q = "insert or replace into %s (key, value) values (?,?)" % self.table
        self.conn.execute(q, (key, value))
        self.conn.commit()

    def encode(self, obj):
        return encode(obj)

    def decode(self, obj):
        return decode(obj)


class JsonSqliteDict(MutableMapping):
    """SQLite-backed dictionary"""

    def __init__(self, database=None, table="dict"):
        self.database = database or ':memory:'
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (key blob primary key, value blob)" \
            % table
        self.conn.execute(q)

    def __getitem__(self, key):
        key = self.encode(key)
        q = "select value from %s where key=?" % self.table
        value = self.conn.execute(q, (key,)).fetchone()
        if value:
            return self.decode(value[0])
        raise KeyError(key)

    def __setitem__(self, key, value):
        key, value = self.encode(key), self.encode(value)
        q = "insert or replace into %s (key, value) values (?,?)" % self.table
        self.conn.execute(q, (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        key = self.encode(key)
        q = "delete from %s where key=?" % self.table
        self.conn.execute(q, (key,))
        self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __iter__(self):
        for k in self.iterkeys():
            yield k

    def iterkeys(self):
        q = "select key from %s" % self.table
        return (self.decode(x[0]) for x in self.conn.execute(q))

    def keys(self):
        return list(self.iterkeys())

    def itervalues(self):
        q = "select value from %s" % self.table
        return (self.decode(x[0]) for x in self.conn.execute(q))

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        q = "select key, value from %s" % self.table
        return ((self.decode(x[0]), self.decode(x[1])) for x in self.conn.execute(q))

    def items(self):
        return list(self.iteritems())

    def encode(self, obj):
        return encode(obj)
        #return sqlite3.Binary(json.dumps(obj).encode('ascii'))

    def decode(self, obj):
        return decode(obj)
        # return json.loads(bytes(obj).decode('ascii'))


class JsonSqlitePriorityQueue(object):
    """SQLite priority queue. It relies on SQLite concurrency support for
    providing atomic inter-process operations.
    """

    def __init__(self, database=None, table="queue"):
        self.database = database or ':memory:'
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = "create table if not exists %s (id integer primary key, " \
            "priority real key, message blob)" % table
        self.conn.execute(q)

    def put(self, message, priority=0.0):
        args = (priority, self.encode(message))
        q = "insert into %s (priority, message) values (?,?)" % self.table
        self.conn.execute(q, args)
        self.conn.commit()

    def pop(self):
        q = "select id, message from %s order by priority desc limit 1" \
            % self.table
        idmsg = self.conn.execute(q).fetchone()
        if idmsg is None:
            return
        id, msg = idmsg
        q = "delete from %s where id=?" % self.table
        c = self.conn.execute(q, (id,))
        if not c.rowcount: # record vanished, so let's try again
            self.conn.rollback()
            return self.pop()
        self.conn.commit()
        return self.decode(msg)

    def remove(self, func):
        q = "select id, message from %s" % self.table
        n = 0
        for id, msg in self.conn.execute(q):
            if func(self.decode(msg)):
                q = "delete from %s where id=?" % self.table
                c = self.conn.execute(q, (id,))
                if not c.rowcount: # record vanished, so let's try again
                    self.conn.rollback()
                    return self.remove(func)
                n += 1
        self.conn.commit()
        return n

    def clear(self):
        self.conn.execute("delete from %s" % self.table)
        self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __iter__(self):
        q = "select message, priority from %s order by priority desc" % \
            self.table
        return ((self.decode(x), y) for x, y in self.conn.execute(q))

    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode('ascii'))

    def decode(self, text):
        return json.loads(bytes(text).decode('ascii'))
