# -*- coding: utf-8 -*-
"""
codac.cache
==========
@source: http://flask.pocoo.org/snippets/87/
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import os, sqlite3,sys
from time import time
try:
    from cPickle import loads, dumps
except:
    from pickle import loads, dumps
if sys.version_info.major==3:
    buffer = memoryview

class cache():

    _create_sql = (
            'CREATE TABLE IF NOT EXISTS bucket '
            '('
            '  key TEXT PRIMARY KEY,'
            '  val BLOB,'
            '  exp FLOAT'
            ')'
            )
    _get_sql = 'SELECT val, exp FROM bucket WHERE key = ?'
    _del_sql = 'DELETE FROM bucket WHERE key = ?'
    _set_sql = 'REPLACE INTO bucket (key, val, exp) VALUES (?, ?, ?)'
    _add_sql = 'INSERT INTO bucket (key, val, exp) VALUES (?, ?, ?)'
    _lst_sql = 'SELECT key, exp FROM bucket'

    def __init__(self, path, default_timeout=3600):
        self.path = os.path.abspath(path)
        self.default_timeout = default_timeout
        self.connection_cache = None

    def _get_conn(self):
        if self.connection_cache is None:
            import os
            isnew = ~os.path.isfile(self.path)
            conn = sqlite3.Connection(self.path, timeout=60)
            if isnew:
                with conn:
                    conn.execute(self._create_sql)
                try:
                    os.chmod(self.path, 666)
                except:
                    pass
            self.connection_cache = conn
        return self.connection_cache

    def get(self, key):
        import MDSplus
        rv = None
        with self._get_conn() as conn:
            for row in conn.execute(self._get_sql, (key,)):
                expire = row[1]
                if expire > time():
                    rv = MDSplus.Data.deserialize(loads(str(row[0])))
                    print('read from cache: '+key)
                break
        return rv

    def delete(self, key):
        with self._get_conn() as conn:
            conn.execute(self._del_sql, (key,))

    def set(self, key, value, timeout=None):
        import MDSplus
        if isinstance(value, (MDSplus.compound.Signal)):
            return(self.set(key, value.serialize().data(), timeout))
        if not timeout:
            timeout = self.default_timeout
        value = buffer(dumps(value, 2))
        expire = time() + timeout
        with self._get_conn() as conn:
            conn.execute(self._set_sql, (key, value, expire))
        self.clean()

    def add(self, key, value, timeout=None):
        if not timeout:
            timeout = self.default_timeout
        expire = time() + timeout
        value = buffer(dumps(value, 2))
        with self._get_conn() as conn:
            try:
                conn.execute(self._add_sql, (key, value, expire))
            except sqlite3.IntegrityError:
                pass

    def clean(self):
        with self._get_conn() as conn:
            for row in conn.execute(self._lst_sql):
                expire = row[1]
                if expire < time():
                    self.delete(str(row[0]))

    def clear(self):
        os.unlink(self.path)
        self.connection_cache = None