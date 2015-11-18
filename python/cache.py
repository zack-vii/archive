"""
archive.cache
==========
@source: http://flask.pocoo.org/snippets/87/
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import os as _os
import sqlite3 as _sql
from time import time
from . import version as _ver
from . import base as _base

if _ver.has_buffer:
    _unpack = lambda x: _ver.pickle.loads(str(x))#_mds.Data.deserialize(_ver.pickle.loads(str(x)))
else:
    _unpack = lambda x: _ver.pickle.loads(x)#_mds.Data.deserialize(_ver.pickle.loads(x))
_pack = lambda x: _ver.buffer(_ver.pickle.dumps(x))
_filepath = _ver.tmpdir+'archive_cache'+str(_ver.pyver[0])

class cache():
    _new_dat = (
            'CREATE TABLE IF NOT EXISTS data'
            '('
            '  hsh INT,'
            '  chn SMALLINT,'
            '  frm BIGINT,'
            '  upt BIGINT,'
            '  dat BLOB,'
            '  exp FLOAT,'
            '  CONSTRAINT key PRIMARY KEY (hsh,chn,frm,upt)'
            ')'
            )
    _set_dat = 'REPLACE INTO data (hsh, chn, frm, upt, dat, exp) VALUES (?, ?, ?, ?, ?, ?)'
    _upd_exp = 'UPDATE data SET exp = ? WHERE hsh = ? and chn = ? and frm = ? and upt = ?'
    _get_dat = 'SELECT dat FROM data WHERE hsh = ? and chn = ? and frm = ? and upt = ?'
    _all_dat = 'SELECT dat, frm, upt FROM data WHERE hsh = ? and chn = ? and upt >= ? and frm <= ? ORDER BY frm ASC'
    _del_dat = 'DELETE FROM data WHERE hsh = ? and chn = ? and frm = ? and upt = ?'
    _cln_dat = 'DELETE FROM data WHERE exp < ?'

    def __init__(self, path=_filepath, default_timeout=3600):
        self.path = _os.path.abspath(path)
        self.default_timeout = default_timeout
        self.connection_cache = None

    def _get_conn(self):
        if self.connection_cache is None:
            isnew = ~_os.path.isfile(self.path)
            conn = _sql.Connection(self.path, timeout=60)
            if isnew:
                with conn:
                    conn.execute(self._new_dat)
                try:
                    _os.chmod(self.path, 0o666)  # -rw-rw-rw-
                except:
                    pass
            self.connection_cache = conn
        return self.connection_cache

    def gets(self, key):
        with self._get_conn() as conn:
            rv = conn.execute(self._all_dat, tuple(key)).fetchall()
            for i in _ver.xrange(len(rv)):
                rv[i] = [_unpack(rv[i][0]),rv[i][1],rv[i][2]]
        return rv

    def get(self, key):
        rv = None
        with self._get_conn() as conn:
            for row in conn.execute(self._get_dat, tuple(key)):
                rv = _unpack(row[0])
                break;
        return rv

    def delete(self, key):
        with self._get_conn() as conn:
            conn.execute(self._del_dat, tuple(key))
            conn.commit()

    def set(self, key, data, timeout=None):
        if not timeout:
            timeout = self.default_timeout
        data = _pack(data)
        expire = time() + timeout
        with self._get_conn() as conn:
            conn.execute(self._set_dat, tuple(key+[data, expire]))
            conn.commit()

    def update(self, key, timeout=None):
        if not timeout:
            timeout = self.default_timeout
        expire = time() + timeout
        with self._get_conn() as conn:
            conn.execute(self._upd_exp, tuple([expire]+key))
            conn.commit()

    def clean(self):
        with self._get_conn() as conn:
            conn.execute(self._cln_dat, (time(),))
            conn.commit()
        self.vacuum()

    def vacuum(self):
        if _os.path.getsize(self.path)>2<<29:
            with self._get_conn() as conn:
                conn.execute('VACUUM')
                conn.commit()

    def close(self):
        _os.unlink(self.path)
        self.connection_cache = None

def getkey(path, time, chk=True, **kwargs):
    if chk:
        time = _base.TimeInterval(time).ns[0:2]
        path = _base.Path(path)
    chn = kwargs.get('channel',-1)
    frm = time[0]
    upt = time[1]
    hsh = hash(path);
    return [hsh,chn,frm,upt]
