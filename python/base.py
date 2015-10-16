"""
codac.baseclasses
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus as _mds
import numpy as _np
import os as _os
import re as _re
import time as _time
from . import version as _ver
_defreadpath = ('raw/W7X/MDSplus')
_rooturl = 'http://archive-webapi.ipp-hgw.mpg.de'
_database = 'Test'
_server = 'mds-data-1.ipp-hgw.mpg.de'


class InsufficientPathException(Exception):
    def __init__(self, value=''):
        self.value = 'insufficient path information'


class Path(object):
    _ROOTURL = _rooturl

    def __init__(self, path=_defreadpath, *_argin):
        if isinstance(path, (Path)):
            self._path = path._path
            self._lev = path._lev
        else:
            self.set_path(str(path))

    def __str__(self):
        return self.path()
    __repr__ = __str__

    def ping(self, timeout=5):
        timeout = max(1, int(timeout))
        hostname = self._ROOTURL.split('//')[-1]
        if _ver.isposix:
            status = _os.system("/bin/ping -c 1 -i %d %s >/dev/null" % (timeout, hostname))
        elif _ver.isnt:
            status = _os.system("%%WINDIR%%\System32\PING.EXE /n 1 /w %d %s >NUL" % (timeout, hostname))
        return status==0

    def set_path(self, *path):
        [self._path, self._lev] = self.path(-2, '/'.join(path))

    def path(self, lev=-1, _path=None):
        if _path is None:
            _path = self._path
        if not (_path[0] == '/'):
            if (_path[0:7].lower() == "http://"):
                _path = '/'.join(_path[7:].split('/')[1:])
            else:
                _path = _database + '/' + _path
        _path = _path.strip('/').split('/')
        if len(_path) > 4:
            if not (_path[4].endswith('_DATASTREAM') or
                    _path[4].endswith('_PARLOG') or
                    _path[4].endswith('_CFGLOG')):
                _path[4] = _path[4] + '_DATASTREAM'
        if lev == -2:
            return ['/' + '/'.join(_path), len(_path)]
        if lev == -1:
            return '/' + '/'.join(_path)
        else:
            _path = _path[0:lev]
            if len(_path) < lev:
                raise InsufficientPathException
        return '/'+'/'.join(_path)

    def url(self, lev=-1, *arg):
        return _url_parms(self._ROOTURL+self.path(lev), *arg)

    # set
    def _set_database(self, database):
        self.set_path(database)
        return self

    def _set_view(self, view):
        if type(view) is int:
            if view <= 0:
                view = 'raw'
            elif view == 1:
                view = 'cocking'
            else:
                view = 'cocked'
        self.set_path(self.path(1), view)
        return self

    def _set_project(self, project):
        if type(project) is int:
            project = self.list_projects()[project]
        self.set_path(self.path(2), project)
        return self

    def _set_streamgroup(self, streamgroup):
        if type(streamgroup) is int:
            streamgroup = self.list_streamgroups()[streamgroup]
        self.set_path(self.path(3), streamgroup)
        return self

    def _set_stream(self, stream):
        if type(stream) is int:
            stream = self.list_streams()[stream]
        self.set_path(self.path(4), stream)
        return self

    def _set_channel(self, channel, index=0):
        self.set_path(self.path(5), str(index), channel)
        return self

    # get
    def _get_database(self):
        return self.path_database().split('/')[-1]

    def _get_view(self):
        return self.path_view().split('/')[-1]

    def _get_project(self):
        return self.path_project().split('/')[-1]

    def _get_streamgroup(self):
        return self.path_streamgroup().split('/')[-1]

    def _get_stream(self):
        return self.path_datastream().split('/')[-1][:-11]

    def _get_channel(self):
        return self.path_channel().split('/')[-1]

    database = property(_get_database, _set_database)
    view = property(_get_view, _set_view)
    project = property(_get_project, _set_project)
    streamgroup = property(_get_streamgroup, _set_streamgroup)
    stream = property(_get_stream, _set_stream)
    channel = property(_get_channel, _set_channel)

    # get path
    def path_database(self):
        if self._lev < 1:
            raise InsufficientPathException
        return self.path(1)

    def path_view(self):
        if self._lev < 2:
            raise InsufficientPathException
        return self.path(2)

    def path_project(self):
        if self._lev < 3:
            raise InsufficientPathException
        return self.path(3)

    def path_streamgroup(self):
        if self._lev < 4:
            raise InsufficientPathException
        return self.path(4)

    def path_cfglog(self):
        if self._lev < 4:
            raise InsufficientPathException
        streamgroup = self.path(4)
        return streamgroup + '/' + streamgroup.split('/')[-1] + '_CFGLOG'

    def path_datastream(self):
        if self._lev < 5:
            raise InsufficientPathException
        return self.path(5)

    def path_parlog(self):
        if self._lev < 5:
            raise InsufficientPathException
        return self.path(5)[:-11]+'_PARLOG'

    def path_channel(self):
        if self._lev < 7:
            raise InsufficientPathException
        return self.path(7)

    # get url
    def url_database(self):
        return self._ROOTURL + self.path_database()

    def url_view(self):
        return self._ROOTURL + self.path_view()

    def url_project(self):
        return self._ROOTURL + self.path_project()

    def url_streamgroup(self):
        return self._ROOTURL + self.path_streamgroup()

    def url_datastream(self, *arg):
        return _url_parms(self._ROOTURL + self.path_datastream(), *arg)

    def url_parlog(self, *arg):
        return _url_parms(self._ROOTURL + self.path_parlog(), *arg)

    def url_cfglog(self, *arg):
        return _url_parms(self._ROOTURL + self.path_cfglog(), *arg)

    def url_channel(self, *arg):
        return _url_parms(self._ROOTURL + self.path_channel(), *arg)

    def url_data(self, *arg):
        if self._lev > 6:
            return self.url_channel(*arg)
        else:
            return self.url_datastream(*arg)

    cfglog = property(url_cfglog)
    parlog = property(url_parlog)


def _url_parms(url, time=None, skip=0, nsamples=0, channels=[]):
    if time is not None:
        time = TimeInterval(time)
        url = url + '/_signal.json'
        par = [str(time)]
        if skip > 0:
            par.append('skip='+str(skip))
        if nsamples > 0:
            par.append('nSamples='+str(nsamples))
        if len(channels):
            par.append('channels='+','.join(map(str, channels)))
        if len(par):
            url = url+'?'+'&'.join(par)
    return url


class Time(_ver.long):
    """
    Time([<ns:long>, <s:float>,'now', 'now_m'])
    """
    _s2ns = 1000000000
    def __new__(self, time='now', local=False):
        if isinstance(time, Time):
            return time
        def listtovalue(time):
            time += [0]*(9-len(time))
            seconds = int(_time.mktime(tuple(time[0:6]+[0]*3)) -
                           _time.timezone)
            if local:
                seconds += int((_time.gmtime(seconds).tm_hour-_time.localtime(seconds).tm_hour)*3600)
            return super(Time, self).__new__(self,
                                            ((seconds*1000+time[6])*1000 +
                                            time[7])*1000+time[8])

        if isinstance(time, (_ver.basestring,)):
            if time.startswith('now'):  # now
                time = time.split('_')
                if len(time)<2 or time[1]=='ms':
                    return super(Time, self).__new__(self, int(_time.time()*1000)*1000000)
                if time[1]=='s':
                    return super(Time, self).__new__(self, int(_time.time())*self._s2ns)
                if time[1]=='m':
                    return super(Time, self).__new__(self, int(_time.time()/60)*60*self._s2ns)
                if time[1]=='h':
                    return super(Time, self).__new__(self, int(_time.time()/3600)*3600*self._s2ns)
            else:  # '2009-02-13T23:31:30.123456789Z'
                time = _re.findall('[0-9]+', time)
                if len(time) == 7:  # we have subsecond precision
                    time = time[0:6] + _re.findall('[0-9]{3}', time[6]+'00')
                time = [int(t) for t in time]
                return listtovalue(time)
        if isinstance(time, (_time.struct_time,)):
            return listtovalue(list(time)[0:6])
        if isinstance(time, (_mds.treenode.TreeNode, _mds.Scalar)):
            time = time.data()
        if isinstance(time, (_np.ScalarType)):
            if _np.array(time).dtype==float:
                time = time*self._s2ns
            return super(Time, self).__new__(self, time)
        return listtovalue(list(time[0:9]))

    def __add__(self, y):
        if isinstance(y, float):
            y = int(y*1E9)
        return Time(_ver.long.__add__(self, y))

    def __radd__(self, y):
        if isinstance(y, float):
            y = int(y*1E9)
        return Time(_ver.long.__radd__(self, y))

    def __sub__(self, y):
        if isinstance(y, float):
            y = int(y*1E9)
        return Time(_ver.long.__sub__(self, y))

    def __rsub__(self, y):
        if isinstance(y, float):
            y = int(y*1E9)
        return Time(_ver.long.__rsub__(self, y))

    def __repr__(self): return self.utc

    def _ns(self): return _ver.long(self)

    def _s(self): return self.ns * 1E-9

    def _subsec(self): return self.ns % self._s2ns

    def _utc(self):
        import time as _time
        values = tuple(list(_time.gmtime((self % (1 << 64))/1e9)[0:6]) +
                       [self.subsec])
        return '%04d-%02d-%02dT%02d:%02d:%02d.%09dZ' % values

    def _local(self):
        import time as _time
        return _time.ctime((self % (1 << 64)) / 1e9)
    ns = property(_ns)
    s = property(_s)
    utc = property(_utc)
    local = property(_local)
    subsec = property(_subsec)


class TimeInterval(list):
    """
    isinstance generic
    from <=  0 : upto -|X| ns
    from  >  0 : epoch +X ns
    upto <  -1 : now  -|X| ns
    upto == -1 : inf
    upto ==  0 : now
    upto  >  0 : epoch +X ns
    """

    def __new__(self, arg=True):
        if type(arg) is TimeInterval:
            return arg  # short cut
        return list.__new__(self)

    def __init__(self, arg=[-1800., 'now', -1]):
        if type(arg) is TimeInterval:
            return  # short cut
        if isinstance(arg, (_mds.Array, _mds.Ident, _mds.treenode.TreeNode, _mds.tdibuiltins.VECTOR)):
            arg = arg.data()
        if isinstance(arg, (_np.ndarray,)):
            arg = arg.tolist()
        if not isinstance(arg, (list, tuple)):
            arg = [-1, arg, 0]
        if len(arg) < 3:
            if len(arg) < 2:
                if len(arg) == 0:
                    arg = [-1800.]
                arg = list(map(Time,arg))
                arg += [0] if arg[0] < 0 else arg
            arg += [-1] if arg[0]<0 else [0]
        if arg[0]==0:
            arg[0] = 'now'
        if arg[1]==0:
            arg[1] = 'now'
        super(TimeInterval, self).append(Time(arg[0]))
        super(TimeInterval, self).append(Time(arg[1]))
        super(TimeInterval, self).append(Time(arg[2]))

    def append(self, time): self._setT0(time)

    def __setitem__(self, idx, time):
        super(TimeInterval, self).__setitem__(min(idx, 2), Time(time))

    def __getitem__(self, idx):
        idx = min(idx, 2)
        time = super(TimeInterval, self).__getitem__(idx)
        if idx == 0 and time < 0:
            return self[1] + time
        elif idx == 1 and (time == 0 or time < -2):
            return Time('now') + time
        elif idx == 2 and time < 0:
            if super(TimeInterval, self).__getitem__(idx) < 0:
                return self.uptoT
            return self.fromT
        else:
            return time

    def __str__(self):
        return 'from=' + str(max(0, self.fromT-1)) + '&upto=' + str(self.uptoT)

    def __repr__(self):
        return 'UTC: [ '+self.fromT.utc+' , '+self.uptoT.utc+' ; '+self.t0T.utc+' ]'

    def _setFrom(self, time): self[0] = Time(time)

    def _setUpto(self, time): self[1] = Time(time)

    def _setT0(self, time): self[2] = Time(time)

    def _getFrom(self): return self[0]

    def _getUpto(self): return self[1]

    def _getT0(self): return self[2]

    def _fromStr(self): return str(self.fromT)

    def _uptoStr(self): return str(self.uptoT)

    def _utc(self): return [self.fromT.utc, self.uptoT.utc]

    def _local(self): return [self.fromT.local, self.uptoT.local]

    def _s(self): return [self.fromT.s, self.uptoT.s]

    def _ns(self): return [self.fromT.ns, self.uptoT.ns]

    ns = property(_ns)
    s = property(_s)
    utc = property(_utc)
    local = property(_local)
    fromT = property(_getFrom, _setFrom)
    uptoT = property(_getUpto, _setUpto)
    t0T = property(_getT0, _setT0)
    fromStr = property(_fromStr)
    uptoStr = property(_uptoStr)

_units = ['unknown', '', 'none', 'arb.unit', 'kg', 'g', 'u', 'kg/s', 'g/s', 'm',
         'cm', 'mm', 'nm', 'Angstrom', 'm^2', 'm^3', 'L', 'm^-1', 'nm^-1',
         'm^-2', 'cm^-2', 'm^-3', 'cm^-3', 'm^3/s', 'm^3/h', 'L/s', 'L/min',
         'L/h', '(m.s)^-1', '(cm.s)^-1', 's', 'min', 'h', 'ms', 'us', 'ns',
         's^-1', 'Hz', 'kHz', 'MHz', 'GHz', 'Bq', 'A', 'kA', 'mA', 'uA', 'A/s',
         'C', 'K', 'oC', 'deg.C', 'rad', 'o', 'r/min', 'sr', 'count',
         'ustrain', '%', 'dB', 'm/s', 'km/s', 'km/h', 'N', 'Pa', 'hPa', 'bar',
         'mbar', 'ubar', 'bar.L', 'mbar.L', 'bar.L/s', 'mbar.L/s',
         'mbar.L/min', 'sccm', 'J', 'kJ', 'MJ', 'eV', 'keV', 'W', 'mW', 'kW',
         'MW', 'W/m^2', 'mW/cm^2', 'W/m^3', 'W/(m^3.sr)', 'W/(m^2.sr.nm)',
         'W/(cm^2.sr.nm)', 'VA', 'MVA', 'V', 'kV', 'mV', 'uV', 'V/s', 'V.s',
         'V^-1', 'Wb', 'Ohm', 'S', 'F', 'pF', 'H', 'T', 'Gs', 'T/A', 'T/s',
         'Gy', 'Sv', 'uSv', 'rem', 'urem', 'Sv/h', '1E20', '%Tm/MA', 'bit',
         'Byte', 'KiByte', 'MiByte']


def Units(units=None, force=False):
    if isinstance(units, (_mds.treenode.TreeNode)):
        if units.isSegmented():
            units = units.getSegment(0).units
        else:
            units = units.units
    elif isinstance(units, (_mds.compound.Signal, _mds.mdsarray.Array)):
        try:
            units = units.units
        except:
            return 'unknown'
    units = str(units)
    if units == ' ':
        return 'unknown'
    if units in _units:
        return units
    if force:
        print("'"+units+"' is not a recognized unit but has been enforced!")
        return units
    raise Exception("Units must be one of '"+"', '".join(_units)+"'")


def createSignal(dat, dim, unit=None, addim=[], units=[], help=None):
    if isinstance(dat, (_np.ndarray,)):
        dat = dat.tolist()

    def _dim(dim):
        if len(dim):
            wind = _mds.Uint64Array([dim[0]-1, dim[-1], 0])
            wind = _mds.Window(wind[0], wind[1], wind[2])
            dim = _mds.Uint64Array(dim)
            dim = _mds.Dimension(wind, dim)
            dim.setUnits('ns')
            return dim
        else:
            return _mds.EmptyData()

    def _addim(dim, units='unknown'):
        if len(dim):
            dim  = _mds.Dimension(None, _dat(dim))
            dim.setUnits(Units(units))#
            return dim
        else:
            return _mds.EmptyData()

    def _dat(dat):
        def _datr(dat, m=0, n=0):
            if len(dat) == 0 or (n > 1 and m+n > 64):
                return m, n
            if isinstance(dat[0], (list,)):
                for x in dat:  # recursive
                    m, n = _datr(x, m, n)
                return m, n
            else:
                n = n or any([x < 0 for x in dat])
                try:
                    m = max(map(int.bit_length, dat)+[m])
                except:
                    # print(error())
                    # if any([isinstance(x, (complex,)) for x in dat]):
                    # return 0, 3
                    # if any([isinstance(x, (float,)) for x in dat]):
                    return 0, 2
                return m, n
        if len(dat) == 0:
            return _mds.EmptyData()
        m, n = _datr(dat)
        if n == 3:
            return _mds.Complex128Array(dat)
        if n == 2:
            return _mds.Float64Array(dat)
        if n:
            if m+1 > 64:
                return _mds.Int128Array(dat)
            elif m+1 > 32:
                return _mds.Int64Array(dat)
            elif m+1 > 16:
                return _mds.Int32Array(dat)
            elif m+1 > 8:
                return _mds.Int16Array(dat)
            else:
                return _mds.Int8Array(dat)
        else:
            if m > 64:
                return _mds.Uint128Array(dat)
            elif m > 32:
                return _mds.Uint64Array(dat)
            elif m > 16:
                return _mds.Uint32Array(dat)
            elif m > 8:
                return _mds.Uint16Array(dat)
            elif m > 0:
                return _mds.Uint8Array(dat)
                # else:
                # return MDSplus.BoolArray(dat==1)
            else:
                return _mds.EmptyData()
    dat = _dat(dat)
    dim = _dim(dim)
    for i in _ver.xrange(len(addim)):
        addim[i] = _addim(addim[i], units[i])
    raw = _mds.Data.compile('*')
    if unit is not None:
        dat.setUnits(unit)
    sig = _mds.Signal(dat, raw, dim, *addim)
    if help:
        sig.setHelp(help)
    return sig
