"""
codac.baseclasses
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from . import version as _ver
if _ver.has_mds:
    import MDSplus as _mds
    import MDSplus.tdibuiltins as _tdi
import numpy as _np
import os as _os
import re as _re
import time as _time
_defreadpath = 'codac/W7X/MDSplus'
_rooturl = 'http://archive-webapi.ipp-hgw.mpg.de'
_database = 'ArchiveDB'
_server = 'mds-data-1.ipp-hgw.mpg.de'


class InsufficientPathException(Exception):
    def __init__(self, value=''):
        self.value = 'insufficient path information'


class Path(object):
    _ROOTURL = _rooturl
    _CFGLOG =  '_CFGLOG'
    _PARLOG =  '_PARLOG'
    _DATASTREAM =  '_DATASTREAM'
    def __new__(cls, path=_defreadpath):
        if type(path) is Path:
            return path
        else:
            newpath = super(Path,cls).__new__(cls)
            newpath.isDataStream = False
            newpath.isParLog = False
            newpath.isCfgLog = False
            newpath.set_path(str(path))
            return newpath

    def __str__(self):
        return self.path(-1)
    __repr__ = __str__

    def __hash__(self):
        return hash(self.path_datastream()[:-11])

    def ping(self, timeout=5):
        timeout = max(1, int(timeout))
        hostname = self._ROOTURL.split('//')[-1]
        if _ver.isposix:
            status = _os.system("/bin/ping -c 1 -i %d %s >/dev/null" % (timeout, hostname))
        elif _ver.isnt:
            status = _os.system("%%WINDIR%%\System32\PING.EXE /n 1 /w %d %s >NUL" % (timeout, hostname))
        return status==0

    def append(self,item):
        self.set_path(self.path(),item)
        return self

    def copy(self):
        cls = type(self)
        newpath = super(Path,cls).__new__(cls)
        newpath.isDataStream = self.isDataStream
        newpath.isParLog = self.isParLog
        newpath.isCfgLog = self.isCfgLog
        newpath._path = list(self._path)
        newpath._lev = self._lev
        return newpath

    def set_path(self, *path):
        [self._path, self._lev] = self.path(-2, '/'.join(path))
        return self

    def path(self, lev=-1, _path=None):
        if _path is None:
            _path = self._path
        else:
            if not (_path[0] == '/'):
                if (_path[0:7].lower() == "http://"):
                    _path = '/'.join(_path[7:].split('/')[1:])
                else:
                    _path = _database + '/' + _path
            _path = _path.strip('/').split('/')
        if lev == -2:
            if len(_path) > 4:
                if _path[4].endswith(Path._DATASTREAM):
                    self.isDataStream = True
                    _path[4] = _path[4][0:-11]
                elif _path[4].endswith(Path._PARLOG):
                    self.isParLog = True
                    _path[4] = _path[4][0:-7]
                elif _path[4].endswith(Path._CFGLOG):
                    self.isCfgLog = True
                    _path[4] = _path[4][0:-7]
            return [_path, len(_path)]
        if len(_path) > 4:
            if self.isDataStream: _path[4]+Path._DATASTREAM
            elif self.isParLog:   _path[4]+Path._PARLOG
            elif self.isCfgLog:   _path[4]+Path._CFGLOG
        if lev == -1:
            return '/' + '/'.join(_path)
        else:
            _path = _path[0:lev]
            if len(_path) < lev:
                raise InsufficientPathException
        return '/'+'/'.join(_path)

    def url(self, lev=-1, **kwargs):
        return parms(self._ROOTURL+self.path(lev), **kwargs)

    # set
    def _set_database(self, database):
        self.set_path(database)
        return self

    def _set_view(self, view):
        if isinstance(view, int):
            if view <= 0:
                view = 'raw'
            elif view == 1:
                view = 'cocking'
            else:
                view = 'cocked'
        self.set_path(self.path(1), view)
        return self

    def _set_project(self, project):
        self.set_path(self.path(2), project)
        return self

    def _set_streamgroup(self, streamgroup):
        self.set_path(self.path(3), streamgroup)
        return self

    def _set_stream(self, stream):
        self.set_path(self.path(4), stream)
        return self

    def _set_channel(self, channel, name):
        if name is None:
            self.set_path(self.path(5), str(channel))
        else:
            self.set_path(self.path(5), str(channel), name)
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
        return streamgroup + '/' + streamgroup.split('/')[-1] + Path._CFGLOG

    def path_datastream(self, **kwargs):
        if self._lev < 5:
            raise InsufficientPathException
        return buildpath(self.path(5)+Path._DATASTREAM, **kwargs)

    def path_parlog(self):
        if self._lev < 5:
            raise InsufficientPathException
        return self.path(5)+Path._PARLOG

    def path_channel(self, **kwargs):
        if self._lev < 6:
            raise InsufficientPathException
        return buildpath(self.path(6), **kwargs)

    def path_data(self, **kwargs):
        if self._lev > 5:
            return self.path_channel(**kwargs)
        else:
            return self.path_datastream(**kwargs)

    # get url
    def url_database(self):
        return self._ROOTURL + self.path_database()

    def url_view(self):
        return self._ROOTURL + self.path_view()

    def url_project(self):
        return self._ROOTURL + self.path_project()

    def url_streamgroup(self):
        return self._ROOTURL + self.path_streamgroup()

    def url_datastream(self, **kwargs):
        return parms(self._ROOTURL + self.path_datastream(), **kwargs)

    def url_parlog(self, **kwargs):
        return parms(self._ROOTURL + self.path_parlog(), **kwargs)

    def url_cfglog(self, **kwargs):
        return parms(self._ROOTURL + self.path_cfglog(), **kwargs)

    def url_channel(self, **kwargs):
        if 'channel' in kwargs.keys():
            return self.url_datastream(**kwargs)
        return parms(self._ROOTURL + self.path_channel(), **kwargs)

    def url_data(self, **kwargs):
        if self._lev > 5:
            return self.url_channel(**kwargs)
        else:
            return self.url_datastream(**kwargs)

    cfglog = property(url_cfglog)
    parlog = property(url_parlog)

def checkPathElemnent(element):
    pattern = '[A-Za-z][A-Za-z0-9._-]*'
    if not _re.match(pattern,element):
        raise Exception('%s does not match %s!' %(element,pattern) )


def parms(url, **kwargs):
    if 'time' in kwargs.keys():
        time = TimeInterval(kwargs['time'])
        url = buildpath(url, **kwargs)
        url = url + '/_signal.json'
        par = [str(time)]
        if 'skip' in kwargs.keys():
            par.append('skip='+str(int(kwargs['skip'])))
        if 'nsamples' in kwargs.keys():
            par.append('nSamples='+str(int(kwargs['nsamples'])))
#        if 'channels' in kwargs.keys():
#            par.append('channels='+str(kwargs['channels']).lstrip('[').rstrip(']').replace(' ',''))
        url = url+'?'+'&'.join(par)
    else:
        par = []
        if 'filterstart' in kwargs.keys():
            par.append('filterstart='+str(int(kwargs['filterstart'])))
        if 'filterstop' in kwargs.keys():
            par.append('filterstop='+str(int(kwargs['filterstop'])))
        if par:
            url = url+'?'+'&'.join(par)
    return url

def buildpath(path, **kwargs):
    if 'channel' in kwargs.keys():
        path = path + '/' + str(int(kwargs['channel']))
    if 'scaled' in kwargs.keys():
        path = path + '/' + str(kwargs['scaled'])
    return path

def filter(path, time=None):
    url = Path(path).url_datastream() + '/_signal.json'
    if time is None:
        return url
    return url+'?'+TimeInterval(time).filter()

class Time(_ver.long):
    """
    Time([<ns:long>, <s:float>,'now', 'now_m'],['ns','us','ms','s','m','h','d'])
    """
    _d2ns = 86400000000000
    _h2ns = 3600000000000
    _m2ns = 60000000000
    _s2ns = 1000000000
    _ms2ns = 1000000
    _us2ns = 1000
    def __new__(cls, time='now', units=None, local=False):
        if type(time) is Time:
            return time
        if _ver.has_mds:
            if isinstance(time, (_mds.treenode.TreeNode)):
                return Time(time.evaluate(),units,local)
            if isinstance(time, (list,_np.ndarray)):
                return [Time(e,units,local) for e in time]
        if isinstance(time, (_mds.mdsarray.Array, _mds.Dimension)):
            if units is None:
                units = time.units
            return Time(time.data(),units,local)
        if isinstance(time, (tuple)):
            return tuple(Time(e,units,local) for e in time)
        def listtovalue(time):
            time += [0]*(9-len(time))
            seconds = int(_time.mktime(tuple(time[0:6]+[0]*3)) -
                           _time.timezone)
            if local:
                seconds += int((_time.gmtime(seconds).tm_hour-_time.localtime(seconds).tm_hour)*3600)
            return super(Time, cls).__new__(cls,
                                            ((seconds*1000+time[6])*1000 +
                                            time[7])*1000+time[8])
        if isinstance(time, (_ver.basestring,)):
            if time.startswith('now'):  # now
                time = time.split('_')
                if len(time)<2 or time[1]=='ms':
                    return super(Time, cls).__new__(cls, int(_time.time()*1000)*Time._ms2ns)
                if time[1]=='s':
                    return super(Time, cls).__new__(cls, int(_time.time())*Time._s2ns)
                if time[1]=='m':
                    return super(Time, cls).__new__(cls, int(_time.time()/60)*Time._m2ns)
                if time[1]=='h':
                    return super(Time, cls).__new__(cls, int(_time.time()/3600)*Time._h2ns)
            else:  # '2009-02-13T23:31:30.123456789Z'
                time = _re.findall('[0-9]+', time)
                if len(time)==1:
                    if len(time[0])==6:
                        time = [time[0][0:2],time[0][2:4],time[0][4:6]]
                    elif len(time[0])==8:
                        time = [time[0][0:4],time[0][4:6],time[0][6:8]]
                elif len(time) == 7:  # we have subsecond precision
                    time = time[0:6] + _re.findall('[0-9]{3}', time[6]+'00')
                time = [int(t) for t in time]
                return listtovalue(time)
        if isinstance(time, (_time.struct_time,)):
            return listtovalue(list(time)[0:6])
        if _ver.has_mds and isinstance(time, (_mds.Scalar)):
            if units is None:
                units = time.units
            time = time.data()
#        if isinstance(time, (_np.ScalarType)):
        if units is None:
            if _np.array(time).dtype==float:
                time = time*cls._s2ns
        else:
            units = units.lower()
            if units =='ns':
                pass
            elif units =='us':
                time = time*Time._us2ns
            elif units =='ms':
                time = time*Time._ms2ns
            elif units =='s':
                time = time*Time._s2ns
            elif units =='m':
                time = time*Time._m2ns
            elif units =='h':
                time = time*Time._h2ns
            elif units =='d':
                time = time*Time._d2ns
        return super(Time, cls).__new__(cls, time)


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

    def _s(self): return self.ns / Time._s2ns

    def _subsec(self): return self.ns % Time._s2ns

    def _utc(self):
        import time as _time
        values = tuple(list(_time.gmtime((self.ns % (1<<64))/Time._s2ns)[0:6]) + [self.subsec])
        return '%04d-%02d-%02dT%02d:%02d:%02d.%09dZ' % values

    def _local(self):
        import time as _time
        return _time.ctime((self.ns % (1<<64))/Time._s2ns)
    ns = property(_ns)
    s = property(_s)
    utc = property(_utc)
    local = property(_local)
    subsec = property(_subsec)


class TimeArray(list):
    def __new__(cls, arg=[]):
        if type(arg) is TimeArray:
            return arg
        else:
            if _ver.has_mds and isinstance(arg, _mds.Dimension): arg = arg.data()
            newarr = super(TimeArray, cls).__new__(cls)
            for i in arg:
                if i is None:
                    super(TimeArray, cls).append(newarr,None)
                else:
                    super(TimeArray, cls).append(newarr,Time(i))
            return newarr

    def __init__(self, arg=[]):
        return

    def __setitem__(self, idx, value):
        super(TimeArray, self).__setitem__(idx, Time(value))

    def append(self, value):
        super(TimeArray, self).append(Time(value))

    def _ns(self): return [i.ns if i is not None else None for i in self]

    def _s(self): return [i.s if i is not None else None for i in self]

    def _subsec(self): return [i.subsec if i is not None else None for i in self]

    def _utc(self): return [i.utc if i is not None else None for i in self]

    def _local(self): return [i.local if i is not None else None for i in self]

    ns = property(_ns)
    s = property(_s)
    utc = property(_utc)
    local = property(_local)
    subsec = property(_subsec)


class TimeInterval(TimeArray):
    """
    isinstance generic
    from <=  0 : upto -|X| ns
    from  >  0 : epoch +X ns
    upto <  -1 : now  -|X| ns
    upto == -1 : inf
    upto ==  0 : now
    upto  >  0 : epoch +X ns
    """

    def __new__(cls, arg=None, constant=True):
        if arg is None: arg = [-1800., 'now_m', -1]
        if type(arg) is TimeInterval:
            newti = arg  # short cut
        else:
            if _ver.has_mds and isinstance(arg, (_mds.Array, _mds.Ident, _mds.treenode.TreeNode, _tdi.VECTOR)):
                arg = arg.data()
            if isinstance(arg, (_np.ndarray,)):
                arg = arg.tolist()
            if _ver.has_mds and isinstance(arg, (_ver.npscalar,_ver.numbers)):
                try: arg = _mds.Tree('W7X',int(arg)).TIMING.data().tolist()
                except:
                    raise Exception('Parameter is treated as MDS shot number. Invalid shot number: '+str(arg))
            if not isinstance(arg, (list, tuple)):
                arg = [-1, arg, 0]
            elif isinstance(arg, (tuple,)):
                arg = list(arg)
            if len(arg) < 3:
                if len(arg) < 2:
                    if len(arg) == 0:
                        arg = [-1800.]
                    arg = list(map(Time,arg))
                    arg += [0] if arg[0] < 0 else arg
                arg += [-1] if arg[0]<0 else [0]
            if arg[0]==0: arg[0] = 'now_m'
            if arg[1]==0: arg[1] = 'now_m'
            newti = super(TimeInterval, cls).__new__(cls,arg)
        if constant:
            newti[0]=newti.fromT
            newti[1]=newti.uptoT
            newti[2]=newti.t0T
        return newti

    def append(self, time): self._setT0(time)

    def __setitem__(self, idx, time):
        super(TimeInterval, self).__setitem__(min(idx, 2), Time(time))

    def __getitem__(self, idx):
        idx = min(idx, 2)
        time = super(TimeInterval, self).__getitem__(idx)
        if idx == 0 and time < 0:
            return self[1] + time
        elif idx == 1:
            if(time == 0 or time < -2):
                return Time('now') + time
            else:
                time0 = super(TimeInterval, self).__getitem__(0)
                if time<time0:
                    return time+time0;
                return time
        elif idx == 2 and time < 0:
            if super(TimeInterval, self).__getitem__(idx) < 0:
                return self.uptoT
            return self.fromT
        else:
            return time

    def __str__(self):
        return 'from=' + str(self.fromT) + '&upto=' + str(self.uptoT)

    def filter(self):
        return 'filterstart=' + str(self.fromT) + '&filterstop=' + str(self.uptoT)

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

#    def _utc(self): return [self.fromT.utc, self.uptoT.utc]
#
#    def _local(self): return [self.fromT.local, self.uptoT.local]
#
#    def _s(self): return [self.fromT.s, self.uptoT.s]
#
#    def _ns(self): return [self.fromT.ns, self.uptoT.ns]
#
#    ns = property(_ns)
#    s = property(_s)
#    utc = property(_utc)
#    local = property(_local)
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
            #units = units.getSegment(0).units
            return 'unknown'
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


if _ver.has_mds:
    def createSignal(dat, dim, t0, unit=None, addim=[], units=[], help=None, value=None, scaling=None,**kwargs):
        def _dim(time,t0):
            if len(time):
                t0 = _mds.Int64(t0)
                time = _mds.Int64Array(time)
                if t0==0:
                    unit = 'ns'
                else:
                    time = _mds.Float64(time-t0)*1E-9
                    unit = 's'
                wind = _mds.Window(time[0], time[time.shape[0]-1], t0)
                dim = _mds.Dimension(wind, time)
                dim.setUnits(unit)
                return dim
            else:
                return _mds.EmptyData()

        def _addim(dim, units='unknown'):
            if len(dim):
                dim  = _mds.Dimension(None, tonumpy(dim))
                dim.setUnits(Units(units))#
                return dim
            else:
                return None

        if isinstance(dat, (list,)):
            dat = tonumpy(dat)
        dat = _mds.makeArray(dat)
        dim = _dim(dim,t0)
        for i in _ver.xrange(len(addim)):
            addim[i] = _addim(addim[i], units[i])
        if unit is not None:
            dat.setUnits(unit)
        if scaling is None and value is None:
            value = dat
            dat = None
        else:
            if value is None: value='$VALUE'
            if scaling is None:
                if isinstance(value, _ver.basestring):
                    value = _mds.Data.compile(value)
            else:
                if not isinstance(value, _ver.basestring):
                    value = value.decompile()
                value = _mds.Data.compile(value.replace('$VALUE',' polyval($VALUE,'+_mds.makeArray(scaling).decompile()+') '))
        sig = _mds.Signal(value, dat, dim, *addim)
        if help:
            sig.setHelp(help)
        return sig

    def tonumpy(dat):
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
            return _np.array([],'uint8')
        m, n = _datr(dat)
        if   n == 3:       nptype = 'complex128'
        elif n == 2:       nptype = 'float64'
        elif n == 1:
            if   m+1 > 64: nptype = 'int128'
            elif m+1 > 32: nptype = 'int64'
            elif m+1 > 16: nptype = 'int32'
            elif m+1 > 8:  nptype = 'int16'
            elif m+1 > 0:  nptype = 'int8'

        else:  # n == 0
            if   m > 64:   nptype = 'uint128'
            elif m > 32:   nptype = 'uint64'
            elif m > 16:   nptype = 'uint32'
            elif m > 8:    nptype = 'uint16'
            elif m > 0:    nptype = 'uint8'
                # else:
                # return MDSplus.BoolArray(dat==1)
            else:
                nptype = 'uint8'
        return _np.array(dat,nptype)

def dimof2w7x(dimof,t0):
    if _ver.has_mds and isinstance(dimof,_mds.Data):
        dimof = dimof.data()
    if isinstance(dimof, _np.ScalarType):
        return _np.int64(_np.around(dimof*1e9,-3))+t0
    arnd = -3 if len(dimof)==1 else int(-_np.log10(dimof[1]-dimof[0])-9)
    return _np.array(_np.around(dimof*1e9,arnd),'int64')+t0
