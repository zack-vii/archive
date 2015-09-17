"""
codac.baseclasses
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import re
import numpy
from .version import long, basestring, xrange
# from support import error
defreadpath = ('/ArchiveDB/raw/W7X/CoDaStationDesc.10251' +
               '/DataModuleDesc.10193_DATASTREAM/0/AAB27CT003')
rooturl = 'http://archive-webapi.ipp-hgw.mpg.de'


class InsufficientPathException(Exception):
    def __init__(self, value=''):
        self.value = 'insufficient path information'


class Path(object):
    _ROOTURL = rooturl

    def __init__(self, path=defreadpath, *_argin):
        if isinstance(path, (Path)):
            self._path = path._path
            self._lev = path._lev
        else:
            self.set_path(str(path))

    def __str__(self):
        return self.path()
    __repr__ = __str__

    def set_path(self, *path):
        [self._path, self._lev] = self.path(-2, '/'.join(path))

    def path(self, lev=-1, _path=None):
        if _path is None:
            _path = self._path
        if not (_path[0] == '/'):
            if (_path[0:7].lower() == "http://"):
                _path = '/'.join(_path[7:].split('/')[1:])
            else:
                _path = 'ArchiveDB/' + _path
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
        return url_parms(self._ROOTURL+self.path(lev), *arg)

    # set
    def set_database(self, database):
        self.set_path(database)

    def set_view(self, view):
        if type(view) is int:
            if view <= 0:
                view = 'raw'
            elif view == 1:
                view = 'cocking'
            else:
                view = 'cocked'
        self.set_path(self.path(1), view)

    def set_project(self, project):
        if type(project) is int:
            project = self.list_projects()[project]
        self.set_path(self.path(2), project)

    def set_streamgroup(self, streamgroup):
        if type(streamgroup) is int:
            streamgroup = self.list_streamgroups()[streamgroup]
        self.set_path(self.path(3), streamgroup)

    def set_stream(self, stream):
        if type(stream) is int:
            stream = self.list_streams()[stream]
        self.set_path(self.path(4), stream)

    def set_channel(self, channel):
        channellist = self.list_channels()
        if type(channel) is int:
            channel = channellist[channel]
        else:
            channel = channellist[[c[0] for c in channellist].index(channel)]
        self.set_path(self.path(5), channel[1], channel[0])

    # get
    def get_database(self):
        return self.path_database().split('/')[-1]

    def get_view(self):
        return self.path_view().split('/')[-1]

    def get_project(self):
        return self.path_project().split('/')[-1]

    def get_streamgroup(self):
        return self.path_streamgroup().split('/')[-1]

    def get_stream(self):
        return self.path_datastream().split('/')[-1][:-11]

    def get_channel(self):
        return self.path_channel().split('/')[-1]

    database = property(get_database, set_database)
    view = property(get_view, set_view)
    project = property(get_project, set_project)
    streamgroup = property(get_streamgroup, set_streamgroup)
    stream = property(get_stream, set_stream)
    channel = property(get_channel, set_channel)

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
        return url_parms(self._ROOTURL + self.path_datastream(), *arg)

    def url_parlog(self, *arg):
        return url_parms(self._ROOTURL + self.path_parlog(), *arg)

    def url_cfglog(self, *arg):
        return url_parms(self._ROOTURL + self.path_cfglog(), *arg)

    def url_channel(self, *arg):
        return url_parms(self._ROOTURL + self.path_channel(), *arg)

    def url_data(self, *arg):
        if self._lev > 6:
            return self.url_channel(*arg)
        else:
            return self.url_datastream(*arg)


def url_parms(url, time=None, skip=0, nsamples=0, channels=[]):
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


class Time(long):
    def __new__(self, time='now'):
        if isinstance(time, Time):
            return time
        else:
            import time as _time

            def listtovalue(time):
                time += [0]*(9-len(time))
                seconds = long(_time.mktime(tuple(time[0:6]+[0]*3)) -
                               _time.timezone)
                return long.__new__(self, ((seconds*1000+time[6])*1000 +
                                           time[7])*1000+time[8])
            from MDSplus import Scalar, TreeNode
            if isinstance(time, (basestring,)):
                if time == 'now':  # now
                    return long.__new__(self, _time.time()*1e9)
                else:  # '2009-02-13T23:31:30.123456789Z'
                    time = re.findall('[0-9]+', time)
                    if len(time) == 7:  # we have subsecond precision
                        time = time[0:6] + re.findall('[0-9]{3}', time[6]+'00')
                    time = [int(t) for t in time]
                    return listtovalue(time)
            elif isinstance(time, (_time.struct_time,)):
                return listtovalue(list(time)[0:6])
            elif isinstance(time, (TreeNode)):
                time = time.data()
                if time < 2E9 and time > 0:  # time in 's'
                    time = time*1000000000
                return long.__new__(self, time)
            elif isinstance(time, (numpy.ScalarType, Scalar)):
                if isinstance(time, (Scalar,)):
                    time = time.data()
                if time < 2E9 and time > 0:  # time in 's'
                    time = time*1000000000
                return long.__new__(self, time)
            else:
                print(type(time))
                return listtovalue(list(time[0:9]))

    def __add__(self, y): return Time(long.__add__(self, y))

    def __radd__(self, y): return Time(long.__radd__(self, y))

    def __sub__(self, y): return Time(long.__sub__(self, y))

    def __rsub__(self, y): return Time(long.__rsub__(self, y))

    def __repr__(self): return self.utc

    def _ns(self): return long(self)

    def _s(self): return long(self) * 1E-9

    def _subsec(self): return self % 1000000000

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

    def __init__(self, arg=[-1800000000000, 'now', -1]):
        if type(arg) is TimeInterval:
            return  # short cut
        from MDSplus.mdsarray import Array
        from MDSplus.treenode import TreeNode
        from MDSplus.tdibuiltins.builtins_other import VECTOR
        if isinstance(arg, (Array, TreeNode, VECTOR)):
            arg = arg.data().tolist()
        elif isinstance(arg, (numpy.ndarray,)):
            arg = arg.tolist()
        elif not isinstance(arg, (list, tuple)):
            arg = [arg]
        if len(arg) < 3:
            if len(arg) < 2:
                if len(arg) == 0:
                    arg == [-1800000000000]
                arg += [0] if arg[0] < 0 else arg
            arg += [-1]
        super(TimeInterval, self).append(Time(arg[0]))
        super(TimeInterval, self).append(Time(arg[1]))
        super(TimeInterval, self).append(Time(arg[2]))

    def append(self, time): self._setT0(time)

    def __setitem__(self, idx, time):
        super(TimeInterval, self).__setitem__(min(idx, 2), Time(time))

    def __getitem__(self, idx):
        idx = min(idx, 2)
        time = super(TimeInterval, self).__getitem__(idx)
        if idx == 0 and time <= 0:
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
        return 'from=' + str(self.fromT-1) + '&upto=' + str(self.uptoT)

    def __repr__(self):
        return 'UTC: [ '+self.fromT.utc+' , '+self.uptoT.utc+' ]'

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

units = ['unknown', '', 'none', 'arb.unit', 'kg', 'g', 'u', 'kg/s', 'g/s', 'm',
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


def Unit(unit, force=False):
    import MDSplus
    if isinstance(unit, (MDSplus.treenode.TreeNode)):
        if unit.isSegmented():
            unit = unit.getSegment(0).units
        else:
            unit = unit.units
    elif isinstance(unit, (MDSplus.compound.Signal, MDSplus.mdsarray.Array)):
        unit = unit.units
    unit = str(unit)
    if unit in units:
        return unit
    if force:
        print("'"+unit+"' is not a recognized unit but has been enforced!")
        return unit
    raise Exception("Unit must be one of '"+"', '".join(units))


def createSignal(dat, dim, t0=0, unit=None, addim=[], units=[], help=None):
    import MDSplus
    from numpy import ndarray
    if isinstance(dat, (ndarray,)):
        dat = dat.tolist()

    def _dim(dim, t0):
        t0 = Time(t0).ns

        def normt(t, t0=t0):
            return (Time(t).ns-t0)/1.E9
        if len(dim):
            wind = MDSplus.Window(dim[0]-1, dim[-1], 0+t0)
            dim = MDSplus.Float64Array(map(normt, dim))
            dim = MDSplus.Dimension(wind, dim)
            dim.setUnits('s')
            return dim
        else:
            return MDSplus.EmptyData()

    def _addim(dim, unit='unknown'):
        if len(dim):
            dim = MDSplus.Dimension(_dat(dim))
            dim.setUnits(unit)
            return dim
        else:
            return MDSplus.EmptyData()

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
            return MDSplus.EmptyData()
        m, n = _datr(dat)
        if n == 3:
            return MDSplus.Complex128Array(dat)
        if n == 2:
            return MDSplus.Float64Array(dat)
        if n:
            if m+1 > 64:
                return MDSplus.Int128Array(dat)
            elif m+1 > 32:
                return MDSplus.Int64Array(dat)
            elif m+1 > 16:
                return MDSplus.Int32Array(dat)
            elif m+1 > 8:
                return MDSplus.Int16Array(dat)
            else:
                return MDSplus.Int8Array(dat)
        else:
            if m > 64:
                return MDSplus.Uint128Array(dat)
            elif m > 32:
                return MDSplus.Uint64Array(dat)
            elif m > 16:
                return MDSplus.Uint32Array(dat)
            elif m > 8:
                return MDSplus.Uint16Array(dat)
            elif m > 0:
                return MDSplus.Uint8Array(dat)
                # else:
                # return MDSplus.BoolArray(dat==1)
            else:
                return MDSplus.EmptyData()
    dat = _dat(dat)
    dim = _dim(dim, t0)
    for i in xrange(len(addim)):
        addim[i] = _addim(addim[i], units[i])
    raw = MDSplus.Data.compile('*')
    if unit is not None:
        dat.setUnits(unit)
    sig = MDSplus.Signal(dat, raw, dim, *addim)
    if help:
        sig.setHelp(help)
    return sig
