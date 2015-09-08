"""
codac.baseclasses
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import re,numpy
from codac import PY3
if PY3:
    basestring=(str,bytes)
    xrange=range
    long=int
#from support import error
defreadpath  = '/ArchiveDB/raw/W7X/CoDaStationDesc.10251/DataModuleDesc.10193_DATASTREAM/0/AAB27CT003'
rooturl = 'http://archive-webapi.ipp-hgw.mpg.de'

class InsufficientPathException(Exception):
    def __init__(self, value=''):
        self.value = 'insufficient path information'
class Path():
    _ROOTURL = rooturl
    def __init__(self, path=defreadpath, *_argin):
        if isinstance(path,(Path)):
            self._path = path._path
            self._lev  = path._lev
        else:
            self.set_path(str(path))
    def __str__(self):
        return self.path()
    def set_path(self,*path):
        [self._path,self._lev] = self.path(-2,'/'.join(path))
    def path(self, lev=-1, _path=None):
        if _path is None:
            _path = self._path
        if not (_path[0]=='/'):
            if (_path[0:7].lower()=="http://"):
                _path = '/'.join(_path[7:].split('/')[1:])
            else:
                _path = 'ArchiveDB/' + _path
        _path = _path.strip('/').split('/')
        if len(_path)>4:
            if not ( _path[4].endswith('_DATASTREAM') or _path[4].endswith('_PARLOG') or _path[4].endswith('_CFGLOG') ):
                _path[4] = _path[4] + '_DATASTREAM'
        if lev==-2:
            return ['/' + '/'.join(_path), len(_path)]
        if lev==-1:
            return '/' + '/'.join(_path)
        else:
            _path = _path[0:lev]
            if len(_path)<lev:
                raise InsufficientPathException
        return '/'+'/'.join(_path)
    def url(self,lev=-1, *arg):
        return url_parms(self._ROOTURL+self.path(lev), *arg)
    def set_database(self, database ):
        self.set_path(database)
    def set_view(self, view ):
        if type(view) is int:
            if view <= 0:
                view = 'raw'
            elif view == 1:
                view = 'cocking'
            else:
               view = 'cocked'
        self.set_path(self.path(1),view)
    def set_project(self, project):
        if type(project) is int:
            project = self.list_projects()[project]
        self.set_path(self.path(2),project)
    def set_streamgroup(self, streamgroup ):
        if type(streamgroup) is int:
            streamgroup = self.list_streamgroups()[streamgroup]
        self.set_path(self.path(3),streamgroup)
    def set_stream(self, stream ):
        if type(stream) is int:
            stream = self.list_streams()[stream]
        self.set_path(self.path(4),stream)
    def set_channel(self, channel ):
        channellist = self.list_channels()
        if type(channel) is int:
            channel = channellist[channel]
        else:
            channel = channellist[[c[0] for c in channellist].index(channel)]
        self.set_path(self.path(5),channel[1],channel[0])
    # get path
    def path_database(self):
        if self._lev<1: raise InsufficientPathException
        return self.path(1)
    def path_view(self):
        if self._lev<2: raise InsufficientPathException
        return self.path(2)
    def path_project(self):
        if self._lev<3: raise InsufficientPathException
        return self.path(3)
    def path_streamgroup(self):
        if self._lev<4: raise InsufficientPathException
        return self.path(4)
    def path_cfglog(self):
        if self._lev<4: raise InsufficientPathException
        streamgroup = self.path(4)
        return streamgroup + '/' + streamgroup.split('/')[-1] + '_CFGLOG'
    def path_datastream(self):
        if self._lev<5: raise InsufficientPathException
        return self.path(5)
    def path_parlog(self):
        if self._lev<5: raise InsufficientPathException
        return self.path(5)[:-11]+'_PARLOG'
    def path_channel(self):
        if self._lev<7: raise InsufficientPathException
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
        if self._lev>6:
            return self.url_channel(*arg)
        else:
            return self.url_datastream(*arg)

    # get name
    def name_database(self):
        return self.path_database().split('/')[-1]
    def name_view(self):
        return self.path_view().split('/')[-1]
    def name_project(self):
        return self.path_project().split('/')[-1]
    def name_streamgroup(self):
        return self.path_streamgroup().split('/')[-1]
    def name_datastream(self):
        return self.path_datastream().split('/')[-1][:-11]
    def name_channel(self):
        return self.path_channel().split('/')[-1]

def url_parms( url, time=None, skip=0, nsamples=0, channels=[]):
    if time is not None:
        time = TimeInterval(time)
        url = url + '/_signal.json'
        par=[str(time)]
        if skip>0:
            par.append('skip=' + str(skip))
        if nsamples>0:
            par.append('nSamples=' + str(nsamples))
        if len(channels):
            par.append('channels=' + ','.join(map(str,channels)))
        if len(par):
            url = url+'?'+'&'.join(par)
    return url


units = ['unknown','','none','arb.unit','kg','g','u','kg/s','g/s','m','cm','mm','nm','Angstrom','m^2','m^3','L','m^-1','nm^-1','m^-2','cm^-2','m^-3','cm^-3','m^3/s','m^3/h','L/s','L/min','L/h','(m.s)^-1','(cm.s)^-1','s','min','h','ms','us','ns','s^-1','Hz','kHz','MHz','GHz','Bq','A','kA','mA','uA','A/s','C','K','oC','deg.C','rad','o','r/min','sr','count','ustrain','%','dB','m/s','km/s','km/h','N','Pa','hPa','bar','mbar','ubar','bar.L','mbar.L','bar.L/s','mbar.L/s','mbar.L/min','sccm','J','kJ','MJ','eV','keV','W','mW','kW','MW','W/m^2','mW/cm^2','W/m^3','W/(m^3.sr)','W/(m^2.sr.nm)','W/(cm^2.sr.nm)','VA','MVA','V','kV','mV','uV','V/s','V.s','V^-1','Wb','Ohm','S','F','pF','H','T','Gs','T/A','T/s','Gy','Sv','uSv','rem','urem','Sv/h','1E20','%Tm/MA','bit','Byte','KiByte','MiByte']
def Unit(unit,force=False):
    import MDSplus
    if isinstance(unit, (MDSplus.treenode.TreeNode)):
        unit = unit.getSegment(0).units if unit.getNumSegments() else unit.units
    elif isinstance(unit, (MDSplus.compound.Signal,MDSplus.mdsarray.Array)):
        unit = unit.units
    unit = str(unit)
    if unit in units:
        return unit
    if force:
        print("'"+unit+"' is not a recognized unit but has been enforced!")
        return unit
    raise Exception("Unit must be one of '"+"', '".join(units))

class Time():
    def __init__(self, t=''):
        if isinstance(t, (Time,)):
            self._value = t._value
        else:
            self.settime(t)
    def __call__(self):
        return self._value
    def __cmp__(self,y):
        return self._value.__cmp__(long(y))
    def __trunc__(self):
        return int(self._value)
    def __add__(self,y):
        return self._value+y
    def __sub__(self,y):
        return self._value-y
    def __mul__(self,y):
        return self._value*y
    def __div__(self,y):
        return self._value/y
    def __radd__(self,y):
        return y+self._value
    def __rsub__(self,y):
        return y-self._value
    def __rmul__(self,y):
        return y*self._value
    def __rdiv__(self,y):
        return y/self._value
    def __str__(self):
        return str(self._value)
    def ns(self):
        return self._value
    def s(self):
        return self._value*1E-9
    def utc(self):
        import time
        return time.ctime(self.ns()/1e9)
    def local(self):
        import time
        return time.ctime(self.ns()/1e9-time.timezone+time.daylight*3600)
    def settime(self, t=''):
        import time
        from MDSplus import Scalar,TreeNode
        if isinstance(t,(basestring)) and t == '':                 #now
            self._value = long(time.time()+time.timezone-time.daylight*3600)*1000000000
        elif isinstance(t, (TreeNode)):
            t=t.data()
            if t<1E10 and t>0:          t = t*1000000000          #time in 's'
            self._value = long(t)
        elif isinstance(t, (numpy.ScalarType, Scalar)) and not isinstance(t,(basestring)):
            if isinstance(t,(Scalar)) : t = t.data()
            if t<1E10 and t>0:          t = t*1000000000          #time in 's'
            self._value = long(t)
        else:
            if isinstance(t,(str)):                #'2015/12/31-23:59:59.123456789'
                t = re.findall('[0-9]+',t)
                for i in xrange(min(len(t),6)):
                    t[i] = int(t[i])
                if len(t) == 7:
                    tmp = re.findall('[0-9]{3}',t.pop() + '00')
                    for i in tmp:
                        t.append(int(i))
            elif isinstance(t,(time.struct_time)):
                t = list(t)[0:6]
            else:
                print(type(t))
                t = list(t[0:9])
            for i in xrange(len(t),9):
                t.append(0)
            s = int(time.mktime((t[0],t[1],t[2],t[3],t[4],t[5],0,0,0)))+time.timezone
            self._value = ((s*1000+t[6])*1000+t[7])*1000+t[8]

class TimeInterval(list):
    def __init__ (self, arg=''):
        from MDSplus.mdsarray import Array
        from MDSplus.treenode import TreeNode
        from MDSplus.tdibuiltins.builtins_other import VECTOR
        if not isinstance(arg, (TimeInterval)):
            if isinstance(arg,(Array,TreeNode,VECTOR)):
                arg = arg.data()[0:2].tolist()
            elif isinstance(arg,(numpy.ndarray)):
                arg = arg[0:2].tolist()
            elif not isinstance(arg,(list,tuple)):
                arg = [arg,-1]
            arg = list(map(Time,arg[0:2]))
        self.append(arg[0])
        if len(arg)>1:
            self.append(arg[1])
        else:
            self.append(arg[0])
    def __setitem__(self,i,t):
        super(TimeInterval,self).__setitem__(i,Time(t))
    def __str__(self):
        return 'from=' + str(self.getFrom()-1) + '&upto=' + str(self.getUpto())
    def setFrom(self,t):
        self[0]  = Time(t)
    def setUpto(self,t):
        self[-1] = Time(t)
    def getFrom(self):
        return self[0]
    def getUpto(self):
        return self[-1]
    def fromStr(self):
        return self[0]
    def uptoStr(self):
        return str(self[0].ns())
    def utc(self):
        return map(Time.uts,self)
    def local(self):
        return map(Time.local,self)
    def s(self):
        return map(Time.s,self)
    def ns(self):
        return map(Time.ns,self)

def get_time_1h():
    time = Time().s()
    return TimeInterval([time-3600,time])
defaulttime = get_time_1h()

def createSignal(dat, dim, t0 = 0, unit=None, addim=[], units=[], help=None):
    import MDSplus
    from numpy import ndarray
    if isinstance(dat,(ndarray)):
        dat = dat.tolist()
    def _dim(dim,t0):
        t0 = Time(t0).ns()
        def normt(t, t0 = t0):
            return (Time(t).ns()-t0)/1.E9
        if len(dim):
            wind = MDSplus.Window(dim[0]-1,dim[-1],0+t0)
            dim  = MDSplus.Float64Array(map(normt,dim))
            dim  = MDSplus.Dimension(wind,dim)
            dim.setUnits('s')
            return dim
        else:
            return MDSplus.EmptyData()
    def _addim(dim, unit='unknown'):
        if len(dim):
            dim  = MDSplus.Dimension(_dat(dim))
            dim.setUnits(unit)
            return dim
        else:
            return MDSplus.EmptyData()
    def _dat(dat):
        def _datr(dat,m=0,n=0):
            if len(dat)==0 or (n>1 and m+n>64):
                return m,n
            if isinstance(dat[0], (list,)):
                for x in dat: # recursive
                    m,n = _datr(x,m,n)
                return m,n
            else:
                n = n or any([x<0  for x in dat])
                try:
                    m = max(map(int.bit_length,dat)+[m])
                except:
                    #print(error())
#                    if any([isinstance(x, (complex,)) for x in dat]):
#                        return 0,3
                    #if any([isinstance(x, (float,)) for x in dat]):
                    return 0,2
                return m,n
        if len(dat)==0:
            return MDSplus.EmptyData()
        m,n = _datr(dat)
        if n==3:
            return MDSplus.Complex128Array(dat)
        if n==2:
            return MDSplus.Float64Array(dat)
        if n:
            if m+1>64:
                return MDSplus.Int128Array(dat)
            elif m+1>32:
                return MDSplus.Int64Array(dat)
            elif m+1>16:
                return MDSplus.Int32Array(dat)
            elif m+1>8:
                return MDSplus.Int16Array(dat)
            else:
                return MDSplus.Int8Array(dat)
        else:
            if m>64:
                return MDSplus.Uint128Array(dat)
            elif m>32:
                return MDSplus.Uint64Array(dat)
            elif m>16:
                return MDSplus.Uint32Array(dat)
            elif m>8:
                return MDSplus.Uint16Array(dat)
            elif m>0:
                return MDSplus.Uint8Array(dat)
#            elif:
#                return MDSplus.BoolArray(dat==1)
            else:
                return MDSplus.EmptyData()
    dat = _dat(dat)
    dim = _dim(dim,t0)
    for i in xrange(len(addim)):
        addim[i] = _addim(addim[i],units[i])
    raw = MDSplus.Data.compile('*')
    if unit is not None:
        dat.setUnits(unit)
    sig = MDSplus.Signal(dat, raw, dim, *addim)
    if help:
        sig.setHelp(help)
    return sig