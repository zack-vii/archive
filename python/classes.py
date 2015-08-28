# -*- coding: utf-8 -*-
"""
codac.classes
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from .interface import post
#from requests import post
from .interface import get_json,read_parlog,read_cfglog,read_signal,write_image,write_data
from .base import Time,TimeInterval,Unit,Path
defwritepath = '/test/raw/W7X/python_interface/test'
import re,sys
if sys.version_info.major==3:
    xrange=range
    long=int


class datastream:
    def __init__(self, path=defwritepath):
        self._chanDesc = []
        self._chanVals = []
        self._chanDims = []
        self._parmsVal = dict()
        self._dimensions = None
        self._isImg = False
        self._path = Path(path)
    def set_path(self,*path):
        self._path.set_path(*path)
        if self._path._lev!=5:
            Warning('url does no seem to be a datastream path')
    def set_dimensions(self, dimensions):
        self._dimensions = [long(t) for t in dimensions]
    def add_channel(self, name, data, unit='unknown', description=''):
        from support import ndims
        isImg = ndims(data)>1
        if isImg & len(self._chanVals) | self._isImg:
            raise Exception('Multidimensional data is only supported as one channel per stream.' )
        self._isImg = isImg
        self._chanDesc.append({'name'            : name,
                               'physicalQuantity': {'type': Unit(unit)},
                               'active'          : 1 if len(data)>0 else 0})
        self._chanVals.append(data)
    def add_property(self,name, value):
        self._parmsVal[name] = value
    def _parlog(self):
        if self._dimensions is None:
            raise Exception('No dimensions defined.\n You may set a common array with setDimensions()')
        self._parmsVal['chanDescs'] = self._chanDesc
        self._parmsVal['dataBoxSize'] = len(self._dimensions)
        struct = {
        'values': [ self._parmsVal ],
        'dimensions': [self._dimensions[0],-1],
        'label': 'parms',
        }
        return struct
    def path(self,lev=-1):
        return self._path.path(lev)
    def url(self,lev=-1):
        return self._path.url(lev)
    def write(self):
        r    = [post(self._path.url_parlog(),     json=self._parlog()),None]
        r[1] = write_data(self._path, self._chanVals, self._dimensions)
        return r
    def writeimg(self):
        r    = [post(self._path.url_parlog(), json=self._parlog()), None]
        r[1] = write_image(self._path, self._chanVals[0], self._dimensions)
        return r
        

class browser(Path):
    def __init__(self, path=Path().path(), time=[Time()-3600000000000,Time()]):
        self.set_path(path)
        self.set_time(time)
    def set_time(self, time=TimeInterval()):
        self._time = TimeInterval(time)
    def time(self):
        return self._time
    # read    
    def read_data(self, skip=0, nsamples=0, channels=[]):
        return read_signal( self.url_datastream(), self.time(), 0, skip, nsamples, channels)
    def read_channel(self, skip=0, nsamples=0):
        return read_signal( self.url_channel(), self.time(), 0, skip, nsamples )
    def read_parlog(self, skip=0, nsamples=0):
        return read_parlog( self.url(4), self.time(), skip, nsamples )
    def read_cfglog(self,time=TimeInterval()):
        return read_cfglog(self.url_cfglog(),time)
    # get lists
    def list_databases(self):
        return list_children(self._path, 1)
    def list_views(self):
        return list_children(self._path, 2)
    def list_projects(self):
        return list_children(self._path, 3)
    def list_streamgroups(self):
        return list_children(self._path, 4)
    def list_streams(self):
        return list_children(self._path, 5)
    def list_channels(self):
        try:
            return list_children(self._path, 6)
        except:
            params = self.read_param(0,1)[0]
            if 'chanDescs' in params.keys():
                cD = params['chanDescs']
                return [(cD[i]["name"], str(i)) for i in xrange(len(cD))]
    def list_chnames(self):
        return list_children(self._path,7)
    # print lists
    def print_views(self):
        self._print_list( self.list_views() )
    def print_projects(self):
        self._print_list( self.list_projects() )
    def print_streamgroups(self):
        self._print_list( self.list_streamgroups() )
    def print_streams(self):
        self._print_list( self.list_streams() )
    def print_channels(self):
        self._print_list( self.list_channels() )
    def print_chnames(self):
        self._print_list( self.list_chnames() )
    def _print_list( self, lst ):
        from support import fixname
        for i in xrange(len(lst)):
            print( "%3d %s" % ( i, fixname(lst[i][0] if type(lst[i]) is tuple else lst[i]) ) )

def list_children(url, lev=-1):
    from re import compile
    if lev<0:
        [url, lev] = Path(url).url(-2)
    else:
        url = Path(url).url(lev-1)
    if lev<2: # databases
        return ['ArchiveDB','Test']
    elif lev<5: # stream group in projects in views
        rec = compile(re.escape(url) + '/([^/]+)()', re.I);    NAME = []
    elif  lev==5: # stream for channel in stream group
        rec = compile(re.escape(url) + '/([^/]+)_(DATASTREAM|PARLOG|CFGLOG)(?:|/\\?filterstart)', re.I);    NAME = {}
    elif  lev==6: # search for channel in stream
        rec = compile(re.escape(url) + '/([^/]+)/([^\\?]+)(?:|\\?filterstart)', re.I);   NAME = {}
    elif lev>=7: # search for channel on index
        rec = compile(re.escape(url) + '/([^/]+)()(?:|/\\?filterstart)', re.I);    NAME = []
    json = get_json(url)
    children = json['_links']['children']
    for c in children:
        m = rec.search(c['href'])
        if m!=None:
            k,v = m.group(1).encode('ascii'),m.group(2).encode('ascii')
            if lev in [5,6]:
                if lev==6:
                    NAME[v] = int(k)
                elif NAME.has_key(k):
                    NAME[k].append(v)
                else:
                    NAME[k] = [v]
            else:
                NAME.append(k)
    if lev in [5,6]:
        return NAME.keys(),NAME.values()
    else:
        return NAME

def get_obj_url( url ):
    from support import obj
    json = get_json(url)
    return obj(json)