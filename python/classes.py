"""
archive.classes
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from . import base as _base
from . import interface as _if
from . import version as _ver
from . import support as _sup
defwritepath = '/test/raw/W7X/python_interface/test'


class datastream:
    def __init__(self, path=defwritepath):
        self._chanDesc = []
        self._chanVals = []
        self._chanDims = []
        self._parmsVal = dict()
        self._dimensions = None
        self._isImg = False
        self._path = _base.Path(path)

    def set_path(self, *path):
        self._path.set_path(*path)
        if self._path._lev != 5:
            Warning('url does no seem to be a datastream path')

    def set_dimensions(self, dimensions):
        self._dimensions = [_ver.long(t) for t in dimensions]

    def add_channel(self, name, data, units='unknown', description=''):
        from support import ndims
        isImg = ndims(data) > 1
        if isImg & len(self._chanVals) | self._isImg:
            raise Exception('Multidimensional data is only supported ' +
                            'as one channel per stream.')
        self._isImg = isImg
        self._chanDesc.append({'name': name,
                               'physicalQuantity': {'type': _base.Units(units)},
                               'active': 1 if len(data) > 0 else 0})
        self._chanVals.append(data)

    def add_property(self, name, value):
        self._parmsVal[name] = value

    def _parlog(self):
        if self._dimensions is None:
            raise Exception('No dimensions defined.\n' +
                            'You may set a common array with setDimensions()')
        self._parmsVal['chanDescs'] = self._chanDesc
        self._parmsVal['dataBoxSize'] = len(self._dimensions)
        struct = {'values': [self._parmsVal],
                  'dimensions': [self._dimensions[0], -1],
                  'label': 'parms',
                  }
        return struct

    def path(self, lev=-1):
        return self._path.path(lev)

    def url(self, lev=-1):
        return self._path.url(lev)

    def write(self):
        r = [_if.post(self._path.url_parlog(), json=self._parlog()),
             _if.write_data(self._path, self._chanVals, self._dimensions)]
        return r


class browser(_base.Path):
    def __new__(cls, path=_base._defreadpath, time=None):
        if type(path) is browser:
            if time is not None: path.set_time(time)
            return path
        elif isinstance(path, _base.Path):
            path = path.path
        newbrowser = super(browser,cls).__new__(cls, path)
        newbrowser.set_time(time)
        return newbrowser

    def set_time(self, time=None):
        self._time = _base.TimeInterval() if time is None else _base.TimeInterval(time)

    def time(self):
        return self._time

    def _set_project(self, project):
        if isinstance(project, int):
            project = self.list_projects()[project]
        super(browser,self)._set_project(project)
        return self

    def _set_streamgroup(self, streamgroup):
        if isinstance(streamgroup, int):
            streamgroup = self.list_streamgroups()[streamgroup]
        super(browser,self)._set_streamgroup(streamgroup)
        return self

    def _set_stream(self, stream):
        if isinstance(stream, int):
            stream = self.list_streams().keys()[stream]
        super(browser,self)._set_stream(stream)
        return self

    def _set_channel(self, channel, name=None):
        if name is None:
            name = self.list_channels()[channel]
        super(browser,self)._set_channel(channel, name)
        return self

    project = property(_base.Path._get_project, _set_project)
    streamgroup = property(_base.Path._get_streamgroup, _set_streamgroup)
    stream = property(_base.Path._get_stream, _set_stream)
    channel = property(_base.Path._get_channel, _set_channel)

    # read
    def read_data(self, skip=None, nsamples=None, channel=None):
        return _if.read_signal(self.url_datastream(), self.time(),
                               0, skip, nsamples, channel)

    def read_channel(self, skip=None, nsamples=None):
        return _if.read_signal(self.url_channel(), self.time(), 0, skip, nsamples)

    def read_parlog(self, skip=None, nsamples=None):
        return _if.read_parlog(self.url(5), self.time())

    def read_cfglog(self, skip=None, nsamples=None):
        return _if.read_cfglog(self.url_cfglog(), self.time)

    # get lists
    def list_databases(self):
        return list_children(self, 1)

    def list_views(self):
        return list_children(self, 2)

    def list_projects(self):
        return list_children(self, 3)

    def list_streamgroups(self):
        return list_children(self, 4)

    def list_streams(self):
        return list_children(self, 5)

    def list_channels(self):
        try:
            return list_children(self, 6)
        except:
            params = self.read_parlog(0, 1)[0]
            if 'chanDescs' in params.keys():
                cD = params['chanDescs']
                return [(cD[i]["name"], str(i)) for i in _ver.xrange(len(cD))]

    def list_chnames(self): return list_children(self, 7)

    def list_scales(self): return list_children(self, 8)

    # print lists
    def print_views(self): self._print_list(self.list_views())

    def print_projects(self): self._print_list(self.list_projects())

    def print_streamgroups(self): self._print_list(self.list_streamgroups())

    def print_streams(self): self._print_list(self.list_streams())

    def print_channels(self): self._print_list(self.list_channels())

    def print_chnames(self): self._print_list(self.list_chnames())

    def _print_list(self, lst):
        from support import fixname
        for i in _ver.xrange(len(lst)):
            fixn = fixname(lst[i][0] if isinstance(lst[i], tuple) else lst[i])
            print("%3d %s" % (i, fixn))


def list_children(url, lev=-1):
    from re import compile, escape, I
    f = '(?:|/\\?filterstart)'
    if lev < 0:
        [url, lev] = _base.Path(url).url(-2)
    else:
        url = _base.Path(url).url(lev-1)
    if lev < 2:  # databases
        return ['ArchiveDB', 'Test']
    elif lev < 5:  # stream group in projects in views
        rec = compile(escape(url) + '/([^/]+)()', I)
        NAME = []
    elif lev == 5:  # stream for channel in stream group
        rec = compile(escape(url) + '/([^/]+)_(DATASTREAM|PARLOG|CFGLOG)'+f, I)
        NAME = {}
    elif lev == 6:  # search for channel in stream
        rec = compile(escape(url) + '/([^/]+)/([^/\\?]+)'+f, I)
        NAME = {}
    elif lev == 7:  # search for channel on index or scaled
        rec = compile(escape(url) + '/([^/]+)()'+f, I)
        NAME = []
    elif lev == 8:  # search for scales
        rec = compile(escape(url) + '/([^/]+)()'+f, I)
        NAME = []
    json = _if.get_json(url+'/')
    children = json['_links']['children']
    for c in children:
        m = rec.search(c['href'])
        if m is not None:
            (k, v) = (str(m.group(1)), str(m.group(2)))
            if lev == 6:
                NAME[int(k)] = v
            elif lev == 5:
                if k in NAME.keys():
                    NAME[k].append(v)
                else:
                    NAME[k] = [v]
            else:
                NAME.append(k)
    return NAME


def get_obj_url(url):
    json = _if.get_json(url)
    return _sup.obj(json)
