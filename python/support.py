"""
codac.support
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from __future__ import absolute_import
import MDSplus as _mds
import numpy as _np
import re as _re
import time as _time
import os as _os
from . import base as _base
from . import version as _ver

debuglevel = int(_os.getenv('DEBUG_ARCHIVE','0'))


def version():
    return '2015.01.05.16.00'


def nowstr():
    return _time.strftime('%Y/%m/%d-%H:%M:%S')


def sampleImage(imgfile='image.jpg'):
    from scipy.misc import imread
    im = imread(imgfile).astype('int32')
    return (im[:, :, 2]+im[:, :, 1]*256+im[:, :, 0]*65536).T.tolist()


def treeToDict(node, Dict={}, exclude=[], name=None):
    """generates a dict of the of a node structure
    called by <multiple>"""
    try:
        if node.usage in exclude:  # exclude by usage
            return Dict
        sDict = {}
        if name is None:
            name = node.getNodeName().lower()
        elif name=='':
            sDict = Dict
        try:
            data = node.data().tolist()
            try:
                data = data.tolist()
            except:
                pass
        except:
            data = None
        for d in node.getDescendants():
            sDict = treeToDict(d, sDict, exclude)
        if len(sDict.keys()):
            if data is not None:
                sDict["$value"] = data
            if name!='':
                Dict[name] = sDict
        elif data is not None:
            if name!='':
                Dict[name] = data
            else:
                Dict = data
    except:
        pass
    return(Dict)


class remoteTree(_mds.Tree):
    def __init__(self, shot='-1', tree='W7X', server='mds-data-1'):
        from MDSplus import Connection
        self._tree = tree
        self._shot = shot
        self._connection = Connection(server)
        super(remoteTree, self).__init__(tree, shot)

    def __exit__(self):
        self.__del__()

    def __del__(self):
        try:
            self._connection.closeTree(self._tree, self._shot)
        finally:
            self._connection.__del__()


def getDateTimeInserted(node):
    return str(_mds.TdiExecute('DATE_TIME(GETNCI($,"TIME_INSERTED"))', (node,)))


def getTimeInserted_old(node):
    timestr = getDateTimeInserted(node)
    try:
        time = list(_re.findall('([0-9]{2})-([A-Z]{3})-([0-9]{4}) ([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]{2})', timestr)[0])
    except:
        return _base.Time(0)
    time[1] = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'].index(time[1])+1
    time = [int(t) for t in time]
    return _base.Time([time[2]]+[time[1]]+[time[0]]+time[3:-1]+[time[-1]*10], local=True)

def getTimeInserted(node):
    return _base.Time(100*(int(_mds.TdiExecute('GETNCI($,"TIME_INSERTED")', (node,)))-35067240000000000), local=True)

class Flags(int):
    _STATE             =0x00000001
    _PARENT_STATE      =0x00000002
    _ESSENTIAL         =0x00000004
    _CACHED            =0x00000008
    _VERSIONS          =0x00000010
    _SEGMENTED         =0x00000020
    _SETUP_INFORMATION =0x00000040
    _WRITE_ONCE        =0x00000080
    _COMPRESSIBLE      =0x00000100
    _DO_NOT_COMPRESS   =0x00000200
    _COMPRESS_ON_PUT   =0x00000400
    _NO_WRITE_MODEL    =0x00000800
    _NO_WRITE_SHOT     =0x00001000
    _PATH_REFERENCE    =0x00002000
    _NID_REFERENCE     =0x00004000
    _INCLUDE_IN_PULSE  =0x00008000
    _COMPRESS_SEGMENTS =0x00010000

    def __new__(self, flags):
        if isinstance(flags, _mds.TreeNode):
            flags = _mds.TdiExecute('GETNCI($,"GET_FLAGS")', (flags,))
        return int.__new__(self, int(flags))

    def write(self,n):
        changed = Flags(Flags(n) ^ self)
        if changed.state: n.setOn(self.on)
        if changed.essential: n.setEssential(self.essential)
        if changed.write_once: n.setWriteOnce(self.write_once)
        if changed.do_not_compress: n.setDoNotCompress(self.do_not_compress)
        if changed.compress_on_put: n.setCompressOnPut(self.compress_on_put)
        if changed.no_write_model: n.setNoWriteModel(self.no_write_model)
        if changed.no_write_shot: n.setNoWriteShot(self.no_write_shot)
        if changed.include_in_pulse: n.setIncludedInPulse(self.include_in_pulse)
        if changed.compress_segments: n.setCompressSegments(self.compress_segments)
        return Flags(n)
    def _state(self): return bool(self & 1<<0)
    def _parent_state(self): return not bool(self & 1<<1)
    def _essential(self): return bool(self & 1<<2)
    def _cached(self): return bool(self & 1<<3)
    def _versions(self): return bool(self & 1<<4)
    def _segmented(self): return bool(self & 1<<5)
    def _setup(self): return bool(self & 1<<6)
    def _write_once(self): return bool(self & 1<<7)
    def _compressible(self): return bool(self & 1<<8)
    def _do_not_compress(self): return bool(self & 1<<9)
    def _compress_on_put(self): return bool(self & 1<<10)
    def _no_write_model(self): return bool(self & 1<<11)
    def _no_write_shot(self): return bool(self & 1<<12)
    def _path_reference(self): return bool(self & 1<<13)
    def _nid_reference(self): return bool(self & 1<<14)
    def _include_in_pulse(self): return bool(self & 1<<15)
    def _compress_segments(self): return bool(self & 1<<16)

    def _on(self): return not self._state()
    def _parent_on(self): return self._parent_state()

    state = property(_state)
    parent_state = property(_parent_state)
    essential = property(_essential)
    cached = property(_cached)
    versions = property(_versions)
    segmented = property(_segmented)
    setup = property(_setup)
    write_once = property(_write_once)
    compressible = property(_compressible)
    do_not_compress = property(_do_not_compress)
    compress_on_put = property(_compress_on_put)
    no_write_model = property(_no_write_model)
    no_write_shot = property(_no_write_shot)
    path_reference = property(_path_reference)
    nid_reference = property(_nid_reference)
    include_in_pulse = property(_include_in_pulse)
    compress_segments = property(_compress_segments)

    on = property(_on)
    parent_on = property(_parent_on)

def getTiming(shot, n=_ver.range(7)):
    def getTn(n):
        node = time.getNode('T%d:IDEAL' % n)
        if node.on:
            return int(node.record)
        else:
            return -1
    time = _mds.Tree('W7X', shot).TIMING
    if isinstance(n, (list, tuple)):
        return type(n)(getTn(i) for i in n)
    else:
        return getTn(n)

def getTimestamp(n=1):
    url = 'http://mds-data-1.ipp-hgw.mpg.de/operator/last_trigger/'+str(n)
    return(_base.Time(int(_ver.urllib.urlopen(url).read(20))))


def fixname(name):
    if not isinstance(name, (str)):
        name = _ver.tostr(name)
    name = _ver.urllib.unquote(name)
    return name


def fixname12(name):
    name = fixname(name)
    if name.lower().startswith('starttrigger'):
        name = 'startTrg' + name[12:]
    return name[:12]


def error(out=None):
    if out is None:
        import traceback
        trace = 'python error:\n'+traceback.format_exc()
        for line in trace.split('\n'):
            debug(line)
        return(trace)
    else:
        import sys
        e = sys.exc_info()
        if isinstance(out, list):
            out.append(e)
        return e


def addReplaceNode(tree, name, usage):
    if name[0].isdigit():
        name = 'N' + name
    name = fixname12(name)
    try:
        node = tree.addNode(name, usage)
    except:
        tree.getNode(name).delete()
        node = tree.addNode(name, usage)
    return node


def addOpenNode(tree, name, usage):
    if name[0].isdigit():
        name = 'N' + name
    name = fixname12(name)
    try:
        node = tree.addNode(name, usage)
    except:
        node = tree.getNode(name)
    return node


def cp(value):
    try:
        if isinstance(value, _np.generic):
            return value.tolist()
        elif isinstance(value, _ver.basestring):
            return _ver.tounicode(value).encode('utf-8')
        elif isinstance(value, _np.ScalarType):
            return value
        else:
            return value
    except:
        return None
        print(value)


class obj(object):
    def __init__(self, d):
        if isinstance(d, (list, tuple)):
            setattr(self, "items",
                    [obj(x) if isinstance(x, dict) else x for x in d])
        else:
            for a, b in d.items():
                if isinstance(b, dict):
                    setattr(self, a, obj(b))
                elif isinstance(b, (list, tuple)):
                    setattr(self, a,
                            [obj(x) if isinstance(x, dict) else x for x in b])
                else:
                    setattr(self, a, b)

    def __getitem__(self, key):
        return self.items[key]


def ndims(signal, N=0):
    if isinstance(signal, (list, tuple)):
        N = N+1
        if len(signal):
            N = ndims(signal[0], N)
    return N

def getSubTrees(expt='W7X',shot=-1, excludelist=('ARCHIVE',), excludereg=('.*_EVAL',)):
    rematch = tuple(_re.compile(reg) for reg in excludereg)
    top = _mds.TreeNode(0,_mds.Tree(expt,shot))
    subtrees = [c for c in top.getChildren() if c.usage=='SUBTREE' and not (c.node_name in excludelist) and all(re.match(str(c.node_name)) is None for re in rematch)]
    return subtrees

def getIncluded(*args):
    subtrees = getSubTrees(*args)
    return [st for st in subtrees if st.include_in_pulse]

def setTIME(tree,shot):
    tree = _mds.Tree(tree,shot)
    _t = tree.getNode('\TIME')
    def setT(n):
        t = _mds.Uint64(getTimestamp(n))
        t.setUnits('ns')
        _t.getNode('T%d:IDEAL' % n).putData(t)
    for i in range(7):
        setT(i)

def debug(msg, lv=1):
    if lv<=debuglevel:
        import inspect
        try:
            print(inspect.stack()[1][3] + ': ' + str(msg))
        except:
            print(msg)
