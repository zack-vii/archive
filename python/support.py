"""
codac.support
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
def version():
    return '2015.08.08.12.00'

def sampleImage(imgfile='image.jpg'):
    from scipy.misc import imread
    im = imread(imgfile).astype('int32')
    return (im[:,:,2]+im[:,:,1]*256+im[:,:,0]*65536).T.tolist()

from MDSplus import Tree
class remoteTree(Tree):
    def __init__(self,shot='-1',tree='W7X',server='mds-data-1'):
        from MDSplus import Connection
        self._tree = tree;
        self._shot = shot;
        self._connection = Connection(server)
        super(remoteTree, self).__init__(tree,shot)
    def __exit__(self):
        self.__del__()
    def __del__(self):
        try:
            self._connection.closeTree(self._tree,self._shot)
        finally:
            self._connection.__del__()


def fixname(name):
    import urllib2
    name = name.encode('ascii')
    name = urllib2.unquote(name)
    return name

def fixname12(name):
    import urllib2
    name = name.encode('ascii')
    name = urllib2.unquote(name)
    if name.lower().startswith('starttrigger'):
        name = 'startTrg' + name[12:]
    return name[:12]

def error(out=None):
    if out is None:
        import traceback
        trace = 'python error:\n'+traceback.format_exc()
        for line in trace.split('\n'):
            print(line)
        return(trace)
    else:
        import sys
        e = sys.exc_info()
        if isinstance(out,list):
            out.append(e)
        return e

def addReplaceNode(tree, name, usage):
    if name[0].isdigit():
        name= 'N' + name
    name = fixname12(name)
    try:
        node = tree.addNode(name, usage)
    except:
        tree.getNode(name).delete()
        node = tree.addNode(name, usage)
    return node

def addOpenNode(tree, name, usage):
    if name[0].isdigit():
        name= 'N' + name
    name = fixname12(name)
    try:
        node = tree.addNode(name,usage)
    except:
        node = tree.getNode(name)
    return node

def cp(arg):
    import numpy
    if isinstance(arg,(str)):
        try:
            return arg.decode()
        except:
            return arg.decode('utf-8')
    if isinstance(arg,(numpy.generic, numpy.ndarray)):
        return arg.tolist()
    return arg

class obj(object):
    def __init__(self, d):
        if isinstance(d, (list, tuple)):
            setattr(self, "items", [obj(x) if isinstance(x, dict) else x for x in d])
        else:
            for a, b in d.items():
                if isinstance(b, (list, tuple)):
    #               setattr(self, a, [obj(x) if isinstance(x, dict) else x for x in b])
                    setattr(self, a,  [x for x in b])
                else:
    #              setattr(self, a, obj(b) if isinstance(b, dict) else b)
                   setattr(self, a, b)
    def __getitem__(self,key):
        return self.items[key]

def ndims(signal,N=0):
    if isinstance(signal,(list,tuple)):
        N = N+1
        if len(signal):
            N = ndims(signal[0],N)
    return N

def setTIME(time):
    from MDSplus import Int64Array,TdiCompile
    from codac import TimeInterval
    time = TimeInterval(time)
    data = Int64Array(time)
    data.setUnits('ns')
    TdiCompile('\TIME').putData(data)