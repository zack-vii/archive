"""
Created on Tue Jul 07 02:47:48 2015

@author: Cloud
"""

from codac import createSignal,Time
def setTiming(treename="W7X",shot=0,t0=Time().ns()):
    from MDSplus import Uint64,Tree
    with Tree(treename,shot) as tree:
        tree.getNode('\TIME.T0:IDEAL').putData(Uint64(t0-60000000000L))
        tree.getNode('\TIME.T1:IDEAL').putData(Uint64(t0))
        tree.getNode('\TIME.T2:IDEAL').putData(Uint64(t0+1000000000L))
        tree.getNode('\TIME.T3:IDEAL').putData(Uint64(t0+2000000000L))
        tree.getNode('\TIME.T4:IDEAL').putData(Uint64(t0+3000000000L))
        tree.getNode('\TIME.T5:IDEAL').putData(Uint64(t0+4000000000L))
        tree.getNode('\TIME.T6:IDEAL').putData(Uint64(t0+5000000000L))
        return(long(tree.getNode('\TIME:T1:IDEAL').data()))


def writemds(path,treename="W7X",shot=0):
    from MDSplus import Tree
    import math
    freq = 1e6
    Range= 2
    interval = int(1e9/freq);
    pre_samples = int(5e3)
    samples = int(2e4)
    minv = -32767
    maxv = 32768
    t0 = setTiming(treename,shot)
    dim = [(samp-pre_samples)*interval+t0 for samp in xrange(samples)]
    raw = [min(maxv,max(minv,int(math.sin(2*math.pi*(t-t0)/1e9*100)/Range*65536))) for t in dim]
    with Tree(treename,shot) as tree:
        node = tree.getNode(path)
        sig = createSignal(raw,dim,t0,unit='V')
        node.putData(sig)
    return sig


def writemds1D(path,treename="W7X",shot=0):
    from MDSplus import Tree
    from codac import createSignal
    import gc
    import numpy
    freq = 200
    interval = int(1e9/freq);
    pre_samples = int(0)
    samples = int(1)
    X  = range(256)
    t0 = setTiming(treename,shot)
    dim = numpy.array([(samp-pre_samples)*interval+t0 for samp in xrange(samples)])
    img = numpy.array([(x<<8) for x in X])
    raw = numpy.array([img+(t<<16) for t in dim])
    sig = createSignal(raw,dim,t0,'count',[X],['xm'])
    gc.collect()
    with Tree(treename,shot) as tree:
        node = tree.getNode(path)
        node.putData(sig)
    return sig

def writemds2D(path,treename="W7X",shot=0):
    from MDSplus import Tree
    from codac import createSignal
    import gc
    freq = 200
    interval = int(1e9/freq);
    pre_samples = int(0)
    samples = int(1)
    X  = range(256)
    Y  = X
    t0 = setTiming(treename,shot)
    dim = [(samp-pre_samples)*interval+t0 for samp in xrange(samples)]
    img = [[int((x<<8)+(y<<16)) for x in X] for y in Y]#
    raw = [img  for t in dim]
    sig = createSignal(raw,dim,t0,'count',[X,Y],['xm','ym'])
    gc.collect()
    with Tree(treename,shot) as tree:
        node = tree.getNode(path)
        node.putData(sig)
    return sig

s1 = writemds(".KKS_EVAL.Results:MYSECTION:MYARRAY","SandBox",1)
s1 = writemds2D(".KKS_EVAL.Results.MYSECTION:MYIMAGE","SandBox",1)

def writedatastream():
    s=1000000000
    l=10
    q=s/l
    from codac import datastream,Time
    import math
    d = datastream('/Test/raw/W7X/python_interface/test')
    t = Time().ns()
    X = range(l)
    dim = [t+(i-l)*q for i in xrange(l)]
    d.add_channel('ch1',[math.sin(1)*math.cos(x) for x in X],'V')
    d.add_channel('ch2',[math.sin(2)*math.cos(x) for x in X],'V')
    d.add_channel('ch3',[math.sin(3)*math.cos(x) for x in X],'V')
    d.add_channel('ch4',[math.sin(4)*math.cos(x) for x in X],'V')
    d.add_channel('ch5',[math.sin(5)*math.cos(x) for x in X],'V')
    d.set_dimensions(dim)
    r = d.write()
    print(r)
    print(str(r[0]))
    print(str(r[1]))
    return d

def writedatastream2D():
    s=1000000000
    l=64
    q=s/l
    from codac import datastream,Time
    d = datastream('/Test/raw/W7X/python_interface/testImage')
    t = Time().ns()
    X = range(l)
    dim = [t+(i-l)*q for i in xrange(l)]
    RGB = [[[int((z<<2)+(x<<8)+(y<<16)) for y in range(256)] for x in range(256)] for z in X]
    d.add_channel('ch1',RGB,'V')
    d.set_dimensions(dim)
    r = d.writeimg()
    print(r)
    print(r[0].text)
    print(r[1].text)
    return d