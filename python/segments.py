"""
Created on Fri Aug 07 13:19:04 2015
Segments
@author: cloud
"""
from MDSplus import Tree, Dimension, Signal, TdiCompile
from MDSplus import Float32Array, Uint32Array
from . import version as _ver


def segments1D():
    import math
    chunk = 100
    samples = 1000
    freq = 100.
    t0 = 0.
    dimT = [float(t/freq-t0) for t in _ver.xrange(samples)]
    datT = [math.cos(t*math.pi) for t in dimT]
    # your time vector 200 frames at 100Hz starting at t=0
    # dim = Dimension( Float32Array(timevector) ).setUnits("s")
    # would becomes Build_With_Units(Build_Dim(<array>), "s")
    # However, it looses the information if you try to index it
    with Tree('KKS_EVAL', 7) as tree:
        node = tree.getNode('.RESULTS.MYSECTION:MYSEGMENT')
        node.deleteData()
        for i in _ver.xrange(samples/chunk):
            dim = Dimension(dimT[i*chunk:i*chunk+chunk]).setUnits('s')
            dat = Float32Array(datT[i*chunk:i*chunk+chunk]).setUnits('V')
            node.makeSegment(dim[0], dim[-1], dim, dat)
        dat = node.getSegment(0)
    return dat


def segments2D():
    img = Uint32Array([[int(((y + x) << 2) / 2 + (x << 8) + (y << 16))
                        for y in range(90)] for x in range(160)])
    Nframes = 5
    freq = 100
    t0 = 0
    timevector = [t/freq-t0 for t in _ver.xrange(Nframes)]
    # your time vector 200 frames at 100Hz starting at t=0
    # dim = MDSplus.Dimension( MDSplus.Float32Array(timevector) ).setUnits("s")
    # would becomes Build_With_Units(Build_Dim(<array>), "s")
    # However, it looses the information if you try to index it
    with Tree('KKS_EVAL', 1) as tree:
        node = tree.getNode('.RESULTS.MYSECTION:MYSEGMENT2')
        node.deleteData()
        for i in _ver.xrange(Nframes):
            dim = Dimension(timevector[i]).setUnits('s')
            node.makeSegment(dim[0], dim[-1], dim, img.T)
        img1 = Signal(img, TdiCompile('*'), dim)
        img2 = node.getSegment(0)
        node.putSegment(img2.data(), 0)
        img3 = node.getSegment(0)
    return img1, img2, img3
print(segments1D())
