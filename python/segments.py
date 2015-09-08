"""
Created on Fri Aug 07 13:19:04 2015
Segments
@author: cloud
"""
import MDSplus,sys
if sys.version_info.major==3:
    xrange=range

def segments1D():
    import math
    chunk = 100
    samples=1000
    freq  = 100.
    t0 = 0.
    dimT = [float(t/freq-t0) for t in xrange(samples)]
    datT = [math.cos(t*math.pi) for t in dimT]
    # your time vector 200 frames at 100Hz starting at t=0
    # dim = MDSplus.Dimension( MDSplus.Float32Array(timevector) ).setUnits("s")
    # would becomes Build_With_Units(Build_Dim(<array>), "s")
    # However, it looses the information if you try to index it
    
    with MDSplus.Tree('KKS_EVAL',7) as tree:
        node = tree.getNode('.RESULTS.MYSECTION:MYSEGMENT')
        node.deleteData()
        for i in xrange(samples/chunk):
            dim = MDSplus.Dimension(dimT[i*chunk:i*chunk+chunk]).setUnits('s')
            dat = MDSplus.Float32Array(datT[i*chunk:i*chunk+chunk]).setUnits('V')
            node.makeSegment(i,i,dim,dat)
        dat = node.getSegment(0)
    return dat
    
def segments2D():
    img = MDSplus.Uint32Array([[int(((y+x)<<2)/2+(x<<8)+(y<<16)) for y in range(90)] for x in range(160)])
    Nframes = 5
    freq = 100
    t0 = 0
    timevector = [t/freq-t0 for t in xrange(Nframes)]
    # your time vector 200 frames at 100Hz starting at t=0
    # dim = MDSplus.Dimension( MDSplus.Float32Array(timevector) ).setUnits("s")
    # would becomes Build_With_Units(Build_Dim(<array>), "s")
    # However, it looses the information if you try to index it
    
    with MDSplus.Tree('KKS_EVAL',1) as tree:
        node = tree.getNode('.RESULTS.MYSECTION:MYSEGMENT2')
        node.deleteData()
        for i in xrange(Nframes):
            dim = MDSplus.Dimension(timevector[i]).setUnits('s')
            node.makeSegment(i,i,dim,img.T)
        img1 = MDSplus.Signal(img,MDSplus.TdiCompile('*'),dim)
        img2 = node.getSegment(0)
        node.putSegment(img2.data(),0)
        img3 = node.getSegment(0)
    return img1,img2,img3
print(segments1D())