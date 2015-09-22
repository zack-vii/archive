# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 10:27:18 2015

@author: Cloud
"""
import MDSplus as _mds
import archive as _archive
import time as _time
import numpy as _np
import sys as _sys
name = 'TEST'
period = 1.
if len(_sys.argv)>1:
    name = _sys.argv[1]
if len(_sys.argv)>2:
    period = float(_sys.argv[2])


tree = _mds.Tree('transient',-1)
if len(tree.getNodeWild(name))<1:
    tree.edit()
    node = tree.addNode(name,'SIGNAL')
    node.addNode('description','TEXT').putData('test description')
    node.addNode('UNITS','TEXT').putData('V')
    tree.write()
    _mds.Event.setevent('transient',name)
tree.quit()

while True:
    try:
        tree = _mds.Tree('transient',0)
        node = tree.getNode(name)
        time = _archive.Time('now')
        data = _mds.Float64Array([_np.sin(time.s/30*_np.pi)])
        dim  = _mds.Uint64Array([time.ns]).setUnits('ns')
        node.makeSegment(dim[0],  dim[len(dim)-1], dim, data,-1)
        print("written: %d: %f (%d)" % (dim[0], data[0], node.getNumSegments()))
    except:
        _archive.support.error()
#    del(tree);tree = _mds.Tree()
    _time.sleep(period-(_time.time() % period))