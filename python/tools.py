"""
codac.tools
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from MDSplus import Tree
import MDSplus,sys
if sys.version_info.major==3:
    xrange=range

def funcheck(funname,*args):
    import re
    import os
    from MDSplus import TdiExecute
    with open(os.environ['MDS_PATH']+'\\'+funname+'.fun','r') as f:
        lines=f.readlines()
    while not (lines[0].startswith('fun') or lines[0].startswith('public fun')):        
        del(lines[0])
    r = re.findall('(as_is|in)\s+(_[a-z0-9_]+)', lines[0])
    for i in xrange(len(args)):
        if r[i][0]=="in":
            res = TdiExecute(r[i][1]+'=$', (args[i],))
        else:
            res = TdiExecute(r[i][1]+'=AS_IS($)', (args[i],))
        print(res)
    for l in lines[2:-1]:
        l = l.strip(' ').replace('return','_return=')
        print(l)
        res = TdiExecute(l)
        print(res)
    try:
        return(TdiExecute('_return'))
    except:
        return()

def plotdata(path='\\ARCHIVESB::DMD10195:CH1', treename='sandbox', shot=5, server=None):
    from pylab import plot,title,xlabel,ylabel
    if server is not None:
        Con = MDSplus.Connection(server)
        Con.openTree(treename,shot)
        name = Con.get('{}:$NAME'.format(path))
        data = Con.get('DATA({})'.format(path))
        time = Con.get('DIM_OF({})'.format(path))
        unit = Con.get('UNITS_OF({})'.format(path))
    else:
        with Tree(treename,shot) as tree:
            Node = tree.getNode(path)
            Data = Node.getData()
            name = Node.getNode('$NAME').data()
            data = Data.data()
            time = Data.dim_of().data()
            unit = Data.getUnits()
    plot(time,data)
    xlabel('time (s)')
    ylabel(unit)
    title(name)

def getNodeContent(tree,node,shot):
    with Tree(tree,shot) as tree:
        content = tree.getNode(node).data()
    return content
def setNodeContent(tree,node,shot,data):
    with Tree(tree,shot) as tree:
        tree.getNode(node).putData(data)
def setNodeParameter(tree,node,shot,data,units):
    from MDSplus.mdsdata import makeData
    from numpy import array
    data = makeData(array(data))
    data.setUnits(units)
    setNodeContent(tree,node,shot,data)