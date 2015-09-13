"""
Created on Wed Aug 12 00:30:33 2015

@author: Cloud
"""
from MDSplus import Tree


def setRecord(node, istop=True):
    from MDSplus import TdiCompile
    if node.getNumDescendants():
        for dec in node.getDescendants():
            if dec.getNodeName() == '$URL' and ~istop:
                dec.record = TdiCompile('codac_url($)', (node,))
            elif dec.usage == 'ANY' and dec.getNodeName() == '$PARLOG':
                dec.record = TdiCompile('archive_parlog($,_time)', (node,))
            elif dec.getNodeName() == '$CFGLOG':
                dec.record = TdiCompile('archive_cfglog($,_time)', (node,))
            elif dec.usage == 'SIGNAL':
                dec.record = TdiCompile('archive_signal($,_time)', (dec,))
            setRecord(dec, False)

tree = Tree('ARCHIVESB', -1)
setRecord(tree.getNode('\TOP'))
