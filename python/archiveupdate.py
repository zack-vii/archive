"""
Created on Wed Aug 12 00:30:33 2015

@author: Cloud
"""
def setRecord(node,istop=True):
    if node.getNumDescendants():
        for dec in node.getDescendants():
            if dec.getNodeName()=='$URL' and ~istop:
                dec.record=MDSplus.TdiCompile('codac_url($)',(node,))
            elif dec.usage=='ANY' and dec.getNodeName()=='$PARLOG':
                dec.record=MDSplus.TdiCompile('codac_parlog($)',(node,))
            elif dec.getNodeName()=='$CFGLOG':
                dec.record=MDSplus.TdiCompile('codac_cfglog($)',(node,))
            elif dec.usage=='SIGNAL':
                dec.record=MDSplus.TdiCompile('codac_signal($)',(dec,))
            setRecord(dec,False)

import MDSplus
tree = MDSplus.Tree('ARCHIVESB',-1)
setRecord(tree.getNode('\TOP'))