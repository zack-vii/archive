"""
Created on Fri Sep 18 11:15:21 2015
transient
@author: Cloud
"""
import MDSplus as _mds
import time as _time
from . import base as _base
from . import diff as _diff
from . import support as _sup
from . import mdsupload as _mdsup
from . import interface as _if
from . import version as _ver


class client(_mds.Connection):
    mds = _mds
    tree = 'transient'

    def __init__(self, stream, hostspec='localhost'):
        _mds.Connection.__init__(self, hostspec)
        self.openTree(self.tree, -1)
        self.stream = stream
        self.addNode()

    def tcl(self, command, *args):
        cmd = 'TCL($'
        for arg in args:
            cmd += '//" "//$'
        cmd += ')'
        expr = str(_mds.TdiCompile(cmd, tuple([command]+list(args))))
        status = self.get(expr)
        print(type(status))
        if not (status & 1):
            raise _mds._treeshr.TreeException("Error executing tcl expression: %s" % (_mds._mdsshr.MdsGetMsg(status),))

    def addNode(self):
        try:
            self.tcl('EDIT', self.tree)
            try:
                self.tcl('ADD NODE', self.stream)
                self.tcl('WRITE')
            except:
                pass
        finally:
            self.tcl('CLOSE')

    def dir(self):
        self.tcl('SET TREE', self.tree)
        return self.tcl('DIR')


class server(object):
    tree = 'transient'
    _path = _base.Path("/Test/raw/W7X/MDS_transient/")
    mds = _mds

    def __init__(self):
        try:
            self.Tree()
        except:
            self.Tree(-1, 'new')
            self.createPulse(1)
            self.createPulse(2)
            self.setCurrent(1)
        self.current = self.getCurrent()
        self.last = (self.current % 2)+1

    def Tree(self, *args):
        return self.mds.Tree(self.tree, *args)

    def createPulse(self, shot):
        self.Tree().createPulse(shot)

    def getCurrent(self):
        return self.mds.Tree.getCurrent(self.tree)

    def setCurrent(self, shot):
        self.mds.Tree.setCurrent(self.tree, shot)
        self.current = shot

    def switch(self):
        self.last = self.current
        new = (self.current % 2)+1
        self.createPulse(new)
        self.setCurrent(new)
        return self.current

    def run(self):
        self.switch()
        for m in self.Tree(self.last).getNode('\TOP').getMembers():
            self.upload(m)

    def autorun(self, timing=60):
        while True:
            self.run()
            try:
                timeleft = timing - (_time.time() % timing)
                print(self.mds.Event.wfevent('transient', int(timeleft+.5)))
            except(_mds._mdsshr.MdsTimeout):
                print('no event: run on timer')

    def getTimeInserted(self, node, shot=-1):
        if isinstance(node, str):
            node = self.Tree(shot).getNode(node)
        return _sup.getTimeInserted(node)

    def getDict(self, node, shot=-1):
        if isinstance(node, str):
            node = self.Tree(shot).getNode(node)
        return _diff.treeToDict(node)

    def configupstream(self, nodename):
        dic = _if.read_parlog(self.getDataPath(nodename),'now')
        return dic['chanDescs'][0]

    def configtree(self, node):
        if isinstance(node, str):
            node = self.Tree().getNode(node)
        return _mdsup.SignalDict(node)

    def getParlogPath(self, nodename):
        return self.getDataPath(nodename).parlog

    def getDataPath(self, nodename):
        if not isinstance(nodename, str):
            nodename = str(nodename.getNodeName())
        return self._path.set_stream(nodename.lower())

    def config(self, node, t0='now'):
        if isinstance(node, str):
            node = self.Tree().getNode(node)
        chanDesc = _mdsup.SignalDict(node)
        parlog = {'chanDescs': [chanDesc]}
        try:
            _if.write_logurl(self.getParlogPath(node), parlog, t0)
        except:
            return _sup.error()

    def upload(self, node):
        if isinstance(node, str):
            node = self.Tree(self.last).getNode(node)
        path = self.getDataPath(node)
        for i in _ver.xrange(int(node.getNumSegments())):
            segi = node.getSegment(i)
            data = segi.data()
            dimof = segi.dim_of()
            while True:
                try:
                    #print(nodename+": ("+str(dimof.data())+", "+str(data)+")")
                    _if.write_data(path, data, dimof)
                    break
                except:
                    _sup.error()

    def clean(self,shot=-1):
        self.Tree(shot).cleanDatafile()
