"""
Created on Fri Sep 18 11:15:21 2015
transient
@author: Cloud
"""
import MDSplus as _mds
import numpy as _np
import time as _time
from . import base as _base
from . import diff as _diff
from . import support as _sup
from . import mdsupload as _mdsup
from . import interface as _if
from . import version as _ver


class client(object):
    mdsarray = _mds.mdsarray
    _tree = 'TRANSIENT'
    def __init__(self, stream, hostspec=_base._server):
        self._con = _mds.Connection(hostspec)
        if len(stream)>12:
            raise Exception('The name of the stream cannot be longer than 12 charaters. '+stream)
        self._stream = stream.upper()
        try:
            self._addNode()
            print('"'+self._stream+'" signal created.')
        except Exception as exc:
            sexc = str(exc)
            if sexc.startswith('%TREE-W-ALREADY_THERE'):
                print('"'+self._stream+'" signal found.')
            elif sexc.startswith('%TREE-E-FOPENW'):
                raise(Exception('"'+self._tree+'" tree not found.'))
            else:
                raise exc

    def __repr__(self):
        return '<'+self._tree+' client for signal '+self._stream+'>'

    def _tcl(self, command, *args):
        cmd = 'TCL($'+'//" "//$'*len(args)+', _output, _error)'
        expr = str(_mds.TdiCompile(cmd, tuple([command]+list(args))))
        status = self._con.get(expr).tolist()
        if (status & 1)==1:
            return self._con.get('_output')
        raise _mds.mdsExceptions.statusToException(status)

    def _addNode(self, node=None, usage='STRUCTURE'):
        if node is None:
            usage = 'SIGNAL'
            node = self._stream
        try:
            self._tcl('EDIT', self._tree)
            self._tcl('ADD NODE', node+'/usage='+usage)
            self._tcl('WRITE')
        finally:
            self._tcl('QUIT')

    def _delNode(self, node, confirm=False):
        try:
            self._tcl('EDIT', self._tree)
            prefix = node+'/CONFIRM' if confirm else node
            self._tcl('DEL NODE', prefix)
            self._tcl('WRITE')
        finally:
            self._tcl('QUIT')

    def _path(self):
        return '\TOP:'+self._stream

    def _clearConfig(self):
        self._con.openTree(self._tree, -1)
        child = map(str.rstrip,self._con.get('IF_ERROR(GETNCI(GETNCI($, "CHILDREN_NIDS"),"NODE_NAME"),[])', self._path()).tolist())
        for c in child:
            self._delNode(self._path()+'.'+c, True)
        self._con.openTree(self._tree, -1)
        memb = map(str.rstrip,self._con.get('IF_ERROR(GETNCI(GETNCI($, "MEMBER_NIDS"),"NODE_NAME"),[])', self._path()).tolist())
        for m in memb:
            if not m in ['UNITS', 'DESCRIPTION']:
                self._delNode(self._path()+':'+m)

    def _setConfig(self, dic):
        diff = _diff.DeepDiff(self.config, dic, case=False)
        for rem in diff.dict_rem:
            nodepath = '.'.join([self._path()]+rem)
            self._delNode(nodepath,True)
        self._addConfig(dic)

    def _addConfig(self, dic):
        def dicttotree(dic, path):
            def addnode(path, usage):
                try:
                    self._addNode(path, usage)
                    print('creating '+path.upper())
                except Exception as exc:
                    if not str(exc).startswith('%TREE-W-ALREADY_THERE'):
                        raise exc
                    print('updating '+path.upper())
            for k,v in dic.items():
                newpath = path+':'+k[0:12]
                if isinstance(v, dict):
                    addnode(newpath, 'STRUCTURE')
                    dicttotree(v, newpath)
                else:
                    v = _np.array(v)
                    if v.dtype.descr[0][1][1] in 'SU':
                        addnode(newpath, 'TEXT')
                    elif v.dtype.descr[0][1][1] in 'if':
                        addnode(newpath, 'NUMERIC')
                    else:
                        addnode(newpath, 'ANY')
                    self._con.openTree(self._tree, -1)
                    self._con.put(newpath,'$',v.tolist())
                    self._con.closeTree(self._tree, -1)
        dicttotree(dic, self._path())

    def _getConfig(self):
        self._con.openTree(self._tree, -1)
        def treetodict(path):
            dic = {}
            child = map(str.rstrip,self._con.get('IF_ERROR(GETNCI(GETNCI($, "CHILDREN_NIDS"),"NODE_NAME"),[])', path).tolist())
            for c in child:
                dic[c] = treetodict(path+'.'+c)
            memb = map(str.rstrip,self._con.get('IF_ERROR(GETNCI(GETNCI($, "MEMBER_NIDS"),"NODE_NAME"),[])', path).tolist())
            for m in memb:
                try:
                    dic[m] = self._con.get(path+':'+m).tolist()
                except Exception as exc:
                    sexc = str(exc)
                    if sexc.startswith('%TREE-E-NODATA'):
                        print(path+':'+m+' does not contain any data')
            return dic
        dic = treetodict(self._path())
        self._con.closeTree(self._tree, -1)
        return dic
    config = property(_getConfig, _setConfig)

    def notify(self, local=False):
        if local:
            _mds.Event.seteventRaw(self._tree, self.mdsarray.Uint8Array(list(map(ord,self._stream))))
        else:
            self._con.get('SETEVENT($,$)', self._tree, self.mdsarray.Uint8Array(list(map(ord,self._stream))))

    def _setUnits(self, units):
        self._addConfig({'UNITS': _base.Units(units)})

    def _getUnits(self):
        return self._con.get('IF_ERROR(EXECUTE($),"unknown")',self._path+':'+'UNITS').tolist()
    units = property(_getUnits, _setUnits)

    def _setDescription(self, description):
        """Set a description text (e.g for the title of a plot)
        setDescription(description)
        @description as str
        """
        self._addConfig({'DESCRIPTION': description})


    def setURL(self, url):
        """
        overrides the URL to be written to
        !No validity check will be applied!
        """
        self._addConfig({'$URL': url})


    def _getDescription(self):
        """Set a description text (e.g for the title of a plot)
        setDescription(description)
        @helptext as str
        """
        return self._con.get('IF_ERROR(EXECUTE($),"")',self._path+':'+'DESCRIPTION').tolist()
    description = property(_getDescription, _setDescription)

    def putFloat32(self, data, dim):
        return self.putData(self.mdsarray.Float32Array(data), dim)
    def putFloat64(self, data, dim):
        return self.putData(self.mdsarray.Float64Array(data), dim)
    def putUint8(self, data, dim):
        return self.putData(self.mdsarray.Uint8Array(data), dim)
    def putInt8(self, data, dim):
        return self.putData(self.mdsarray.Int8Array(data), dim)
    def putUint16(self, data, dim):
        return self.putData(self.mdsarray.Uint16Array(data), dim)
    def putInt16(self, data, dim):
        return self.putData(self.mdsarray.Int16Array(data), dim)
    def putUint32(self, data, dim):
        return self.putData(self.mdsarray.Uint32Array(data), dim)
    def putInt32(self, data, dim):
        return self.putData(self.mdsarray.Int32Array(data), dim)
    def putUint64(self, data, dim):
        return self.putData(self.mdsarray.Uint64Array(data), dim)
    def putInt64(self, data, dim):
        return self.putData(self.mdsarray.Int64Array(data), dim)

    def putData(self, data, dim):
        """Write a chunk of data to the TRANSIENT tree
        putData(data, dim)
        @data as any <mdsarray convertable>
        @dim  as [int/long] in ns -> Uint64Array
        """
        if not isinstance(data, self.mdsarray.Array):
            data = self.mdsarray.makeArray(data)
        if not isinstance(dim, self.mdsarray.Array):
            dim = self.mdsarray.makeArray(dim)
        dim = self.mdsarray.Uint64Array(list(map(lambda t: _base.Time(t).ns, dim)))
        end = len(dim)-1
        putexpr = 'makeSegment(Compile($),$,$,Build_Dim(*,$),$,-1,-1)'
        chkexpr = 'GetNumSegments(Compile($))'
        try:
            shot = 0;
            self._con.openTree(self._tree, shot)
            try:
                shot = self._con.get('$SHOT')
                status = self._con.get(putexpr, self._path(), dim[0], dim[end], dim, data).tolist();
                if (status & 1) == 0:
                    raise _mds.mdsExceptions.statusToException(status)
                segs = int(self._con.get(chkexpr, self._path()));
            finally:
                self._con.closeTree(self._tree, shot)
        except Exception as exc:
            print(self._tree+' tree not ready. Sending notification')
            self.notify()
            raise(exc)
        return("written: %d: %f (%d: %d)" % (dim[0], data[0], shot, segs))


class server(object):
    _tree = 'TRANSIENT'
    _path = _base.Path("raw/W7X/MDS_transient/")
    mds = _mds
    tdi = _mds.TdiExecute
    upload_as_block = True

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
        return _mds.Tree(self._tree, *args)

    def createPulse(self, shot):
        try:
            self.clean()
        except:
            pass
        self.Tree().createPulse(shot)

    def getCurrent(self):
        return _mds.Tree.getCurrent(self._tree)

    def setCurrent(self, shot):
        _mds.Tree.setCurrent(self._tree, shot)
        self.current = shot

    def switch(self):
        self.last = self.current
        new = (self.current % 2)+1
        self.createPulse(new)
        self.setCurrent(new)
        return self.current

    def ping(self, timeout=5):
        return self._path.ping(timeout)

    def autorun(self, timing=60):
        while True:
            try:
                self.run()
                try:
                    timeleft = int(timing - ((_time.time()+1) % timing)+1)
                    print('idle: waiting for event or timeout in '+str(timeleft)+'s')
                    result = _mds.Event.wfeventRaw(self._tree, timeleft)
                    print('event: '+''.join(map(chr,result)))
                except(_mds._mdsshr.MdsTimeout):
                    print('timeout: run on timer')
            except:
                _sup.error()

    def run(self):
        while not self.ping():
            print('web-archive unreachable: retrying')
        self.switch()
        for m in self.Tree(self.last).getNode('\TOP').getMembers():
            self.upload(m)

    def getTimeInserted(self, node, shot=-1):
        if isinstance(node, str):
            node = self.Tree(shot).getNode(node)
        return _sup.getTimeInserted(node)

    def getUpdateTime(self, node, shot=-1):
        if isinstance(node, str):
            node = self.Tree(shot).getNode(node)
        t = 0;
        for c in node.getChildren():
            t = max(t, self.getUpdateTime(c))
        for m in node.getMembers():
            t = max(t, self.getUpdateTime(m))
            t = max(t, self.getTimeInserted(m))
        return t

    def configUpstream(self, nodename):
        dic = _if.read_parlog(self.getDataPath(nodename),'now')
        return dic['chanDescs'][0]

    def configTree(self, node):
        if isinstance(node, str):
            node = self.Tree(self.last).getNode(node)
        return _mdsup._signalDict(node)

    def checkconfig(self, node):
        tdict = self.configTree(node)
        try:
            udict = self.configUpstream(node)
        except:
            udict = {}
        return _diff.DeepDiff(udict, tdict, case=False)

    def getParlogPath(self, nodename):
        return self.getDataPath(nodename).parlog

    def getDataPath(self, node):
        if isinstance(node, str):
            node = self.Tree(self.last).getNode(node)
        url = node.getNodeWild('$URL')
        if len(url):
            return _base.Path(str(url[0].data()))
        else:
            return self._path._set_stream(str(node.getNodeName()).lower())

    def config(self, node):
        if isinstance(node, str):
            node = self.Tree(self.last).getNode(node)
        chanDesc = self.configTree(node)
        t0 = self.getUpdateTime(node)
        parlog = {'chanDescs': [chanDesc]}
        try:
            _if.write_logurl(self.getParlogPath(node), parlog, t0)
        except Exception as exc:
            print(exc)

    def upload(self, node):
        if isinstance(node, str):
            node = self.Tree(self.last).getNode(node)
        if node.getNumSegments()>0:
            if len(self.checkconfig(node).all())>0:
                self.config(node)
            if self.upload_as_block:
                self._uploadBlock(node)
            else:
                self._uploadSegmented(node)

    def _uploadBlock(self, node):
        path = self.getDataPath(node)
        data = []
        dimof = []
        Nsamp = int(node.getNumSegments())
        for i in _ver.xrange(Nsamp):
            segi = node.getSegment(i)
            data += list(segi.data().tolist())
            dimof += list(segi.dim_of().data().tolist())
        for i in _ver.xrange(Nsamp):
            try:
                _if.write_data(path, data[i:], dimof[i:])
                break
            except Exception as exc:
                print(exc)

    def _uploadSegmented(self, node):
        path = self.getDataPath(node)
        for i in _ver.xrange(int(node.getNumSegments())):
            segi = node.getSegment(i)
            data = segi.data().tolist()
            dimof = segi.dim_of().data().tolist()
            for i in range(3):
                try:
                    _if.write_data(path, data, dimof)
                    break
                except Exception as exc:
                    print(exc)
                    _time.sleep(1)

    def clean(self,shot=-1):
        self.Tree(shot).cleanDatafile()
