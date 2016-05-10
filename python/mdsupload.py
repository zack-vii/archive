"""
archive.MDSupload
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus as _mds
import numpy as _np
import re as _re
import time as _time
from . import base as _b
from . import diff as _diff
from . import interface as _if
from . import support as _sup
from . import version as _ver
from . import process as _prc
_MDS_shotdb_arc = '/ArchiveDB/raw/W7X/MDSplus/Shots'  # raw/W7X/MDSplus/Shots
_MDS_shotrt_arc = '/ArchiveDB/raw/W7X'  # raw/W7X
_MDS_shotdb = '/Test/raw/W7X/MDSplus/Shots'  # raw/W7X/MDSplus/Shots
_MDS_shotrt = '/Test/raw/W7X'  # raw/W7X
_treename  = 'W7X'
_subtrees  = 'included'
_exclude   = {'usage':['ACTION', 'TASK', 'SIGNAL']}
_pool = []
_threads = False


def uploadSection(shotfrom,shotupto,kks,section,pool=0,force=False,prefix=''):
    import socket
    if socket.gethostname()=='mds-data-1':
        from MDSplus import TdiExecute
        shots = TdiExecute('getShotDb("W7X",3)').data()
    else:
        from archive.support import getShotDB
        shots = getShotDB(3)
    tt = _time.time()
    if pool:
        if isinstance(pool,int):
            startPool(pool)
        else:
            startPool(len(Section((kks,-1,section)).getDevices()))
    try:
        for shot in shots:
            if shot<shotfrom: continue
            if shot>shotupto: break
            t = _time.time()
            try:
                S = Section((shot,kks,section))
            except _mds.mdsExceptions.TreeNNF:
                print('shot %d: %s in %s not found' % (shot,section,kks))
                continue
            print('shot %d' % (shot,))
            D = [(d.toParams(),force) for d in S.getDevices()]
            log = map(_uploadDev,D)
            ti = _time.time()
            print(log)
            print('shot %d in %.1f sec - total %.f' % (shot, ti-t, ti-tt))
    finally:
        stopPool()


def startPool(num):
    if not _pool:
        num = min(num,_prc.cpu_count()-1)
        _pool.append(_prc.Pool(num))

def stopPool():
    while _pool:
        _pool[-1].close()
        try:
            _pool[-1].join(3)
        except:
            _pool[-1].terminate()
            _pool[-1].join()
        _pool.remove(_pool[-1])

def write_data(pth,dat,dim,**kwarg):
    try:
        if _threads:
            return _if.write_data_async(pth,dat.T,dim,**kwarg) # time,height,width -> width,height,time
        if 'name' in kwarg.keys():
            del(kwarg['name'])
        return _if.write_data(pth,dat.T,dim,**kwarg) # time,height,width -> width,height,time
    except KeyboardInterrupt as ki: raise ki
    except Exception as exc:  return _sup.requeststr(exc)

def write_logurl(url,log,T0,join=None):
    try:
        if _threads:
            return _prc.Worker(join).put(_if.write_logurl,url,log,T0, timeout=5, retry=9)
        return _if.write_logurl(url,log,T0, timeout=5, retry=9)
    except KeyboardInterrupt as ki: raise ki
    except Exception as exc:  return _sup.requeststr(exc)

def setupTiming(version=0):
    """sets up the parlog of the shots datastream in the web archive
    should be executed only once before frist experiment"""
    def chanDesc(n):
        if n==0:    return {'name':'shot','physicalQuantity':{'type':'none'}}
        else:       return {'name':'T%d' % n,'physicalQuantity':{'type':'ns'}}
    parlog = {'chanDescs':[chanDesc(n) for n in _ver.xrange(7)]}
    result = _if.write_logurl(_MDS_shotdb.url_parlog(), parlog, version)
    print(result.msg)
    return result

def uploadModel(shot, subtrees=_subtrees, T0=None):
    """uploads full model tree of a given shot into the web archive
    should be executed right after T0"""
    if shot<0:
        raise Exception("Shot number must be positive (must not direct to the model).")
    if isinstance(subtrees,_ver.basestring):
        if subtrees=='included':    subtrees = [str(st.node_name) for st in _sup.getIncluded(_treename,-1)]
        elif subtrees=='all':       subtrees = [str(st.node_name) for st in _sup.getSubTrees(_treename,-1)]
        else:                       subtrees = [subtrees]
    def getModel():
        nodenames = ['ADMIN','TIMING']+subtrees
        w7x = _mds.Tree(_treename,-1)
        model = {}
        for key in nodenames:
            print('reading %s' % key)
            model[key]=_diff.treeToDict(w7x.getNode(key))
        return model
    if T0 is None:  T0 = _sup.getTiming(shot, 0)[0]
    else:           T0 = _b.Time(T0)
    cfglog = getModel()
    result = (_MDS_shotdb.url_cfglog(), cfglog, T0)
    result = _if.write_logurl(_MDS_shotdb.url_cfglog(), cfglog, T0, timeout=10, retry=3)
    print(result.msg)
    return result,cfglog

def uploadTiming(shot):
    """uploads the timing of a given shot into the web archive
    should be executed soon after T6"""
    data = _np.int64((t[0] for t in _sup.getTiming(shot)))
    dim = data[0]
    if dim<0:   raise Exception('T0 must not be turned off.')
    data[0] = int(shot)
    result = _if.write_data(_MDS_shotdb, data, dim, timeout=3, retry=3)
    print(result.msg)
    return result

def uploadShot(shot, subtrees=_subtrees, T0=None, T1=None, force=False, prefix=''):
    """uploads the data of all sections of a given shot into the web archive
    should be executed after all data is written to the shot file"""
    if shot<0:  raise Exception("Shot number must be positive (must not direct to the model).")
    S = Shot(shot, T0=T0, T1=T1, prefix=prefix)
    return S.upload(subtrees, force=force)

def _uploadSub(args):
    sub,param = args
    shot,T0,T1,prefix,force = param
    try:
        subtree = SubTree((shot,sub),T0=T0,T1=T1,prefix=prefix)
        return (sub,subtree.upload(force=force))
    except KeyboardInterrupt as ki: raise ki
    except _mds.mdsExceptions.TreeNNF: return (sub,'not included')
    except: return (sub,_sup.error())

def _uploadSec(param):
    sec_params,force = param
    section = Section.fromParams(sec_params)
    sec = section.path
    try:
        return (sec,section.upload(force=force))
    except KeyboardInterrupt as ki: raise ki
    except: return (sec,_sup.error())

def _uploadDev(params):
    dev_params,force = params
    device = Device.fromParams(dev_params)
    sec = device.section.path
    dev = device.path
    try:
        return (sec,dev,device.upload(force=force))
    except KeyboardInterrupt as ki: raise ki
    except: return (sec,_sup.error())

class Shot(_mds.Tree):
    """
    Shot(shot, T0=None, T1=None, prefix='')
    """
    @staticmethod
    def fromParams(params):
        shot,T0,T1,prefix = params[0:4]
        return Shot(shot,T0=T0,T1=T1,prefix=prefix)
    def toParams(self):
        return (self.shot,self.T0,self.T1,self.prefix)

    _index=0
    def __init__(self, shot, T0=None, T1=None, prefix=''):
        super(Shot,self).__init__(_treename, shot, "Readonly")
        self._name = 'Shot-'+str(Shot._index)
        Shot._index+=1
        self.T0 = _sup.getTiming(shot, 0)[0] if T0 is None else _b.Time(T0)
        self.T1 = _sup.getTiming(shot, 1)[0] if T1 is None else _b.Time(T1)
        self.prefix = prefix;

    def getSubTrees(self, subtrees=_subtrees):
        subtrees = self._getSubTreeList(subtrees)
        subs = []
        for subtree in subtrees:
            try:
                subs.append(SubTree((subtree,self), T0=self.T0, T1=self.T1, prefix=self.prefix))
            except _mds.mdsExceptions.treeshrExceptions.TreeNNF:
                _sup.debug('Node not found: %s'%(subtree,))
        return subs

    def _getSubTreeList(self,subtrees):
        if isinstance(subtrees,(dict,)):
            exclude = subtrees.get('exclude',[])
            if isinstance(exclude, _ver.basestring):
                exclude = [exclude]
            subtrees = subtrees.get('include',_subtrees)
        else:
            exclude = []
        if isinstance(subtrees,_ver.basestring):
            if subtrees=='included':    subtrees = [str(st.node_name) for st in _sup.getIncluded(self.tree,self.shot)]
            elif subtrees=='all':       subtrees = [str(st.node_name) for st in _sup.getSubTrees(self.tree,self.shot)]
            else:                       subtrees = [subtrees]
        subtrees = [sub for sub in subtrees if sub not in exclude]
        return subtrees

    def getSections(self, subtrees=_subtrees):
        secs = []
        for subtree in self.getSubTrees(subtrees):
            try:
                secs+= subtree.getSections()
            except _mds.mdsExceptions.treeshrExceptions.TreeNNF:
                _sup.debug('Node not found: %s'%(subtree,))
        return secs

    def getSectionNids(self, subtrees=_subtrees):
        nids = []
        for subtree in self.getSubTrees(subtrees):
            try:
                nids+= subtree.getSectionNids()
            except _mds.mdsExceptions.treeshrExceptions.TreeNNF:
                _sup.debug('Node not found: %s'%(subtree,))
        return nids


    def getDevices(self, subtrees=_subtrees):
        devs = []
        for subtree in self.getSubTrees(subtrees):
            try:
                devs+= subtree.getDevices()
            except _mds.mdsExceptions.treeshrExceptions.TreeNNF:
                _sup.debug('Node not found: %s'%(subtree,))
        return devs

    def uploadPoolSec(self, subtrees=_subtrees, force=False, excludeSec=[]):
        param = [((self.shot,s,self.T0,self.T1,self.prefix),force) for s in self.getSectionNids(subtrees) if s.nid not in excludeSec]
        if _pool:
            log = _pool[-1].pool.map_async(_uploadSec,param).get(1<<31)
        else:
                log = [_uploadSec(p) for p in param]
        return log

    def upload(self, subtrees=_subtrees, force=False):
        log = {}
        for sub in self.getSubTrees(subtrees):
            log[sub.node_name] = sub.upload(force)
        return log

    def uploadPoolDev(self, subtrees=_subtrees, force=False, excludeDev=[]):
        param = [(d.toParams(),force) for d in self.getDevices(subtrees) if d.nid not in excludeDev]
        if _pool:
            log = _pool[-1].map_async(_uploadDev,param).get(1<<31)
        else:
            log = [_uploadDev(p) for p in param]
        clog = [(sec.path,sec.writeCfgLog()) for sec in self.getSections(subtrees)]
        return log,clog

    def join(self):
        _prc.join(self._name)

class SubTree(_mds.TreeNode):
    """
    SubTree(subtree_tree, T0=None, T1=None, prefix='')
    SubTree(subtree_node, T0=None, T1=None, prefix='')
    SubTree((shot,kks), T0=None, T1=None, prefix='')
    """
    _index=0
    def __new__(cls, subtree, *a, **kv):
        if not isinstance(subtree,(_mds.TreeNode,)):
            if isinstance(subtree,(_mds.Tree,)):
                subtree = (subtree.shot,subtree.name)
            subtree = _mds.Tree(_treename,subtree[0],'Readonly').getNode("\%s::TOP"%(subtree[1],))
        return super(SubTree,cls).__new__(cls,subtree.nid,subtree.tree)

    def __init__(self, subtree, T0=None, T1=None, prefix=''):
        self.name = 'SubTree-'+str(SubTree._index)
        SubTree._index+=1
        self.T0 = _sup.getTiming(self.tree.shot, 0)[0] if T0 is None else _b.Time(T0)
        self.T1 = _sup.getTiming(self.tree.shot, 1)[0] if T1 is None else _b.Time(T1)
        self.prefix = prefix;

    def getSections(self):
        secs = []
        data = self.DATA
        for sec in data.getDescendants():
            secs.append(Section(sec, T0=self.T0, T1=self.T1, prefix=self.prefix))
        return secs

    def getSectionNids(self):
        return [node.nid for node in self.DATA.getDescendants()]

    def getDevices(self, subtrees=_subtrees):
        devs = []
        for section in self.getSections():
            devs+= section.getDevices()
        return devs

    def upload(self, force=False,join=None):
        if join is None: join = self.name
        log = []
        for sec in self.getSections():
            log.append(sec.upload(force=force,join=join))
        if join==self.name: self.join()
        return log

    def join(self):
        _prc.join(self.name)

class Section(_mds.TreeNode):
    """
    Section(section_node, T0=None, T1=None, prefix='')
    Section((shot,nid), T0=None, T1=None, prefix='')
    Section((shot,path), T0=None, T1=None, prefix='')
    Section((shot,kks,section), T0=None, T1=None, prefix='')
    Section((shot,kks,section_idx), T0=None, T1=None, prefix='')
    """
    @staticmethod
    def fromParams(params):
        shot,secnid,T0,T1,prefix = params[0:5]
        return Section((shot,secnid),T0=T0,T1=T1,prefix=prefix)
    def toParams(self):
        return (self.tree.shot,self.nid,self.T0,self.T1,self.prefix)

    _index=0
    def __new__(cls, section, *a, **kv):
        if not isinstance(section,(_mds.TreeNode)):
            tree = _mds.Tree(_treename, section[0], "Readonly")
            if len(section)<3:
                section = tree.getNode(section[1])
            else:
                data = tree.getNode("\%s::TOP"%(section[1],)).DATA
                if isinstance(section[2], int):
                    section = data.getDescendants()[section[2]]
                else:
                    section = data.getNode(section[2])
        return super(Section,cls).__new__(cls,section.nid,section.tree)

    def __init__(self, section, T0=None, T1=None,prefix=''):
        self.prefix = prefix
        self.kks = self.getParent().getParent()
        self.name = 'Section-'+str(Section._index)
        Section._index += 1
        self.T0 = _sup.getTiming(self.tree.shot, 0)[0] if T0 is None else _b.Time(T0)
        self.T1 = _sup.getTiming(self.tree.shot, 1)[0] if T1 is None else _b.Time(T1)
        self.address = _b.Path(_MDS_shotrt_arc if prefix=='' else _MDS_shotrt)
        self.address.streamgroup = prefix + self.kks.node_name.upper() + '_'+getDataName(self)

    def getDevices(self):
        HW = self.kks.HARDWARE
        if HW.getNumDescendants()==0: return
        devices = []
        for devnid,channels in self.channeldict.items():
            if len(channels)==0: continue
            devices.append(Device(devnid,channels,self))
        return devices

    def upload(self,force=False,join=None):
        if join is None: join = self.name
        print(self.tree,self,self.T0)
        Tx = self.CfgLogUpto()
        if Tx<self.T0 or force:
            logs = self.uploadDevices(force=force,join=join)
            logc = self.writeCfgLog(Tx,join)
            log={'logs': logs, "logc": logc}
        else:
            log = 'cfglog already written - skipped'
        if join==self.name: self.join()
        return log

    def uploadDevices(self,force=False,join=None):
        """generates the parlog for a section under .DATA and upload the data"""
        """signalDict:   (nid of channel) :{dict of signal}"""
        """channelLists: (nid of device):[nid of channels]"""
        if join is None: join = self.name
        log = []
        HW = self.kks.HARDWARE
        if HW.getNumDescendants()==0: return
        for devnid,channels in self.channeldict.items():
            device = Device(devnid,channels,self)
            if len(channels)==0:
                log.append((str(device),"no channels"))
                continue
            try:   log.append((str(device),device.upload(force=force,join=join)))
            except KeyboardInterrupt as ki: raise ki
            except:log.append((str(device),_sup.error()))
        if join==self.name: self.join()
        return log

    def join(self):
        _prc.join(self.name)

    def _getKksLog(self):
        """generates the base cfglog of a kks subtree called by uploadShot"""
        if not self.__dict__.has_key('_kkslog'):
            kkslog = {}
            for m in self.kks.getMembers():
                kkslog  = _sup.treeToDict(m, kkslog, _exclude)
            for c in self.kks.getChildren():
                if not c.getNodeName() in ['HARDWARE','DATA']:
                    kkslog = _sup.treeToDict(c, kkslog, _exclude)
            self._kkslog = kkslog
        return self._kkslog.copy()
    kkslog = property(_getKksLog)

    def _getCfgLog(self):
        if not self.__dict__.has_key('_cfglog'):
            self._cfglog = _sup.treeToDict(self,self.kkslog,_exclude,'')
        return self._cfglog.copy()
    cfglog = property(_getCfgLog)

    def CfgLogUpto(self):
        return checkLogUpto(self.address.cfglog,self.T0)

    def writeCfgLog(self,Tx=None,join=None):
        if Tx is None: Tx = self.CfgLogUpto()
        if Tx<self.T0:
            if _sup.debuglevel>=3: print(('write_cfglog',self.address.cfglog,self.cfglog))
            try:
                return write_logurl(self.address.cfglog, self.cfglog, self.T0, join=join)
            except KeyboardInterrupt as ki: raise ki
            except Exception as exc:
                print(exc)
                return exc
        else:
            return 'cfglog already written'

    def _getSignalDict(self):
        if not self.__dict__.has_key('_signaldict'):
            def _signaldict(signal, signaldict, prefix=[]):
                """collects the properties of a signal node and assiciates it with a channel nid"""
                nameList = prefix+[getDataName(signal)]
                if signal.usage == "SIGNAL":
                    try:
                        desc = {}
                        if signal.getNumDescendants()>0: desc = _sup.treeToDict(signal, desc, _exclude,'')
                        desc["name"] = '_'.join(nameList)
                        rec = signal.record
                        nid = extractNid(rec)
                        signaldict[nid] = desc
                    except AttributeError as exc:
                        print('_getSignalDict',exc,signal.record)
                    except: print(_sup.error(0))
                else:
                    for sig in signal.getDescendants():
                        signaldict = _signaldict(sig, signaldict, nameList)
                return signaldict
            signaldict = {}
            for signal in self.getDescendants():
                signaldict = _signaldict(signal, signaldict)
            self._signaldict = signaldict
        return self._signaldict.copy()
    signaldict = property(_getSignalDict)

    def _getChannelDict(self):
        if not self.__dict__.has_key('_channellists'):
            """collects the channel lists of all devices"""
            f = _re.compile('(?<=\.HARDWARE[:\.])([^\.:]+)')
            HW = self.kks.HARDWARE
            channeldict = dict([device.Nid, []] for device in HW.getDescendants())
            for channel in self.signaldict:
                deviceName = f.search(str(_mds.TreeNode(channel).fullpath)).group(0)
                device = HW.getNode(deviceName)
                channeldict[device.nid].append(channel)
            self._channeldict = channeldict
        return self._channeldict.copy()
    channeldict = property(_getChannelDict)

class Device(_mds.TreeNode):
    """
    Device(devnid, channels, section)
    """
    @staticmethod
    def fromParams(params):
        devnid,channels,sec_params = params[0:3]
        return Device(devnid,channels,Section.fromParams(sec_params))
    def toParams(self):
        return (self.nid,self.channels,self.section.toParams())

    _index=0
    def __new__(cls,devnid,channels,section):
        return super(Device,cls).__new__(cls,devnid,section.tree)
    def __init__(self,devnid,channels,section):
        super(Device,self).__init__(devnid,section.tree)
        self.name = 'Device-'+str(Device._index)
        Device._index+=1
        self.address = _b.Path(section.address.path())
        self.address.stream = getDataName(self)
        self.channels = channels
        self.section = section
    def upload(self, force=False,join=None):
        if join is None: join = self.name
        Tx = self.ParLogUpto()
        T0 = self.section.T0
        if Tx<T0 or force:
            log = {}
            if len(self.signals)==1:
                return self._write_signal(Tx,force=force,join=join)
            scalars,images = self.sortedSignals()
            log['scalars']=self._write_scalars(scalars,Tx,force=force,join=join)
            log['images']=self._write_images(images,force=force,join=join)
        else:
            log = 'parlog exists '+str(Tx)+' : '+str(T0)
        if join==self.name: self.join()
        return log

    def join(self):
        _prc.join(self.name)

    def ParLogUpto(self):
        return checkLogUpto(self.address.parlog,self.section.T0)

    def sortedSignals(self):
        if _sup.debuglevel>=2: print('%d signals' % len(self.signals))
        scalar = [[],None,[]]
        images = []
        for i,signal in enumerate(self.signals):
            if _sup.debuglevel>=3: print('signal: %s' % str(signal))
            try:
                signal = signal.evaluate()
            except _mds.mdsExceptions.TreeNODATA:
                continue
            try:
                units = _b.Units(signal, 1)
                if units != 'unknown':
                    self.chandescs[i]["physicalQuantity"]['type'] = units
            except KeyboardInterrupt as ki: raise ki
            except: pass
            sigdata = signal.data() # time,*
            ndims = len(sigdata.shape)
            sigdimof = _b.dimof2w7x(signal.dim_of(),self.section.T1)
            if ndims==1:
                scalar[0].append(sigdata) #  channels,time
                scalar[2].append(self.chandescs[i])
                if scalar[1] is None:
                    scalar[1] = sigdimof
                else:
                    if not (scalar[0][0].dtype==scalar[0][-1].dtype):
                        raise(Exception('data types are not equal for all channels'))
                    # if not (len(scalar[1])==len(sigdimof) and scalar[1][0]==sigdimof[0] and scalar[1][-1]==sigdimof[-1]):
                    if not all(scalar[1]==sigdimof):
                        raise(Exception('dimensions are not equal for all channels'))
            elif ndims>1:
                images.append((sigdata,sigdimof,self.chandescs[i]))
        return scalar,images

    def writeParLog(self,path,Tx=None,join=None):
        if Tx is None: Tx = self.ParLogUpto()
        T0 = self.section.T0
        if Tx<T0:
            if _sup.debuglevel>=4: print(('write_parlog',path.parlog, self.parlog))
            return write_logurl(path.parlog, self.parlog, T0, join=join)
        return "parlog already written"


    def _write_signal(self,Tx=None,force=False,join=None):
        """prepares signal and uploads to webarchive"""
        if Tx is None: Tx = self.ParLogUpto()
        T0 = self.section.T0
        T1 = self.section.T1
        if Tx<T0 or force:
            signal = self.signals[0]
            if _sup.debuglevel>=2: print('one signal',signal)
            nSeg = signal.getNumSegments()
            if nSeg>0:
                url = getCheckURL_seg(signal,T1,self.address)
                logs = []
                if _sup.debuglevel>=2: print('is segmented',nSeg)
                for segment in _ver.xrange(nSeg):
                    try:
                        url = checkURL_seg(signal,T1,url,segment)
                    except:
                        logs.append({"segment": segment, "log": "already presend"})
                        continue
                    seg = signal.getSegment(segment)
                    dimof =  _b.dimof2w7x(seg.dim_of(),T1)
                    data = seg.data()
                    if _sup.debuglevel>=2: print(('image',self.address, data.shape, data.dtype, dimof.shape, dimof[0], T1))
                    log = write_data(self.address, data, dimof, one=True,name=join, timeout=10, retry=9) # time,*
                    logs.append({"segment": segment, "log": log})
            else:
                if _sup.debuglevel>=3: print('is not segmented')
                try:     data = signal.data()
                except _mds.mdsExceptions.TreeNODATA: return 'nodata'
                except: return {'signal',_sup.error()}
                dimof = _b.dimof2w7x(signal.dim_of(),T1)
                logs = write_data(self.address, data, dimof, one=True,name=join, timeout=10, retry=9) # time,height,width
            logp = self.writeParLog(self.address,Tx,join)
            if join is None: _prc.join()
            return {'signal':logs,'parlog':logp,"path":self.address.path()}
        return {'signal':'already there','parlog':'already there',"path":self.address.path()}

    def _write_scalars(self, scalars, Tx=None, force=False, join=None):
        if Tx is None: Tx = self.ParLogUpto()
        T0 = self.section.T0
        if scalars[1] is None:
            return  {'signal':'empty','parlog':'empty',"path":self.address.path()}
        if Tx<T0 or force:
            data = _np.array(scalars[0]).T # channels,time -> time,channels
            dimof = _np.array(scalars[1])
            url = getCheckURL_arr(dimof,self.address)
            length= len(dimof)
            idx = 0;logs=[]
            while idx<length:
                N = 1000000 if length-idx>1100000 else length-idx
                dim  = dimof[idx:idx+N]
                try:
                    url = checkURL_arr(dim,url)
                    logs.append(write_data(self.address, data[idx:idx+N], dim,name=join, timeout=10, retry=9)) #  time,channels
                except:
                    logs.append({"idx": idx, "log": "already presend"})
                idx += N
                _sup.debug(idx)
            logp = self.writeParLog(self.address,Tx,join)
            if join is None: _prc.join()
            return {'signal':logs,'parlog':logp,"path":self.address.path()}
        return {'signal':'already there','parlog':'already there',"path":self.address.path()}

    def _write_images(self, images, force=False, join=None):
        logs=[];logp=[];paths=[]
        for image in images:
            imagepath = _b.Path(self.address.path())
            imagepath.stream = self.address.stream+"_"+image[2]['name'].split('_',2)[1];
            T0 = self.section.T0
            T1 = self.section.T1
            Tx = checkLogUpto(imagepath.parlog,T0)
            paths.append(imagepath.path())
            if Tx<T0 or force:
                data = _np.array(image[0])
                dimof = _np.array(image[1])
                print(data.shape,dimof.shape)
                if _sup.debuglevel>=3: print(('image',imagepath, data.shape, data.dtype, dimof.shape, dimof[0], T1))
                logs.append(write_data(imagepath, data, dimof,name=join, timeout=10, retry=9)) # time,height,width
                logp.append(self.writeParLog(imagepath,Tx,join))
            else:
                logs.append('already there')
                logp.append('already there')
        if join is None: _prc.join()
        return {'signal':logs,'parlog':logp,"path":paths}

    def _getAllSignals(self):
        if not self.__dict__.has_key('_allsignals'):
            def _searchSignals(node):
                """checks DEVICE:SIGNAL and DEVICE.STRUCTURE:SIGNAL"""
                signals = []
                for descendant in node.getDescendants():
                    if descendant.usage == "SIGNAL":
                        signals.append([descendant,descendant])
                    else:
                        sigs = _searchSignals(descendant)
                        signals+= sigs
                if len(signals)==1: signals[0][0] = node
                return signals
            self._allsignals = _searchSignals(self)
        return list(self._allsignals)
    allsignals = property(_getAllSignals)

    def _getDeviceDict(self):
        """collects the data of channels and their parenting device"""
        if not self.__dict__.has_key('_devicedict'):
            signals = []
            chandescs = []
            for signal in self.allsignals:
                if signal[1].nid in self.channels:  # add to signal list
                    chandescs.append(self._chanDesc(signal))
                    signals.append(signal[1])
            exclude = _exclude.copy()
            exclude['nid'] = [s[1].nid for s in self.allsignals]
            self._chandescs = chandescs
            self._signals = signals
            self._devicedict = _sup.treeToDict(self,{},exclude,'')
        return self._devicedict.copy()
    devicedict = property(_getDeviceDict)

    def _getSignals(self):
        if not self.__dict__.has_key('_signals'):
            self._getDeviceDict()
        return list(self._signals)
    signals = property(_getSignals)

    def _getChanDescs(self):
        if not self.__dict__.has_key('_chandescs'):
            self._getDeviceDict()
        return list(self._chandescs)
    chandescs = property(_getChanDescs)

    def _getParlog(self):
        if not self.__dict__.has_key('_parlog'):
            parlog = self.devicedict
            parlog['chanDescs'] = self.chandescs
            self._parlog = parlog
        return self._parlog.copy()
    parlog = property(_getParlog)

    def _getMergeDict(self):
        return self.getMergeDict()

    def _chanDesc(self,signalset):
        """generates the channel descriptor for a given channel"""
        def _channelDict(signalroot, nid):
            """collects the parameters of a channel"""
            channelDict = {}
            channelDict['name'] = getDataName(signalroot)
            channelDict["active"] = int(signalroot.isOn())
            channelDict["physicalQuantity"] = {'type': 'unknown'}
            if not self.nid==signalroot.nid:  # in case: DEVICE.STRUCTURE:SIGNAL
                for sibling in signalroot.getDescendants():
                    if not sibling.Nid == nid:
                        channelDict = _sup.treeToDict(sibling, channelDict, _exclude)
            return channelDict
        def mergeSignalDict(chanDesc):
            if self.section.signaldict.has_key(nid):
                for key, value in self.section.signaldict[nid].items():
                    chanDesc[key] = value
            return chanDesc
        def substituteUnits():
            if 'units' in chanDesc.keys():
                chanDesc["physicalQuantity"]['type'] = _b.Units(chanDesc["units"], 1)
                del(chanDesc["units"])
            return chanDesc

        signal = signalset[1]
        print(signal)
        nid = signal.nid
        chanDesc = _channelDict(signalset[0], nid)
        chanDesc = mergeSignalDict(chanDesc)
        chanDesc = substituteUnits()
        return chanDesc

def getDataName(datanode):
    """converts 'ABCD_12XY_II' to 'Abcd_12Xy_II"""
    def process(i):
        if all(c=='I' for c in i): return i
        else: return i[0]+i[1:].lower()
    if isinstance(datanode,_mds.TreeNode):
        datanode = str(datanode.node_name)
    f = _re.findall('([^A-Z]*)([A-Z]*)',datanode)
    return ''.join([i[0]+process(i[1]) for i in f])

def checkLogUpto(path,Tfrom):
    try:
        filterstop = 2000000000000000000
        filterstart = filterstop-1 #Tfrom.ns-1
        p = _if.get_json(path,time=[filterstart,filterstop],Nsamples=2,timeout=3,retry=9)
        t = p['dimensions']
        return t[0]
    except KeyboardInterrupt as ki: raise ki
    except: return 0

def extractNid(obj):
    if isinstance(obj,(_mds.TreeNode)):
        if obj.usage == 'SIGNAL':
            return obj.nid
    elif isinstance(obj,(_mds.tdibuiltins.EXT_FUNCTION)):
        for arg in obj.args:
            nid = extractNid(arg)
            if nid is not None:
                return nid

def getCheckURL_seg(signal,T1,path):
    tstart = _b.dimof2w7x(signal.getSegmentEnd(0),T1)
    tend   = _b.dimof2w7x(signal.getSegmentEnd(signal.getNumSegments()-1),T1)
    try:
        return str(_if.get_json('%s/?filterstart=%d&filterstop=%d'%(path.url_datastream(),tstart,tend), timeout=3, retry=9)['_links']['children'][0]['href'].split('?')[0])
    except:
        return None

def checkURL_seg(signal,T1,url,segment):
    if url is None: return
    tend = _b.dimof2w7x(signal.getSegmentEnd(segment),T1)
    try:
        _if.get_json('%s?filterstart=%d&filterstop=%d'%(url,tend-500,tend+5000), timeout=3, retry=9)
    except:
        return None
    raise Exception()

def getCheckURL_arr(dim,path):
    tstart = dim[0]
    tend   = dim[-1]
    try:
        return str(_if.get_json('%s/?filterstart=%d&filterstop=%d'%(path.url_datastream(),tstart,tend), timeout=3, retry=9)['_links']['children'][0]['href'].split('?')[0])
    except:
        return None

def checkURL_arr(dim,url):
    if url is None: return
    tend = dim[-1]
    try:
        _if.get_json('%s?filterstart=%d&filterstop=%d'%(url,tend-500,tend+5000), timeout=3, retry=9)
    except:
        return None
    raise Exception()
