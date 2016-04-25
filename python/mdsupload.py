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
from . import base as _base
from . import diff as _diff
from . import interface as _if
from . import support as _sup
from . import version as _ver
from . import process as _prc
_MDS_shotdb = '/Test/raw/W7X/MDSplus/Shots'  # raw/W7X/MDSplus/Shots
_MDS_shotrt = '/Test/raw/W7X'  # raw/W7X
_treename  = 'W7X'
_subtrees  = 'included'
_exclude   = {'usage':['ACTION', 'TASK', 'SIGNAL']}
in_pool=not(__name__=='__main__')

def write_data(*args,**kwarg):
    try:
        if in_pool:
            if 'name' in kwarg.keys():
                del(kwarg['name'])
            return _if.write_data(*args,**kwarg)
        return _if.write_data_async(*args,**kwarg)
    except KeyboardInterrupt as ki: raise ki
    except Exception as exc:  return _sup.requeststr(exc)

def write_logurl(url,log,T0,join=None):
    try:
        if in_pool:
            return _if.write_logurl(url,log,T0)
        return _prc.Worker(join).put(_if.write_logurl,url,log,T0)
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
    else:           T0 = _base.Time(T0)
    cfglog = getModel()
    result = (_MDS_shotdb.url_cfglog(), cfglog, T0)
    result = _if.write_logurl(_MDS_shotdb.url_cfglog(), cfglog, T0)
    print(result.msg)
    return result,cfglog

def uploadTiming(shot):
    """uploads the timing of a given shot into the web archive
    should be executed soon after T6"""
    data = _np.int64((t[0] for t in _sup.getTiming(shot)))
    dim = data[0]
    if dim<0:   raise Exception('T0 must not be turned off.')
    data[0] = int(shot)
    result = _if.write_data(_MDS_shotdb, data, dim)
    print(result.msg)
    return result

def uploadShot(shot, subtrees=_subtrees, T0=None, T1=None, force=False, prefix=''):
    """uploads the data of all sections of a given shot into the web archive
    should be executed after all data is written to the shot file"""
    if shot<0:  raise Exception("Shot number must be positive (must not direct to the model).")
    S = Shot(shot, T0=T0, T1=T1, prefix=prefix)
    return S.upload(subtrees, force=force)

class Shot(_mds.Tree):
    _index=0
    def __init__(self, shot, T0=None, T1=None, prefix=''):
        super(Shot,self).__init__(_treename, shot, "Readonly")
        self._name = 'Shot-'+str(Shot._index)
        Shot._index+=1
        self.T0 = _sup.getTiming(shot, 0)[0] if T0 is None else _base.Time(T0)
        self.T1 = _sup.getTiming(shot, 1)[0] if T1 is None else _base.Time(T1)
        self.prefix = prefix;

    def getSubTrees(self, subtrees=_subtrees):
        subtrees = self._getSubTreeList(subtrees)
        subs = []
        for subtree in subtrees:
            subs.append(SubTree((subtree,self), T0=self.T0, T1=self.T1, prefix=self.prefix))
        return subs

    def _getSubTreeList(self,subtrees):
        if isinstance(subtrees,_ver.basestring):
            if subtrees=='included':    subtrees = [str(st.node_name) for st in _sup.getIncluded(self.tree,self.shot)]
            elif subtrees=='all':       subtrees = [str(st.node_name) for st in _sup.getSubTrees(self.tree,self.shot)]
            else:                       subtrees = [subtrees]
        return subtrees

    def getSections(self, subtrees=_subtrees):
        secs = []
        for subtree in self.getSubTrees(subtrees):
            secs+= subtree.getSections()
        return secs

    def getSectionNids(self, subtrees=_subtrees):
        nids = []
        for subtree in self.getSubTrees(subtrees):
            nids+= subtree.getSectionNids()
        return nids


    def getDevices(self, subtrees=_subtrees):
        devs = []
        for subtree in self.getSubTrees(subtrees):
            devs+= subtree.getDevices()
        return devs

    def upload(self, subtrees=_subtrees, force=False):
        secs = self.getSectionNids(subtrees)
        num = len(secs)
        param = [(self.tree,self.shot,self.T0,self.T1,self.prefix,force)]*num
        num = 0#min(num,_prc.cpu_count()-1)
        if num>1:
            pool = _prc.Pool(num)
            try:
                log = pool.map(_uploadSec,zip(secs,param))
            finally:
                pool.close()
        else:
            log = map(_uploadSec,zip(secs,param))
        return log

    def join(self):
        _prc.join(self._name)

def _uploadSub(args):
    sub,param = args
    expt,shot,T0,T1,prefix,force = param
    try:
        subtree = SubTree((expt,shot,sub),T0=T0,T1=T1,prefix=prefix)
        return (sub,subtree.upload(force=force))
    except KeyboardInterrupt as ki: raise ki
    except _mds.mdsExceptions.TreeNNF: return (sub,'not included')
    except: return (sub,_sup.error())

def _uploadSec(args):
    secnid,param = args
    expt,shot,T0,T1,prefix,force = param
    section = Section((expt,shot,secnid),T0=T0,T1=T1,prefix=prefix)
    sec = section.path
    try:
        return (sec,section.upload(force=force))
    except KeyboardInterrupt as ki: raise ki
    except: return (sec,_sup.error())

class SubTree(_mds.TreeNode):
    _index=0
    def __init__(self, subtree, T0=None, T1=None, prefix=''):
        if isinstance(subtree,tuple):
            if len(subtree)<3:
                super(SubTree,self).__init__(subtree[1].getNode("\\%s::TOP" % subtree[0]).nid,subtree[1])
            else:
                tree = _mds.Tree(subtree[0],subtree[1],'Readonly')
                super(SubTree,self).__init__(tree.getNode("\\%s::TOP" % subtree[2]).nid,tree)
        else:
            super(SubTree,self).__init__(subtree.nid,subtree.tree)
        self.name = 'SubTree-'+str(SubTree._index)
        SubTree._index+=1
        self.T0 = _sup.getTiming(self.tree.shot, 0)[0] if T0 is None else _base.Time(T0)
        self.T1 = _sup.getTiming(self.tree.shot, 1)[0] if T1 is None else _base.Time(T1)
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
    _index=0
    def __init__(self, section, T0=None, T1=None,prefix=''):
        if isinstance(section,(tuple)):
            if isinstance(section[0],_ver.basestring):
                tree = _mds.Tree(section[0], section[1], "Readonly")
                super(Section,self).__init__(section[2],tree)
                self.kks = self.getParent().getParent()
            else:
                tree = _mds.Tree(_treename, section[0], "Readonly")
                kks = tree.getNode(section[1])
                super(Section,self).__init__(kks.DATA.getDescendants()[section[2]].nid,tree)
                self.kks = kks
        else:
            super(Section,self).__init__(section.nid,section.tree)
            self.kks = self.getParent().getParent()
        self.name = 'Section-'+str(Section._index)
        Section._index+=1
        self.T0 = _sup.getTiming(self.tree.shot, 0)[0] if T0 is None else _base.Time(T0)
        self.T1 = _sup.getTiming(self.tree.shot, 1)[0] if T1 is None else _base.Time(T1)
        self.address = _base.Path(_MDS_shotrt)
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
        return self._kkslog
    kkslog = property(_getKksLog)

    def _getCfgLog(self):
        if not self.__dict__.has_key('_cfglog'):
            self._cfglog = _sup.treeToDict(self,self.kkslog.copy(),_exclude,'')
        return self._cfglog
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
        return self._signaldict
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
        return self._channeldict
    channeldict = property(_getChannelDict)

class Device(_mds.TreeNode):
    _index=0
    def __init__(self,devnid,channels,section):
        super(Device,self).__init__(devnid,section.tree)
        self.name = 'Device-'+str(Device._index)
        Device._index+=1
        self.address = _base.Path(section.address.path())
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
                units = _base.Units(signal, 1)
                if units != 'unknown':
                    self.chandescs[i]["physicalQuantity"]['type'] = units
            except KeyboardInterrupt as ki: raise ki
            except: pass
            sigdata = signal.data()
            ndims = len(sigdata.shape)
            sigdimof = (signal.dim_of().data()*1E9+self.section.T0).astype('uint64')
            if ndims==1:
                scalar[0].append(sigdata)
                scalar[2].append(self.chandescs[i])
                if scalar[1] is None:
                    scalar[1] = sigdimof
                else:
                    if not (scalar[0][0].dtype==scalar[0][-1].dtype):
                        raise(Exception('data types are not equal for all channels'))
                    # if not (len(scalar[1])==len(sigdimof) and scalar[1][0]==sigdimof[0] and scalar[1][-1]==sigdimof[-1]):
                    if not all(scalar[1]==sigdimof):
                        raise(Exception('dimesions are not equal for all channels'))
            elif ndims>1:
                images.append((sigdata,sigdimof,self.chandescs[i]))
        return scalar,images

    def writeParLog(self,path,Tx=None,join=None):
        if Tx is None: Tx = self.ParLogUpto()
        T0 = self.section.T0
        if Tx<T0:
            parlog = self.getMergeDict(self.chandescs)
            if _sup.debuglevel>=4: print(('write_parlog',path.parlog,parlog))
            return write_logurl(path.parlog, parlog, T0, join=join)
        return "parlog already written"


    def _write_signal(self,Tx=None,force=False,join=None):
        """prepares signal and uploads to webarchive"""
        if Tx is None: Tx = self.ParLogUpto()
        T0 = self.section.T0
        if Tx<T0 or force:
            signal = self.signals[0]
            if _sup.debuglevel>=2: print('one signal',signal)
            if signal.isSegmented():
                logs = []
                nSeg = signal.getNumSegments()
                if _sup.debuglevel>=2: print('is segmented',nSeg)
                for segment in _ver.xrange(nSeg):
                    seg = signal.getSegment(segment)
                    data = seg.data()
                    dimof = (seg.dim_of().data()*1E9+self.section.T0).astype('uint64')
                    if _sup.debuglevel>=2: print(('image',self.address, data.shape, data.dtype, dimof.shape, dimof[0], T0))
                    log = write_data(self.address, data, dimof, one=True,name=join)
                    logs.append({"segment": segment, "log": log})
            else:
                if _sup.debuglevel>=3: print('is not segmented')
                try:     data = signal.data()
                except _mds.mdsExceptions.TreeNODATA: return 'nodata'
                except: return {'signal',_sup.error()}
                dimof = (seg.dim_of().data()*1E9+self.section.T0).astype('uint64')
                logs = write_data(self.address, data, dimof, one=True,name=join)
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
            data = _np.array(scalars[0]).T
            dimof = _np.array(scalars[1])
            length= len(dimof)
            idx = 0;logs=[]
            while idx<length:
                N = 1000000 if length-idx>1100000 else length-idx
                logs.append(write_data(self.address, data[idx:idx+N].T, dimof[idx:idx+N]),name=join)
                idx += N
                _sup.debug(idx)
            logp = self.writeParLog(self.address,Tx,join)
            if join is None: _prc.join()
            return {'signal':logs,'parlog':logp,"path":self.address.path()}
        return {'signal':'already there','parlog':'already there',"path":self.address.path()}

    def _write_images(self, images, force=False, join=None):
        logs=[];logp=[];paths=[]
        for image in images:
            imagepath = _base.Path(self.address.path())
            imagepath.stream = self.address.stream+"_"+image[2]['name'].split('_',2)[1];
            T0 = self.section.T0
            Tx = checkLogUpto(imagepath.parlog,T0)
            paths.append(imagepath.path())
            if Tx<T0 or force:
                data = _np.array(image[0])
                dimof = _np.array(image[1])
                print(data.shape,dimof.shape)
                if _sup.debuglevel>=3: print(('image',imagepath, data.shape, data.dtype, dimof.shape, dimof[0], T0))
                logs.append(write_data(imagepath, data, dimof,name=join))
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
        return self._allsignals
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
        return self._devicedict
    devicedict = property(_getDeviceDict)

    def _getSignals(self):
        if not self.__dict__.has_key('_signals'):
            self._getDeviceDict()
        return self._signals
    signals = property(_getSignals)

    def _getChanDescs(self):
        if not self.__dict__.has_key('_chandescs'):
            self._getDeviceDict()
        return self._chandescs
    chandescs = property(_getChanDescs)

    def getMergeDict(self,chanDescs):
        dic = self.devicedict.copy()
        dic['chanDescs'] = chanDescs
        return dic

    def _chanDesc(self,signalset):
        """generates the channel descriptor for a given channel"""
        def _channelDict(signalroot, nid):
            """collects the parameters of a channel"""
            channelDict = {}
            channelDict['name'] = getDataName(signalroot)
            channelDict["active"] = int(signalroot.isOn())
            channelDict["physicalQuantity"] = {'type': 'unknown'}
            for sibling in signalroot.getDescendants():
                if not sibling.Nid == nid:  # in case: DEVICE.STRUCTURE:SIGNAL
                    channelDict = _sup.treeToDict(sibling, channelDict, _exclude)
            return channelDict
        def mergeSignalDict(chanDesc):
            if self.section.signaldict.has_key(nid):
                for key, value in self.section.signaldict[nid].items():
                    chanDesc[key] = value
            return chanDesc
        def substituteUnits():
            if 'units' in chanDesc.keys():
                chanDesc["physicalQuantity"]['type'] = _base.Units(chanDesc["units"], 1)
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
        p = _if.get_json(path,time=[filterstart,filterstop],Nsamples=2)
        t = p['dimensions']
        return t[0]
    except:
        return 0

def extractNid(obj):
    if isinstance(obj,(_mds.TreeNode)):
        if obj.usage == 'SIGNAL':
            return obj.nid
    elif isinstance(obj,(_mds.tdibuiltins.EXT_FUNCTION)):
        for arg in obj.args:
            nid = extractNid(arg)
            if nid is not None:
                return nid