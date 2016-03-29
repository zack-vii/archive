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
from archive import base as _base
from archive import diff as _diff
from archive import interface as _if
from archive import support as _sup
from archive import version as _ver
_MDS_shotdb = _base.Path('/Test/raw/W7X/MDSplus/Shots')  # raw/W7X/MDSplus/Shots
_MDS_shotrt = _base.Path('/Test/raw/W7X')  # raw/W7X
_subtrees  = 'included'
_exclude   = {'usage':['ACTION', 'TASK', 'SIGNAL']}

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

def uploadModel(shot, subtrees=_subtrees, treename='W7X', T0=None):
    """uploads full model tree of a given shot into the web archive
    should be executed right after T0"""
    if shot<0:
        raise Exception("Shot number must be positive (must not direct to the model).")
    if isinstance(subtrees,_ver.basestring):
        if subtrees=='included':    subtrees = [str(st.node_name) for st in _sup.getIncluded(treename,-1)]
        elif subtrees=='all':       subtrees = [str(st.node_name) for st in _sup.getSubTrees(treename,-1)]
        else:                       subtrees = [subtrees]
    def getModel():
        nodenames = ['ADMIN','TIMING']+subtrees
        w7x = _mds.Tree(treename,-1)
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

def uploadShot(shot, subtrees=_subtrees, treename='W7X', T0=None, T1=None, force=False, prefix=''):
    """uploads the data of all sections of a given shot into the web archive
    should be executed after all data is written to the shot file"""
    if shot<0:  raise Exception("Shot number must be positive (must not direct to the model).")
    if isinstance(subtrees,_ver.basestring):
        if subtrees=='included':    subtrees = [str(st.node_name) for st in _sup.getIncluded(treename,shot)]
        elif subtrees=='all':       subtrees = [str(st.node_name) for st in _sup.getSubTrees(treename,shot)]
        else:                       subtrees = [subtrees]
    print(subtrees)
    if T0 is None:  T0 = _sup.getTiming(shot, 0)[0]
    else:           T0 = _base.Time(T0)
    if T1 is None:  T1 = _sup.getTiming(shot, 1)[0]
    else:           T1 = _base.Time(T1)
    sectionDicts = []
    w7x = _mds.Tree(treename, shot)
    path = _MDS_shotrt
    for subtree in subtrees:
        kks = w7x.getNode(subtree)
        kkscfg = _getCfgLog(kks,shot)
        data = kks.DATA
        for sec in data.getDescendants():
            section = prefix + subtree.upper()+'_'+getDataName(sec)
            path.streamgroup = section
            Tx = checkLogUpto(path.cfglog,T0)
            print(shot,sec,section,T0,Tx)
            if Tx<0 or Tx!=T0-1 or force:
                sectionDict = _sectionDict(sec, kks, T0, T1, path)
                if Tx<0 or Tx!=T0-1:
                    try:
                        cfglog = _sup.treeToDict(sec,kkscfg.copy(),_exclude,'')
                        if _sup.debuglevel>=3: print('write_cfglog',sec,cfglog)
                        log_cfglog = _if.write_logurl(path.cfglog, cfglog, T0, Tx)
                    except Exception as exc:
                        print(exc)
                        log_cfglog = exc
                else:
                    log_cfglog = 'not written, force=True'
                sectionDicts.append({'sectionDict': sectionDict, "cfglog": log_cfglog})
            else:
                print('cfglog already written: skip')
    return(sectionDicts)

def _getCfgLog(kks,shot=None,treename='W7X'):
    """generates the base cfglog of a kks subtree
    called by uploadShot"""
    if isinstance(kks, str): kks = _mds.Tree(treename,shot).getNode(kks)
    cfglog = {}
    for m in kks.getMembers():
        cfglog  = _sup.treeToDict(m, cfglog, _exclude)
    for c in kks.getChildren():
        if not c.getNodeName() in ['HARDWARE','DATA']:
            cfglog = _sup.treeToDict(c, cfglog, _exclude)
    return cfglog

def _sectionDict(section, kks, T0, T1, path, test=False):
    """generates the parlog for a section under .DATA and upload the data
    called by uploadShot"""
    """signalDict:   (nid of channel) :{dict of signal}"""
    """channelLists: (nid of device):[nid of channels]"""
    sectionDict = []
    HW = kks.HARDWARE
    if HW.getNumDescendants()==0: return
    signalDict = {}
    for signal in section.getDescendants(): signalDict = _signalDict(signal, signalDict)
    channelLists = _getChannelLists(kks,signalDict)
    for devnid,channels in channelLists.items():
        try:
            if len(channels)==0: continue
            sectionDict.append({"log":{}})
            device = _mds.TreeNode(devnid)
            stream = getDataName(device)
            path.stream = stream
            Tx = checkLogUpto(path.parlog,T0)
            if Tx<0 or Tx!=T0-1:
                sectionDict[-1]["path"]=path.path()
                deviceDict, signalList = _deviceDict(device, channels, signalDict)
                if _sup.debuglevel>=2: print(deviceDict)
                log_signal = _write_signals(path, signalList, T1)
                sectionDict[-1]["log"]['signal']=log_signal
                print(T0,Tx)
                try:    log_parlog = _if.write_logurl(path.url_parlog(), deviceDict, T0, Tx)
                except: log_parlog = _sup.error()
                sectionDict[-1]["log"]['parlog']=log_parlog
            else:
                sectionDict[-1]['log'] = 'parlog exists '+Tx.utc+' : '+T0.utc
        except:
            _sup.error()
    return sectionDict

def _signalDict(signal, signalDict, prefix=[]):
    """collects the properties of a signal node and assiciates it with a channel nid
    called by _sectionDict"""
    nameList = prefix+[getDataName(signal)]
    if signal.usage == "SIGNAL":
        try:
            desc = {}
            if signal.getNumDescendants()>0: desc = _sup.treeToDict(signal, desc, _exclude,'')
            desc["name"] = '_'.join(nameList)
            nid = signal.record.nid
            signalDict[nid] = desc
        except AttributeError as exc:
            print('_signalDict',exc,signal.record)
        except:
            _sup.error()
    else:
        for sig in signal.getDescendants(): signalDict = _signalDict(sig, signalDict, nameList)
    return signalDict

def _getChannelLists(kks, channels):
    """collects the channel lists of all devices
    called by _sectionDict"""
    f = _re.compile('(?<=\.HARDWARE[:\.])([^\.:]+)')
    HW = kks.HARDWARE
    channelLists = dict([device.Nid, []] for device in HW.getDescendants())
    for channel in channels:
        deviceName = f.search(str(_mds.TreeNode(channel).fullpath)).group(0)
        device = HW.getNode(deviceName)
        channelLists[device.nid].append(channel)
    return channelLists

def _deviceDict(device, channelList, signalDict):
    """collects the data of channels and their parenting device
    called by _sectionDict"""
    signalList = []
    chanDescs = []
    signals = _searchSignals(device)
    for signal in signals:
        if signal[1].nid in channelList:  # add to signal list
            chanDescs.append(_chanDesc(signal, signalDict))
            signalList.append(signal[1])
    exclude = _exclude.copy()
    exclude['nid'] = [s[0] for s in signals]
    deviceDict = _sup.treeToDict(device,{},_exclude,'')
    deviceDict["chanDescs"] = chanDescs
    return(deviceDict, signalList)

def _searchSignals(device):
    """checks DEVICE:SIGNAL and DEVICE.STRUCTURE:SIGNAL"""
    signals = []
    for descendant in device.getDescendants():
        if descendant.usage == "SIGNAL":
            signals.append([descendant,descendant])
        else:
            sigs = _searchSignals(descendant)
            signals+= sigs
    if len(signals)==1: signals[0][0] = device
    return signals

def _chanDesc(signalset, signalDict={}):
    """generates the channel descriptor for a given channel"""
    def mergeSignalDict(chanDesc):
        if nid in signalDict.keys():
            for key, value in signalDict[nid].items():
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
    try:
        units = _base.Units(signal, 1)
        if units != 'unknown':
            chanDesc["physicalQuantity"]['type'] = units
    except:pass
    return chanDesc

def _channelDict(signalroot, nid):
    """collects the parameters of a channel
    called by _chanDesc"""
    channelDict = {}
    channelDict['name'] = getDataName(signalroot)
    channelDict["active"] = int(signalroot.isOn())
    channelDict["physicalQuantity"] = {'type': 'unknown'}
    for sibling in signalroot.getDescendants():
        if not sibling.Nid == nid:  # in case: DEVICE.STRUCTURE:SIGNAL
            channelDict = _sup.treeToDict(sibling, channelDict, _exclude)
    return channelDict

def _write_signals(path, signals, t0):
    """prepares signals and uploads to webarchive
    called by <multiple>"""
    logs = []
    t0 = _base.Time(t0).ns
    if len(signals) == 1:
        if _sup.debuglevel>=2: print('one signal',signals[0])
        signal = signals[0]
        if signal.isSegmented():
            nSeg = signal.getNumSegments()
            if _sup.debuglevel>=2: print('is segmented',nSeg)
            for segment in _ver.xrange(nSeg):
                seg = signal.getSegment(segment)
                data = seg.data()
                dimof = seg.dim_of().data()
                if _sup.debuglevel>=2: print('image',path, data, dimof, t0)
                try:
                    log = _if.write_data(path, data, dimof, t0)
                    if log.getcode() >= 400:   print(segment,log.content)
                except:
                    log = _sup.error(1)
                logs.append({"segment": segment, "log": log})
        else:
            if _sup.debuglevel>=3: print('is not segmented')
            try:     data = signal.data().T
            except:  return []
            dimof = _base.TimeArray(signal.dim_of().data()).ns
            log = _if.write_data(path, data, dimof, t0)
            if log.getcode() >= 400:   print(0,log.content)
        return [log]
    if _sup.debuglevel>=2: print('%d signal' % len(signals))
    scalar = [[],None]
    images = []
    for signal in signals:
        if _sup.debuglevel>=3: print('signal',signal)
        signal = signal.evaluate()
        sigdata = signal.data()
        ndims = len(sigdata.shape)
        sigdimof = (signal.dim_of().data()*1E9).astype('uint64')
        if ndims==1:
            scalar[0].append(sigdata)
            if scalar[1] is None:
                scalar[1] = sigdimof
            else:
                if not (scalar[0][0].dtype==scalar[0][-1].dtype):
                    raise(Exception('data types are not equal for all channels'))
                if not all(scalar[1]==sigdimof):
                    raise(Exception('dimesions are not equal for all channels'))
        elif ndims>1:
            images.append((sigdata,sigdimof))
    del(signals)
    if _sup.debuglevel>=3: print('scalar',scalar)
    if scalar[1] is not None:
        data = _np.array(scalar[0]).T
        dimof = _np.array(scalar[1])
        length= len(dimof)
        idx = 0;
        while idx<length:
            N = 10000 if length-idx>15000 else length-idx
            try:
                logs.append(_if.write_data(path, data[idx:idx+N].T, dimof[idx:idx+N], t0))
                if logs[-1].getcode() >= 400:
                    print(idx,idx+N-1,logs[-1].content)
            except:
                pass
            idx = idx+N
    del(scalar)
    for image in images:
        data = _np.array(image[0]).T
        dimof = _np.array(image[1])
        print(data.shape,dimof.shape)
        if _sup.debuglevel>=3: print('image',path, data, dimof, t0)
        try:     logs.append(_if.write_data(path, data, dimof, t0))
        except:  logs.append(None)
    return logs

def getDataName(datanode):
    """converts 'ABCD_12XY_II' to 'Abcd_12Xy_II"""
    def process(i):
        if all(c=='I' for c in i): return i
        else: return i[0]+i[1:].lower()
    if isinstance(datanode,_mds.TreeNode):
        datanode = str(datanode.node_name)
    f = _re.findall('([^A-Z]*)([A-Z]*)',datanode)
    return ''.join([i[0]+process(i[1]) for i in f])

def _buildPath(node):
    """generates a path out of a treepath
    called by uploadNode"""
    PathParts = _re.split('::|:|\.|_', node.path.lstrip('\\'))
    KKS = PathParts[0]
    if PathParts[1] == 'EVAL':  view = 'raw'  # will be cocking once supported
    else:                       view = 'raw'
    section = PathParts[-2].lower()
    groupname = PathParts[-1].lower()
    path = _base.Path('/'.join([view, 'W7X', KKS+'_'+section, groupname]))
    return(path)

def uploadNode(node, shot=0, treename='W7X'):
    """upload a single node structure to the webarchive for kks_EVAL trees
    uploadNode("\\KKS_EVAL::TOP.RESULTS:MYSECTION:MYIMAGE", -1, "sandbox")
    """
    if isinstance(node, (_mds.treenode.TreeNode)):
        tree = node.tree
        shot = int(tree.shot)
    else:
        tree = _mds.Tree(treename, shot)
        node = tree.getNode(node)
    path = _buildPath(node)
    T0 = _sup.getTiming(shot, 0)[0]
    T1 = _sup.getTiming(shot, 1)[0]
    parlog, sig = _deviceDict(node)
    if node.usage == "SIGNAL":
        if len(sig):  # prepend
            sig = [node] + sig
            parlog["chanDescs"] = [_chanDesc(node, node)] + parlog["chanDescs"]
        else:  # replace
            sig = [node]
            parlog = {"chanDescs": [_chanDesc(node, node)]}
    parlog["shot"] = int(shot)
    r = [None, None]
    r[0] = _if.write_logurl(path.url_parlog(), parlog, T0)
    r[1] = _write_signals(path, sig, T1)
    print(r)
    if not r[0].ok: print(r[0].content)
    if not r[1].ok: print(r[1].content)
    return r

def checkLogUpto(path,Tfrom):
    try:
        filterstart = Tfrom.ns-1
        filterstop=2000000000000000000
        p = _if.get_json(path,filterstart=filterstart,filterstop=filterstop)
        for i in range(10):
            s = str(p['_links']['children'][-1]['href'])
            try:
                t = map(int,_re.findall('(?<=from=|upto=)([0-9]+)',s))
                if t[0]==filterstart:
                    if t[1]>=filterstop:
                      return _base.Time(-1)
                    else:
                      return _base.Time(t[1])
                else: return _base.Time(t[0])
            except:
                p = _if.get_json(s)
    except: pass
    return _base.Time(-1)
