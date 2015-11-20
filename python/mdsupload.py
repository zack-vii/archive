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
_MDS_shots = _base.Path('raw/W7X/MDSplus/shots')
_subtrees  = ['QMC','QMR','QRN','QSD','QSQ','QSR','QSW','QSX']
_exclude   = ['ACTION', 'TASK', 'SIGNAL']

def setupTiming():
    """sets up the parlog of the shots datastream in the web archive
    should be executed only once before frist experiment"""
    def chanDesc(n):
        if n==0:
            return {'name':'shot','physicalQuantity':{'type':'none'}}
        else:
            return {'name':'T%d' % n,'physicalQuantity':{'type':'ns'}}
    parlog = {'chanDescs':[chanDesc(n) for n in _ver.xrange(7)]}
    result = _if.write_logurl(_MDS_shots.url_parlog(), parlog, 1)
    print(result.msg)
    return result


def uploadModel(shot, subtrees=_subtrees, treename='W7X', T0=None):
    """uploads full model tree of a given shot into the web archive
    should be executed right after T0"""
    def getModel():
        nodenames = ['ADMIN','TIMING']+subtrees
        w7x = _mds.Tree(treename,-1)
        model = {}
        for key in nodenames:
            print('reading %s' % key)
            model[key]=_diff.treeToDict(w7x.getNode(key))

    if T0 is None:
        T0 = _sup.getTiming(shot, 0)
    else:
        T0 = _base.Time(T0)
    cfglog = getModel()
    result = _if.write_logurl(_MDS_shots.url_cfglog(), cfglog, T0)
    print(result.msg)
    return result


def uploadTiming(shot):
    """uploads the timing of a given shot into the web archive
    should be executed soon after T6"""
    data = _np.uint64(_sup.getTiming(shot))
    dim = data[0]
    data[0] = int(shot)
    result = _if.write_data(_MDS_shots, data, dim)
    print(result.msg)
    return result


def uploadShot(shot, subtrees=_subtrees, treename='W7X', T0=None, T1=None):
    """uploads the data of all sections of a given shot into the web archive
    should be executed after all data is written to the shot file"""
    sectionDicts = []
    w7x = _mds.Tree(treename, shot)
    if T0 is None:
        T0 = _sup.getTiming(shot, 0)
    else:
        T0 = _base.Time(T0)
    if T1 is None:
        T1 = _sup.getTiming(shot, 0)
    else:
        T1 = _base.Time(T1)
    path = _base.Path('raw/W7X')
    for subtree in subtrees:
        kks = w7x.getNode(subtree)
        kkscfg = _getCfgLog(kks)
        data = kks.DATA
        for sec in data.getDescendants():
            print(sec)
            path.streamgroup = subtree.upper()+'_'+sec.getNodeName().lower()
            try:
                cfglog = _sup.treeToDict(sec,kkscfg.copy(),_exclude,'')
                log_cfglog = _if.write_logurl(path.url_cfglog(), cfglog, T0)
            except:
                log_cfglog = _sup.error(1)
            sectionDict = _sectionDict(sec, kks, T0, T1, path)
            sectionDicts.append({'sectionDict': sectionDict, "cfglog": log_cfglog})
    return(sectionDicts)


def _getCfgLog(kks):
    """generates the base cfglog of a kks subtree
    called by uploadShot"""
    cfglog = {}
    for m in kks.getMembers():
        cfglog  = _sup.treeToDict(m, cfglog, _exclude)
    for c in kks.getChildren():
        if not c.getNodeName() in ['HARDWARE','DATA']:
            cfglog = _sup.treeToDict(c, cfglog, _exclude)
    return(cfglog)


def _sectionDict(section, kks, T0, T1, path):
    """generates the parlog for a section under .DATA and upload the data
    called by uploadShot"""
    signalDict = {}
    deviceDict = {}
    sectionDict = []
    try:
        for signal in section.getDescendants():
            signalDict = _signalDict(signal, signalDict)
        HW = kks.HARDWARE
        if HW.getNumDescendants()>0:
            channelLists = _getChannelLists(kks)
            for dev in HW.getDescendants():
                if len(channelLists[dev.Nid]):
                    path.stream = dev.getNodeName().lower()
                    deviceDict, signalList = _deviceDict(dev, channelLists[dev.Nid], signalDict)
                    try:
                        log_parlog = _if.write_logurl(path.url_parlog(), deviceDict, T0)
                    except:
                        log_parlog = _sup.error()
                    log_signal = _write_signals(path, signalList, T1)
                    sectionDict.append({"path": path.path(),
                               "deviceDict": deviceDict,
                               "signalList": signalList,
                               "log": {'parlog':log_parlog, 'signal':log_signal}})
    except:
        _sup.error()
    return sectionDict


def _signalDict(signal, signalDict={}):
    """collects the properties of a signal node and assiciates it with a channel nid
    called by _sectionDict"""
    if signal.usage == "SIGNAL":
        desc = {}
        if signal.getNumDescendants()>0:
            _sup.treeToDict(signal, desc, _exclude)
        desc["name"] = signal.getNodeName().lower()
        nid = signal.getData().Nid
        signalDict[nid] = desc
    return signalDict


def _getChannelLists(kks, channels):
    """collects the channel lists of all devices
    called by _sectionDict"""
    f = _re.compile('(?<=\.HARDWARE[:\.])([^\.:]+)')
    HW = kks.HARDWARE
    channelLists = dict([device.Nid, []] for device in HW.getDescendants())
    for channel in channels.keys():
        deviceName = f.search(str(kks.tree.getNode(channel).FullPath)).group(0)
        device = HW.getNode(deviceName)
        channelLists[device.nid].append(channel)
    return channelLists


def _deviceDict(device, channelList, signalDict={}):
    """collects the data of channels and their parenting devices
    called by _sectionDict"""
    def _searchSignal(descendant):
        """checks DEVICE:SIGNAL and DEVICE.STRUCTURE:SIGNAL"""
        if descendant.usage == "SIGNAL":
            return descendant
        elif descendant.usage == "STRUCTURE":
            for member in descendant.getMembers():
                if member.usage == 'SIGNAL':
                    return member
        return None

    deviceDict = {}
    signalList = []
    chanDescs = []
    for descendant in device.getDescendants():
        signal = _searchSignal(descendant)
        if signal is None:  # add to parlog
            _sup.treeToDict(descendant, deviceDict, _exclude)
        elif signal.Nid in channelList:  # add to signal list
            chanDescs.append(_chanDesc(descendant, signal, signalDict))
            signalList.append(signal)
    deviceDict["chanDescs"] = chanDescs
    return(deviceDict, signalList)


def _chanDesc(signalroot, signal, signalDict={}):
    """generates the channel descriptor for a given channel"""
    def mergeSignalDict():
        if nid in signalDict.keys():
            for key, value in signalDict[nid].items():
                chanDesc[key] = value

    def substituteUnits():
        if 'units' in chanDesc.keys():
            chanDesc["physicalQuantity"]['type'] = _base.Units(chanDesc["units"], 1)
            del(chanDesc["units"])
    nid = signal.Nid
    chanDesc = _channelDict(signalroot, nid)
    chanDesc = mergeSignalDict(nid)
    chanDesc = substituteUnits()
    try:
        units = _base.Units(signal, 1)
        if units != 'unknown':
            chanDesc["physicalQuantity"]['type'] = units
    except:
        pass
    return chanDesc


def _channelDict(signalroot, nid):
    """collects the parameters of a channel
    called by _chanDesc"""
    channelDict = {}
    channelDict['name'] = signalroot.getNodeName().lower()
    channelDict["active"] = int(signalroot.isOn())
    channelDict["physicalQuantity"] = {'type': 'unknown'}
    for sibling in signalroot.getDescendants():
        if not sibling.Nid == nid:  # in case: DEVICE.STRUCTURE:SIGNAL
            channelDict = _sup.treeToDict(sibling, channelDict, _exclude)


def _write_signals(path, signals, t0):
    """prepares signals and uploads to webarchive
    called by <multiple>"""
    logs = []
    t0 = _base.Time(t0).ns
    if len(signals) == 1:
        signal = signals[0]
        if signal.isSegmented():
            for segment in _ver.xrange(signal.getNumSegments()):
                data = signal.getSegment(segment).data()
                dimof = _base.TimeArray(signal.getSegmentDim(segment)).ns
                log = _if.write_data(path, data, dimof, t0)
                logs.append({"segment": segment, "log": log})
                if logs[-1].getcode() >= 400:
                    print(segment,logs[-1].content)
        else:
            data = signal.data()
            dimof = _base.TimeArray(signal.dim_of()).ns
            logs.append(_if.write_data(path, data, dimof, t0))
            if logs[-1].getcode() >= 400:
                print(0,logs[-1].content)
        return logs
    scalar = [[],None]
    images = []
    for signal in signals:
        signal = signal.evaluate()
        ndims = len(signal.data().shape)
        sigdimof = _base.TimeArray(signal.dim_of()).ns
        if ndims==1:
            data.append(signal.data())
            if scalar[1] is None:
                scalar[1] = sigdimof
            else:
                if not (data[0].dtype==data[-1].dtype):
                    raise(Exception('data types are not equal for all channels'))
                if not all(scalar[1]==sigdimof):
                    raise(Exception('dimesions are not equal for all channels'))
        elif ndims>1:
            images.append((signal.data(),sigdimof))
    del(signals)
    if scalar[1] is not None:
        data = _np.array(scalar[0]).T
        dimof = _np.array(scalar[1])
        del(scalar)
        length= len(dimof)
        idx = 0;
        while idx<length:
            N = 5000 if length-idx>7500 else length-idx
            logs.append(_if.write_data(path, data[idx:idx+N], dimof[idx:idx+N], t0))
            if logs[-1].getcode() >= 400:
                print(idx,idx+N-1,logs[-1].content)
            idx = idx+N
    del(scalar)
    for image in images:
        data = _np.array(image[0]).T
        dimof = _np.array(image[1])
        logs.append(_if.write_data(path, data, dimof, t0))

    return logs


def _buildPath(node):
    """generates a path out of a treepath
    called by uploadNode"""
    PathParts = _re.split('::|:|\.|_', node.path.lstrip('\\'))
    KKS = PathParts[0]
    if PathParts[1] == 'EVAL':
        view = 'raw'  # will be cocking once supported
    else:
        view = 'raw'
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
    T0 = _sup.getTiming(shot, 0)
    T1 = _sup.getTiming(shot, 1)
    parlog, sig = _deviceDict(node)
    if node.usage == "SIGNAL":
        if len(sig):  # prepend
            sig = [node] + sig
            parlog["chanDescs"] = [_chanDesc(node, node)] + parlog["chanDescs"]
        else:  # replace
            sig = [node]
            parlog = {"chanDescs": [_chanDesc(node, node)]}
#    parlog["shot"] = int(shot)
    r = [None, None]
    r[0] = _if.write_logurl(path.url_parlog(), parlog, T0)
    r[1] = _write_signals(path, sig, T1)
    print(r)
    if not r[0].ok:
        print(r[0].content)
    if not r[1].ok:
        print(r[1].content)
    return r
