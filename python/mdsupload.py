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


def setupTiming():
    """should be executed only once before frist experiment"""
    def chanDesc(n):
        if n==0:
            return {'name':'shot','physicalQuantity':{'type':'none'}}
        else:
            return {'name':'T%d' % n,'physicalQuantity':{'type':'ns'}}
    parlog = {'chanDescs':[chanDesc(n) for n in _ver.xrange(7)]}
    result = _if.write_logurl(_MDS_shots.url_parlog(), parlog, 1)
    print(result.msg)
    return result


def uploadModel(shot, subtrees=_subtrees, treename='W7X'):
    """should be executed right after T0"""
    def getModel():
        nodenames = ['ADMIN','TIMING']+subtrees
        w7x = _mds.Tree(treename,-1)
        model = {}
        for key in nodenames:
            print('reading %s' % key)
            model[key]=_diff.treeToDict(w7x.getNode(key))

    time = _sup.getTiming(shot, 0)
    cfglog = getModel()
    result = _if.write_logurl(_MDS_shots.url_cfglog(), cfglog, time)
    print(result.msg)
    return result


def uploadTiming(shot):
    """should be executed soon after T6"""
    data = _np.uint64(_sup.getTiming(shot))
    dim = data[0]
    data[0] = int(shot)
    result = _if.write_data(_MDS_shots, data, dim)
    print(result.msg)
    return result


def uploadShot(shot, subtrees=_subtrees, treename='W7X', time=None):
    '''
    uploadData(['KKS'], shot=0, treename='W7X')
    uploads all sections of a given tree
    e.g.: D=uploadData(['QMC', 'QMR', 'QRN', 'QSW', 'QSX'], 2, 'W7X')
    '''
    SD = []
    w7x = _mds.Tree(treename, shot)
    if time is None:
        time = _base.TimeInterval(w7x.getNode('\TIME'))
    else:
        time = _base.TimeInterval(time)
    path = _base.Path('raw/W7X')
    for subtree in subtrees:
        kks = w7x.getNode(subtree)
        kkscfg = _getCfgLog(kks)
        data = kks.getNode('DATA')
        secdict = {}
        for sec in data.getDescendants():
            print(sec)
            path.streamgroup = subtree.upper()+'_'+sec.getNodeName().lower()+'1'
            try:
                cfg = _treeToDict(sec,kkscfg.copy(),'')
                pcl = _if.write_logurl(path.url_cfglog(), cfg, time.fromT)
            except:
                pcl = _sup.error(1)
            ch, secdict = _sectionDict(sec, secdict, kks, time, path)
            SD.append({"ch": ch, "pcl": pcl})
    return(SD)


def _getCfgLog(node):
    parms = {}
    for m in node.getMembers():
        parms  = _treeToDict(m,parms)
    for c in node.getChildren():
        if not c.getNodeName() in ['HARDWARE','DATA']:
            parms = _treeToDict(c,parms)
    return(parms)


def _sectionDict(node, secs, kks, time, path):
    ''' for interenal use
    browse sections of .DATA
    '''
    f = _re.compile('(?<=\.HARDWARE[:\.])([^\.:]+)')
    chans = {}
    devs = {}
    CH = []
    try:
        for d in node.getDescendants():
            chans = _channelDescs(d, chans)
        secs[str(node.getNodeName())] = chans
        HW = kks.getNode('HARDWARE')
        if HW.getNumDescendants()>0:
            for dev in HW.getDescendants():
                devs[dev.Nid] = []
            for ch in chans.keys():
                devName = f.search(str(kks.tree.getNode(ch).FullPath)).group(0)
                dev = HW.getNode(devName)
                devs[dev.nid].append(ch)
            for dev in HW.getDescendants():
                if len(devs[dev.Nid]):
                    path.stream = dev.getNodeName().lower()
                    par, sig = _iterateDevices(dev, devs[dev.Nid], chans)
                    try:
                        rpl = _if.write_logurl(path.url_parlog(), par, time.fromT)
                    except:
                        rpl = _sup.error()
                    rd = _write_signals(path, sig, time.t0T)
                    CH.append({"path": path.path(),
                               "par": par, "sig": sig, "rpl": rpl, "rd": rd})
    except:
        _sup.error()
    return(CH, secs)


def _channelDescs(ch, dic):
    if ch.usage == "SIGNAL":
        desc = {}
        if ch.getNumDescendants()>0:
            _treeToDict(ch, desc)
        desc["name"] = ch.getNodeName().lower()
        nid = ch.getData().Nid
        dic[nid] = desc
    return dic


def _iterateDevices(node, chlist, chans):
    parms = {}
    signals = []
    descs = []
    for des in node.getDescendants():
        sig = None
        if des.usage == "SIGNAL":
            sig = des
        elif des.usage == "STRUCTURE":
            for m in des.getMembers():
                if m.usage == 'SIGNAL':
                    sig = m
        if sig is None:
            _treeToDict(des, parms)
        elif sig.Nid in chlist:
            descs.append(_signalDict(des, sig, chans))
            signals.append(sig)
    parms["chanDescs"] = descs
    return(parms, signals)


def _signalDict(node, sig=None, dic={}):
    if sig is None:
        sig = node
    desc = {}
    desc['name'] = node.getNodeName().lower()
    desc["active"] = int(node.isOn())
    desc["physicalQuantity"] = {'type': 'unknown'}
    nid = sig.Nid
    for des in node.getDescendants():
        if not des.Nid == nid:
            desc = _treeToDict(des, desc)
    if nid in dic.keys():
        for k, v in dic[nid].items():
            desc[k] = v
    if 'units' in desc.keys():
        desc["physicalQuantity"]['type'] = _base.Units(desc["units"], 1)
        del(desc["units"])
    try:
        units = _base.Units(sig, 1)
        if units != 'unknown':
            desc["physicalQuantity"]['type'] = units
    except:
        pass
    return(desc)


def _treeToDict(node, Dict={}, name=None):
    try:
        if node.usage in ['ACTION', 'TASK', 'SIGNAL']:  # exclude by usage
            return Dict
        sDict = {}
        if name is None:
            name = node.getNodeName().lower()
        elif name=='':
            sDict = Dict
        try:
            data = node.data().tolist()
            try:
                data = data.tolist()
            except:
                pass
        except:
            data = None
        for d in node.getDescendants():
            sDict = _treeToDict(d, sDict)
        if len(sDict.keys()):
            if data is not None:
                sDict["$value"] = data
            if name!='':
                Dict[name] = sDict
        elif data is not None:
            if name!='':
                Dict[name] = data
            else:
                Dict = data
    except:
        pass
    return(Dict)


def _write_signals(path, signals, t0):
    R = []
    t0 = _base.Time(t0).ns
    if len(signals) == 1:
        sig = signals[0]
        if sig.isSegmented():
            R = []
            for seg in _ver.xrange(sig.getNumSegments()):
                data = sig.getSegment(seg).data()
                dimof = _base.TimeArray(sig.getSegmentDim(seg)).ns
                r = _if.write_data(path, data, dimof, t0)
                R.append({"seg": seg, "rds": r})
                print(seg, r)
                if r.getcode() >= 400:
                    print(r.content)
                    return(R)
        else:
            data = sig.data()
            dimof = _base.TimeArray(sig.dim_of()).ns
            R = _if.write_data(path, data, dimof, t0)
            print(R)
            if R.getcode() >= 400:
                print(R.content)
        return(R)
    elif len(signals) > 1:
        R = []
        data = []
        dimof = []
        for sig in signals:
            sig = sig.evaluate()
            data.append(sig.data())
            sigdimof = _base.TimeArray(sig.dim_of()).ns
            if dimof==[]:
                dimof = sigdimof
            else:
                if not (data[0].dtype==data[-1].dtype):
                    raise(Exception('data types are not equal for all channels'))
                if not (dimof==sigdimof):
                    raise(Exception('dimesions are not equal for all channels'))
        data = _np.array(data)
        dimof = _np.array(dimof)
        N = 100000
        for i in _ver.xrange(int((len(dimof)-1)/N+1)):
            R.append(_if.write_data(path, data[:,i*N:(i+1)*N], dimof[i*N:(i+1)*N], t0))
            if R[-1].getcode() >= 400:
                print(R.content)
        return R


def _buildPath(node):  # , subsection=None):
    from re import split
    PathParts = split('::|:|\.|_', node.path.lstrip('\\'))
    KKS = PathParts[0]
    if PathParts[1] == 'EVAL':
        view = 'raw'  # will be cocking once supported
    else:
        view = 'raw'
    section = PathParts[-2].lower()
    groupname = PathParts[-1].lower()
    path = _base.Path('/'.join([view, 'W7X', KKS+'_'+section, groupname]))
    return(path)


def uploadNode(node):  # , subsection=None):
    '''
    node = TreeNode or (tree, shot, path) or path -> (tree='w7X', shot=0)
    e.g.:
    uploadNode(("sandbox", -1, "\\KKS_EVAL::TOP.RESULTS:MYSECTION:MYIMAGE"))
    '''
    if isinstance(node, (_mds.treenode.TreeNode)):
        tree = node.tree
    elif isinstance(node, (tuple, list)):
        tree = _mds.Tree(node[0], node[1])
        node = tree.getNode(node[2])
    else:
        tree = _mds.Tree('W7X', 0)
        node = tree.getNode(node)
    path = _buildPath(node)  # , subsection)
    t0 = _base.Time(tree.getNode('\TIME.T0:IDEAL').data())
    t1 = _base.Time(tree.getNode('\TIME.T1:IDEAL').data())
    par, sig = _iterateDevices(node)
    if node.usage == "SIGNAL":
        if len(sig):  # prepend
            sig = [node] + sig
            par["chanDescs"] = [_signalDict(node, node)] + par["chanDescs"]
        else:  # replace
            sig = [node]
            par = {"chanDescs": [_signalDict(node, node)]}
    par["shot"] = int(tree.shot)
    r = [None, None]
    r[0] = _if.write_logurl(path.url_parlog(), par, t0)
    r[1] = _write_signals(path, sig, t1)
    print(r)
    if not r[0].ok:
        print(r[0].content)
    if not r[1].ok:
        print(r[1].content)
    return r
