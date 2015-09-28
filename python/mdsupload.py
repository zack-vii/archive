"""
archive.MDSupload
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus as _mds
import re as _re
from . import base as _base
from . import support as _sup
from . import interface as _if
from . import version as _ver


def upload(names=['SINWAV'], shot=0, treename='W7X', time=None):
    '''
    upload(['KKS'], shot=0, treename='W7X')
    uploads all sections of a given tree
    e.g.: D=upload(['QMC', 'QMR', 'QRN', 'QSW', 'QSX'], 2, 'W7X')
    '''
    SD = []
    w7x = _mds.Tree(treename, shot)
    if time is None:
        time = _base.TimeInterval(w7x.getNode('\TIME'))
    path = _base.Path('raw/W7X')
    for name in names:
        kks = w7x.getNode(name)
        cfg = _getCfgLog(kks)
        data = kks.getNode('DATA')
        secdict = {}
        for sec in data.getDescendants():
            print(sec)
            path.set_streamgroup(name+'_'+sec.getNodeName().lower())
            try:
                pcl = _if.write_logurl(path.url_cfglog(), cfg, time.fromT)
            except:
                pcl = _sup.error(1)
            ch, secdict = _sectionDict(sec, secdict, kks, time, path)
            SD.append({"ch": ch, "pcl": pcl})
    return(SD)


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


def _getCfgLog(node):
    parms = {}
    for m in node.getMembers():
        #  if m.usage in ("TEXT", "NUMERIC"):
            try:
                k = m.getNodeName().lower()
                v = m.data()
                if v is not None:
                    parms[k] = _sup.cp(v)
            except:
                pass
    return(parms)


def _sectionDict(node, secs, kks, time, path):
    '''for interenal use'''
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
                    path.set_stream(dev.getNodeName().lower())
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
    desc = {}
    nid = ch.getData().Nid
    if ch.getNumDescendants()>0:
        _treeToDict(ch, desc)
    if ch.usage == "SIGNAL":
        desc["name"] = ch.getNodeName().lower()
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


def _treeToDict(node, Dict):
    if node.usage not in ['ACTION', 'TASK', 'SIGNAL']:  # exclude by usage
        name = node.getNodeName().lower()
        try:
            data = node.data().tolist()
            try:
                data = data.tolist()
            except:
                pass
        except:
            data = None

        if node.getNumDescendants()>0:
            sDict = {}
            for d in node.getDescendants():
                sDict = _treeToDict(d, sDict)
            if len(sDict.keys()):
                if data is not None:
                    sDict["$value"] = data
                Dict[name] = sDict
                return(Dict)
        if data is not None:
            Dict[name] = data
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
                dimof = sig.getSegmentDim(seg).data()
                r = _if.write_data(path, data, dimof, t0)
                R.append({"seg": seg, "rds": r})
                print(seg, r)
                if r.status_code >= 400:
                    print(r.content)
                    return(R)
        else:
            data = sig.data().tolist()
            dimof = sig.dim_of()
            R = _if.write_data(path, data, dimof, t0)
            print(R)
            if R.status_code >= 400:
                print(R.content)
        return(R)
    elif len(signals) > 1:
        R = []
        data = []
        dimof = []
        for sig in signals:
            sig = sig.evaluate()
            data.append(sig.data())
            dimof.append(sig.dim_of().data())
        wearegood = True
        for d in dimof[1:]:
            wearegood &= (dimof[0]==d).all()
        if not wearegood:
            raise(Exception('dimesions are not equal for all channels'))
        dimof = dimof[0]
        N = 100000
        for i in _ver.xrange(int((len(dimof)-1)/N+1)):
            R.append(_if.write_data(path, [d[i*N:(i+1)*N].tolist() for d in data], dimof[i*N:(i+1)*N]), t0)
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
