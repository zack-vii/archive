"""
archive.MDSupload
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus
import re
from archive.base import Unit, Time, Path
from archive.support import error, cp, ndims
from archive.interface import write_logurl, write_data, write_image
import archive.version as _ver

archivedb = '/Test'  # ArchiveDB


def upload(names=['QMC', 'QMR', 'QRN', 'QSW', 'QSX'], shot=0, treename='W7X'):
    '''
    upload(['KKS'], shot=0, treename='W7X')
    uploads all sections of a given tree
    e.g.: D=upload(['QSW'], 2, 'W7X')
    '''
    SD = []
    tree = MDSplus.Tree(treename, shot)
    t0 = Time(tree.getNode('\TIME:IDEAL.T0').data())
#    t1 = Time(tree.getNode('\TIME:IDEAL.T1').data())
#    time = Time().ns-1000000000
    path = Path(archivedb+'/raw/W7X')
    for name in names:
        kks = tree.getNode(name)
        cfg = getCfgLog(kks)
        try:
            data = kks.getNode('DATA')
            secdict = {}
            if data.getNumDescendants():
                for sec in data.getDescendants():
                    path.set_streamgroup(name+'_'+sec.getNodeName().lower())
                    pcl = write_logurl(path.url_cfglog(), cfg, t0)
                    ch = SectionDict(sec, secdict, kks, t0, path)
                    SD.append({"ch": ch, "pcl": pcl})
        except:
            print(error())
    return(SD)


def SectionDict(node, secs, kks, time, path):
    '''
    tracks the
    '''
    f = re.compile('(?<=\.HARDWARE[:\.])([^\.:]+)')
    chans = {}
    devs = {}
    CH = []
    try:
        if node.getNumDescendants():
            for d in node.getDescendants():
                ChannelDescs(d, chans)
        secs[str(node.getNodeName())] = chans
        HW = kks.getNode('HARDWARE')
        if HW.getNumDescendants():
            for dev in HW.getDescendants():
                devs[dev.Nid] = []
            for ch in chans.keys():
                devName = f.search(str(kks.tree.getNode(ch).FullPath)).group(0)
                dev = HW.getNode(devName)
                devs[dev.nid].append(ch)
            for dev in HW.getDescendants():
                if len(devs[dev.Nid]):
                    path.set_stream(dev.getNodeName().lower())
                    par, sig = iterDevices(dev, devs[dev.Nid], chans)
                    rpl = write_logurl(path.url_parlog(), par, time)
                    rd = write_signals(path, sig, time)
                    CH.append({"path": path.path(),
                               "par": par, "sig": sig, "rpl": rpl, "rd": rd})
    except:
        print(error())
    return(CH)


def ChannelDescs(ch, dic):
    desc = {}
    nid = ch.getData().Nid
    if ch.getNumDescendants():
        treeToDict(ch, desc)
    if ch.usage == "SIGNAL":
        desc["name"] = ch.getNodeName().lower()
    dic[nid] = desc


def iterDevices(node, chlist, chans):
    parms = {}
    signals = []
    descs = []
    if node.getNumDescendants():
        for des in node.getDescendants():
            sig = None
            if des.usage == "SIGNAL":
                sig = des
            elif des.usage == "STRUCTURE":
                if des.getNumMembers():
                    for m in des.getMembers():
                        if m.usage == 'SIGNAL':
                            sig = m
            if sig is None:
                treeToDict(des, parms)
            elif sig.Nid in chlist:
                descs.append(SignalDict(des, sig, chans))
                signals.append(sig)
    parms["chanDescs"] = descs
    return(parms, signals)


def SignalDict(node, sig, dic={}):
    desc = {}
    desc['name'] = node.getNodeName().lower()
    desc["active"] = int(node.isOn())
    try:
        desc["physicalQuantity"] = {'type': Unit(sig, 1)}
    except:
        pass
    nid = sig.Nid
    if node.getNumDescendants():
        for des in node.getDescendants():
            if not des.Nid == nid:
                treeToDict(des, desc)
    if nid in dic.keys():
        for k, v in dic[nid].items():
            desc[k] = v
    return(desc)


def treeToDict(node, Dict):
    if node.usage not in ['ACTION', 'TASK', 'SIGNAL']:  # exclude by usage
        name = node.getNodeName().lower()
        try:
            data = cp(node.data())
        except:
            try:
                data = str(node.getData())
            except:
                data = None
        if node.getNumDescendants():
            sDict = {}
            if node.getNumDescendants():
                for d in node.getDescendants():
                    treeToDict(d, sDict)
            if len(sDict.keys()):
                if data is not None:
                    sDict["$value"] = data
                Dict[name] = sDict
                return
        if data is not None:
            Dict[name] = data


def write_signals(path, signals, t0):
    t0 = Time(t0).ns

    def writedata(data, dimof, unit, path=path, t0=t0):
        dimof = dimof.data().tolist()
        if unit != 'ns':
            dimof = [int(t*1e9+t0) for t in dimof]
        if ndims(data) > 2:
            return(write_image(path, data, dimof))
        elif ndims(data) > 1:
            return(write_image(path, data, dimof))
        else:
            return(write_data(path, [data], dimof))
    if len(signals) == 1:
        sig = signals[0]
        R = []
        if sig.getNumSegments():
            unit = Unit(sig.getSegmentDim(0), 1)
            for seg in _ver.xrange(sig.getNumSegments()):
                data = sig.getSegment(seg).data().tolist()
                dimof = sig.getSegmentDim(seg)
                r = writedata(data, dimof, unit)
                R.append({"seg": seg, "rds": r})
                print(seg, r)
                if r.status_code >= 400:
                    print(r.content)
                    return(R)
        return(R)
    elif len(signals) > 1:
        data = []
        dim = None
        for sig in signals:
            try:
                dim = sig
                data.append(sig.data().tolist())
            except:
                data.append([])
        if dim is None:
            dimof = t0
        else:
            dimof = dim.dim_of()
        return(writedata(data, dimof, dim.dim_of().unit))


def getCfgLog(node):
    parms = {}
    if node.getNumMembers():
        for m in node.getMembers():
            #  if m.usage in ("TEXT", "NUMERIC"):
                try:
                    k = m.getNodeName().lower()
                    v = m.data()
                    if v is not None:
                        parms[k] = cp(v)
                except:
                    pass
    return(parms)


def buildPath(node):  # , subsection=None):
    from re import split
    PathParts = split('::|:|\.|_', node.path.lstrip('\\'))
    KKS = PathParts[0]
    if PathParts[1] == 'EVAL':
        view = 'raw'  # will be cocking once supported
    else:
        view = 'raw'
    section = PathParts[-2].lower()
    groupname = PathParts[-1].lower()
    path = Path('/'.join([archivedb, view, 'W7X', KKS+'_'+section, groupname]))
    return(path)


def uploadNode(node):  # , subsection=None):
    '''
    node = TreeNode or (tree, shot, path) or path -> (tree='w7X', shot=0)
    e.g.:
    uploadNode(("sandbox", -1, "\\KKS_EVAL::TOP.RESULTS:MYSECTION:MYIMAGE"))
    '''
    if isinstance(node, (MDSplus.treenode.TreeNode)):
        tree = node.tree
    elif isinstance(node, (tuple, list)):
        tree = MDSplus.Tree(node[0], node[1])
        node = tree.getNode(node[2])
    else:
        tree = MDSplus.Tree('W7X', 0)
        node = tree.getNode(node)
    path = buildPath(node)  # , subsection)
    t0 = Time(tree.getNode('\TIME.T0:IDEAL').data())
    t1 = Time(tree.getNode('\TIME.T1:IDEAL').data())
    par, sig = iterDevices(node)
    if node.usage == "SIGNAL":
        if len(sig):  # prepend
            sig = [node] + sig
            par["chanDescs"] = [SignalDict(node, node)] + par["chanDescs"]
        else:  # replace
            sig = [node]
            par = {"chanDescs": [SignalDict(node, node)]}
    par["shot"] = int(tree.shot)
    r = [None, None]
    r[0] = write_logurl(path.url_parlog(), par, t0)
    r[1] = write_signals(path, sig, t1)
    print(r)
    if not r[0].ok:
        print(r[0].content)
    if not r[1].ok:
        print(r[1].content)
    return r
