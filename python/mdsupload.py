"""
archive.MDSupload
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus
import re
from archive.base import Unit, Time, TimeInterval, Path
from archive.support import error, cp, ndims
from archive.interface import write_logurl, write_data, write_image
from archive.version import xrange

archivedb = '/Test'  # ArchiveDB


def upload(names=['SINWAV'], shot=0, treename='W7X'):
    '''
    upload(['KKS'], shot=0, treename='W7X')
    uploads all sections of a given tree
    e.g.: D=upload(['QMC', 'QMR', 'QRN', 'QSW', 'QSX'], 2, 'W7X')
    '''
    SD = []
    w7x = MDSplus.Tree(treename, shot)
#    time = TimeInterval([w7x.getNode('\TIME:T0:IDEAL'),
#              w7x.getNode('\TIME:T4:IDEAL'),
#              w7x.getNode('\TIME:T1:IDEAL')])
    time = TimeInterval(w7x.getNode('\TIME'))
    time = TimeInterval()  # TODO
    path = Path(archivedb+'/raw/W7X')
    for name in names:
        kks = w7x.getNode(name)
        cfg = getCfgLog(kks)
        data = kks.getNode('DATA')
        secdict = {}
        for sec in data.getDescendants():
            print(sec)
            path.set_streamgroup(name+'_'+sec.getNodeName().lower())
            try:
                pcl = write_logurl(path.url_cfglog(), cfg, time.fromT)
            except:
                pcl = error()
            ch, secdict = SectionDict(sec, secdict, kks, time, path)
            SD.append({"ch": ch, "pcl": pcl})
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
        for d in node.getDescendants():
            chans = ChannelDescs(d, chans)
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
                    par, sig = iterDevices(dev, devs[dev.Nid], chans)
                    try:
                        rpl = write_logurl(path.url_parlog(), par, time.fromT)
                    except:
                        rpl = error()
                    rd = write_signals(path, sig, time.t0T)
                    CH.append({"path": path.path(),
                               "par": par, "sig": sig, "rpl": rpl, "rd": rd})
    except:
        error()
    return(CH, secs)


def ChannelDescs(ch, dic):
    desc = {}
    nid = ch.getData().Nid
    if ch.getNumDescendants()>0:
        treeToDict(ch, desc)
    if ch.usage == "SIGNAL":
        desc["name"] = ch.getNodeName().lower()
    dic[nid] = desc
    return dic

def iterDevices(node, chlist, chans):
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
        if node.getNumDescendants()>0:
            sDict = {}
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
    R = []
    t0 = Time(t0).ns
    def writedata(data, dim, path=path, t0=t0):
        if dim.dtype==float:
            dim = (dim*1e9).astype('uint64')+t0
        if ndims(data) > 2:
            return(write_image(path, data, dim.tolist()))
        elif ndims(data) > 1:
            return(write_data(path, data, dim.tolist()))
        else:
            return(write_data(path, [data], dim.tolist()))
    if len(signals) == 1:
        sig = signals[0]
        if sig.isSegmented():
            for seg in xrange(sig.getNumSegments()):
                data = sig.getSegment(seg).data()
                dimof = sig.getSegmentDim(seg).data()
                r = writedata(data, dimof)
                R.append({"seg": seg, "rds": r})
                print(seg, r)
                if r.status_code >= 400:
                    print(r.content)
                    return(R)
        else:
            data = sig.data().tolist()
            dimof = sig.dim_of()
            R = writedata(data, dimof)
            print(R)
            if r.status_code >= 400:
                print(R.content)
        return(R)
    elif len(signals) > 1:
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
        for i in xrange(int((len(dimof)-1)/N+1)):
            R.append(writedata([d[i*N:(i+1)*N].tolist() for d in data], dimof[i*N:(i+1)*N]))
        return R


def getCfgLog(node):
    parms = {}
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
