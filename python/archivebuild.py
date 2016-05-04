"""
archive.archivebuild
=======================================
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus as _mds
import re as _re
from . import base as _b
from . import classes as _cls
from . import interface as _if
from . import support as _sup
from . import version as _ver

def addValue(tree='archive', shot=-1):
    valueDB = {
        'CDSD104:DMD236:CH0' :'Build_With_Units(polyval($VALUE, [146.64,42.133,116.76]),"kW")',  # RF_C1
        'CDSD101:DMD229:CH0' :'Build_With_Units(polyval($VALUE, [67.552,62.879,45.399]),"kW")',  # RF_C5
        'CDSD101:DMD229:CH8' :'Build_With_Units(polyval($VALUE, [72.739,124.39,17.327]),"kW")',  # RF_D5
        'CDSD108:DMD240:CH0' :'Build_With_Units(polyval($VALUE, [26.759,190.34,29.842]),"kW")',  # RF_A1
        'CDSD108:DMD240:CH8' :'Build_With_Units(polyval($VALUE, [112.58,8.9552,32.902]),"kW")',  # RF_B1
        'CDSD106:DMD237:CH8' :'Build_With_Units(polyval($VALUE, [96.270,148.65,107.20]),"kW")',  # RF_B5
        'CDSD104:DMD236:CH6' :'Build_With_Units(polyval($VALUE, [33.512,712.72]),"kW")',  # Bolo_C1
        'CDSD101:DMD229:CH6' :'Build_With_Units(polyval($VALUE, [-43.156,684.25]),"kW")', # Bolo_C5
        'CDSD101:DMD229:CH14':'Build_With_Units(polyval($VALUE, [-10.469,161.96]),"kW")', # Bolo_D5
        'CDSD108:DMD240:CH6' :'Build_With_Units(polyval($VALUE, [-0.3427,1319.2]),"kW")', # Bolo_A1
        'CDSD106:DMD237:CH14':'Build_With_Units(polyval($VALUE, [85.734,996.06]),"kW")',  # Bolo_B5
        'CDSD106:DMD237:CH14':'Build_With_Units(polyval($VALUE, [85.734,996.06]),"kW")',  # Bolo_B5
        }
    with _mds.Tree(tree,shot,'edit') as arc:
        for k,v in valueDB.iteritems():
            node = arc.getNode(k)
            try:    vnode = node.addNode('$VALUE','AXIS')
            except: vnode = node.getNode('$VALUE')
            vnode.record = _mds.TdiCompile(v)
        arc.write()

def modECE(tree='archive', shot=-1):
    with _mds.Tree(tree,shot,'edit') as arc:
        stream = arc.CDSD16007.DRPD17547
        for i in range(32):
            node = stream.getNode('CH%d'%i)
            try:    ece = node.addNode('$ECE','SIGNAL')
            except: ece = node.getNode('$ECE')
            ece.record = _mds.TdiCompile('ECE($)',(node,))
        arc.write()

def advNode(node,name,usage="STRUCTURE"):
    try:    return node.addNode(name,usage)
    except: return node.getNode(name)

def addTest(tree='archive', shot=-1):
    def buildSignalPath(node,path):
        if not isinstance(path, (list,tuple)):
            path = path.replace(':','.').split('.')
        for name in path[0:-1]:
            node = advNode(node,name,"STRUCTURE")
        return advNode(node,path[-1],"SIGNAL")
    nodes = [("ECRH","TotalPower","CBG_ECRH/TotalPower_DATASTREAM/0")]
    with _mds.Tree(tree,shot,'edit') as arc:
        test = advNode(arc,'TEST')
        url = advNode(test,'$URL','TEXT')
        url.record = "http://archive-webapi.ipp-hgw.mpg.de/Test/codac/W7X/"
        for node in nodes:
            sig = buildSignalPath(test,node[0])
            sig.record = archive_channel(sig)
            advNode(sig,"$NAME","TEXT").record = node[1]
            advNode(sig,"$URL","TEXT").record = _mds.TdiCompile(url.path+'//"'+node[2]+'"')
        arc.write()

def build(tree='archive', shot=-1, T='now', rootpath='/ArchiveDB/codac/W7X',tags=False):
    re = _re.compile('[A-Z]+[0-9]+')
    cap = _re.compile('[^A-Z0-9]')
    def addProject(T, node, nname, name='', url=None):
        _sup.debug(nname,1)
        if name != '':
            node = node.addNode(nname, 'STRUCTURE')
            if re.match(nname) is not None:
                if tags: node.addTag(nname)
            node.addNode('$NAME', 'TEXT').putData(name)
        if url is None: url = archive_url(node)
        urlNode = node.addNode('$URL', 'TEXT').putData(url)
        b = _cls.browser(str(urlNode.data()))
        streamgroups = b.list_streamgroups()
        for s in streamgroups:
            try:
                cnname = s.split('.')
                cnname[0] = cap.sub('', cnname[0])
                addStreamgroup(T, node, ''.join(cnname), s,tags=tags)
            except:
                _sup.error()

    def addShotsDB(tree):
        mds = tree.addNode('MDS','STRUCTURE')
        node = mds.addNode('S','SIGNAL')
        node.putData(archive_stream(node))
        node.addNode('$NAME','TEXT').record = 'Shots'
        node.addNode('$URL', 'TEXT').record = archive_url(node)
        for i in range(7):
            ch = node.addNode('CH%d' % i,'SIGNAL')
            ch.putData(archive_channel(ch))
            name = 'shot' if i==0 else 'T%d' % i
            ch.addNode('$NAME','TEXT').putData(name)
            ch.addNode('$IDX','NUMERIC').putData(i)


    def addStreamgroup(T, node, nname, name='', url=None,tags=False):
        _sup.debug(nname,2)
        if name != '':
            node = node.addNode(nname, 'STRUCTURE')
            # node is stream group
            if re.match(nname) is not None:
                if tags: node.addTag(nname)
            node.addNode('$NAME', 'TEXT').putData(name)
        if url is None:
            url = archive_url(node)
        urlNode = node.addNode('$URL', 'TEXT').putData(url)
        node.addNode('$CFGLOG', 'ANY').putData(archive_cfglog(node))
        b = _cls.browser(str(urlNode.data()))
        streams = b.list_streams()
        for stream, content in streams.items():
            try:
                cnname = stream.split('.')
                cnname[0] = cap.sub('', cnname[0])
                cnname = ''.join(cnname)
                if 'DATASTREAM' in content:
                    addStream(T, node, cnname, stream, tags=tags)
                elif 'PARLOG' in content:
                    plogNode = node.addNode(cnname, 'STRUCTURE')
                    if re.match(cnname) is not None:
                        _sup.debug(cnname,2)
                        if tags: plogNode.addTag(cnname)
                    plogNode.addNode('$URL', 'TEXT').putData(archive_url(plogNode))
                    plogNode.addNode('$NAME', 'TEXT').putData(stream)
                    addParlog(T, plogNode)
            except:
                _sup.error()


    def addStream(T, node, nname, name='', url=None,tags=False):
        _sup.debug(nname,2)
        if name != '':
            node = node.addNode(nname, 'SIGNAL')
            if re.match(nname) is not None:
                if tags: node.addTag(nname)
            node.addNode('$NAME', 'TEXT').putData(name)
        node.putData(archive_stream(node))
        if url is None: url = archive_url(node)
        node.addNode('$URL', 'TEXT').putData(url)
        chanDescs = addParlog(T, node)
        for i in _ver.xrange(len(chanDescs)):
            addChannel(node, 'CH'+str(i), i, chanDescs[i])


    def addParlog(T, node):
        parNode = node.addNode('$PARLOG', 'ANY')
        parNode.putData(archive_parlog(node))
        try:
            url = str(node.getNode('$URL').data())
            dist = _if.read_parlog(url, T)
        except:
            _sup.error()
            return []
        if not isinstance(dist, dict): return []
        if 'chanDescs' in dist.keys():
            chanDescs = dist['chanDescs']
            del(dist['chanDescs'])
        else:
            chanDescs = []
        for k, v in dist.items():
            if v is None: continue
            try: addField(parNode,k,v)
            except:
                _sup.debug((parNode.MinPath,k,v),1)
                _sup.error()
        return chanDescs


    def addChannel(node, nname, idx, chan={}, url=None):
        node = node.addNode(nname, 'SIGNAL')
        node.putData(archive_channel(node))
        if url == None: url = archive_url(node)
        node.addNode('$URL', 'TEXT').putData(url)
        node.addNode('$IDX', 'NUMERIC').putData(idx)
        nameNode = node.addNode('$NAME', 'TEXT')
        for k, v in chan.items():
            try:
                if k == 'physicalQuantity': pass
                elif k == 'active':
                    v = int(v)
                    node.setOn(v != 0)
                elif k == 'name':
                    nameNode.putData(_ver.tobytes(v))
                else:
                    addField(node,k,v)
            except:
                _sup.debug((node.MinPath,k,v),1)
                _sup.error()
        addScale(node,'scaled')

    def addScale(node, nname, url=None):
        node = node.addNode('$'+nname,'SIGNAL')
        node.putData(archive_scaled(node))

    def addField(node,name,v):
        k = _sup.fixname12(name)
        if isinstance(v, (_ver.basestring, )):
            pn = node.addNode(k, 'TEXT').putData(_ver.tobytes(v))
        elif isinstance(v, (int, float)):
            pn = node.addNode(k, 'NUMERIC').putData(v)
        elif isinstance(v, (list,)):
            if isinstance(v[0], _ver.numbers):
                pn = node.addNode(k, 'NUMERIC').putData(_mds.makeArray(v))
            else:
                pn = node.addNode(k, 'ANY').putData(v)
        elif isinstance(v, (dict,)):
            if '['+str(len(v)-1)+']' in v.keys():
                v = [v['['+str(i)+']'] for i in _ver.xrange(len(v))]
                if all(isinstance(vi, _ver.numbers) for vi in v):
                    pn = node.addNode(k, 'NUMERIC').putData(_mds.makeArray(v))
                else:
                    try:
                        pn = node.addNode(k, 'ANY').putData(v)
                    except:
                        pn.putData([str(i) for i in v])
            else:
                try:
                    pn = node.addNode(k,'TEXT')
                    for vk,vv in v.items():
                        addField(pn,vk,vv)
                    try: pn.setUsage('STRUCTURE')
                    except: _sup.debug(pn.minpath,2)
                except:
                    pn.putData(str(v))
        pn.addNode('$NAME','TEXT').record = name

    name = "codac"
    path = _b.Path(rootpath).url()
    with _mds.Tree(tree, shot, 'new') as arc:
        arc.getNode('\TOP').setIncludeInPulse(False)
        T = _b.Time(T)
        sys = arc.addNode('$SYSTEM','STRUCTURE')
        sys.addNode('VERSION','TEXT').putData(T.utc)
        sys.addNode('FUN_URL','TEXT').putData('archive_url')
        sys.addNode('FUN_PROGRAM','TEXT').putData('archive_program')
        sys.addNode('FUN_CFGLOG','TEXT').putData('archive_cfglog')
        sys.addNode('FUN_PARLOG','TEXT').putData('archive_parlog')
        sys.addNode('FUN_STREAM','TEXT').putData('archive_image')
        sys.addNode('FUN_CHANNEL','TEXT').putData('archive_signal')
        sys.addNode('FUN_SCALED','TEXT').putData('archive_signal')
        prog = arc.addNode('$PROGRAM','STRUCTURE')
        n = prog.addNode('ID','NUMERIC');n.putData(archive_program(n))
        n = prog.addNode('TIME','NUMERIC');n.putData(archive_program(n))
        n = prog.addNode('NAME','TEXT');n.putData(archive_program(n))
        n = prog.addNode('DESCRIPTION','TEXT');n.putData(archive_program(n))
        n = prog.addNode('TRIGGER','NUMERIC');n.putData(archive_program(n))
        addShotsDB(arc)
        addProject(T, arc, name, '', path)
        arc.write()
    addValue(tree, shot)
    addTest(tree, shot)
    modECE(tree, shot)
    _mds.Tree(tree, shot).compressDatafile()


def archive_url(node):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_URL,$)', (node, ))

def archive_channel(channelNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_CHANNEL,$,_time)', (channelNode, ))

def archive_scaled(scaledNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_SCALED,$,_time)', (scaledNode, ))

def archive_stream(streamNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_STREAM,$,_time)', (streamNode, ))

def archive_parlog(streamNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_PARLOG,$,_time)', (streamNode, ))

def archive_cfglog(streamgroupNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_CFGLOG,$,_time)', (streamgroupNode, ))

def archive_program(programNode):
    return _mds.TdiCompile('EXT_FUNCTION(*,$SYSTEM:FUN_PROGRAM,$,_time)', (programNode, ))
