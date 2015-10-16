"""
archive.archivebuild
=======================================
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import MDSplus as _mds
import re as _re
from . import base as _base
from . import classes as _cls
from . import interface as _if
from . import support as _sup
from . import version as _ver


def build(tree='test', shot=-1, T=0):
    name = "raw"
    path = _base.Path("/ArchiveDB/raw/W7X").url()
    with _mds.Tree(tree, shot, 'New') as arc:
        T = _base.Time(T)
        arc.addNode('$VERSION','TEXT').putData(T.utc)
        addProject(T, arc, name, '', path)
        arc.write()
        arc.close


def addProject(T, node, nname, name='', url=None):
    re = _re.compile('[A-Z]+[0-9]+')
    cap = _re.compile('[^A-Z]')
    if name != '':
        node = node.addNode(nname, 'STRUCTURE')
        if re.match(nname) is not None:
            print(nname)
            node.addTag(nname)
        node.addNode('$NAME', 'TEXT').putData(name)
    if url is None:
        url = archive_url(node)
    urlNode = node.addNode('$URL', 'TEXT')
    urlNode.putData(url)
    url = str(urlNode.data())
    b = _cls.browser(url)
    streamgroups = b.list_streamgroups()
    for s in streamgroups:
        cnname = s.split('.')
        cnname[0] = cap.sub('', cnname[0])
        addStreamgroup(T, node, ''.join(cnname), s)


def addStreamgroup(T, node, nname, name='', url=None):
    re = _re.compile('[A-Z]+[0-9]+')
    cap = _re.compile('[^A-Z]')
    if name != '':
        node = node.addNode(nname, 'STRUCTURE')
        # node is stream group
        if re.match(nname) is not None:
            print(nname)
            node.addTag(nname)
        node.addNode('$NAME', 'TEXT').putData(name)
    if url is None:
        url = archive_url(node)
    urlNode = node.addNode('$URL', 'TEXT')
    urlNode.putData(url)
    node.addNode('$CFGLOG', 'ANY').putData(archive_cfglog(node))
    url = str(urlNode.data())
    b = _cls.browser(url)
    streams, contents = b.list_streams()
    for stream, content in zip(streams, contents):
        cnname = stream.split('.')
        cnname[0] = cap.sub('', cnname[0])
        cnname = ''.join(cnname)
        if 'DATASTREAM' in content:
            addStream(T, node, cnname, stream)
        elif 'PARLOG' in content:
            plogNode = node.addNode(cnname, 'STRUCTURE')
            if re.match(cnname) is not None:
                print(cnname)
                try:
                    plogNode.addTag(cnname)
                except:
                    print(cnname)
            plogNode.addNode('$URL', 'TEXT').putData(archive_url(plogNode))
            plogNode.addNode('$NAME', 'TEXT').putData(stream)
            addParlog(T, plogNode)


def addStream(T, node, nname, name='', url=None):
    re = _re.compile('[A-Z]+[0-9]+')
    if name != '':
        node = node.addNode(nname, 'SIGNAL')
        if re.match(nname) is not None:
            print(nname)
            node.addTag(nname)
        node.addNode('$NAME', 'TEXT').putData(name)
    node.putData(archive_stream(node))
    if url is None:
        url = archive_url(node)
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
    if 'chanDescs' in dist.keys():
        chanDescs = dist['chanDescs']
        del(dist['chanDescs'])
    else:
        chanDescs = []
    if len(dist):
        for k, v in dist.items():
            if v is None:
                continue
            try:
                k = _sup.fixname12(k)
                if isinstance(v, (_ver.basestring, )):
                    parNode.addNode(k, 'TEXT').putData(_ver.tobytes(v))
                elif isinstance(v, (int, float)):
                    parNode.addNode(k, 'NUMERIC').putData(v)
                elif isinstance(v, (list,)) and isinstance(v[0], (int, float)):
                    parNode.addNode(k, 'NUMERIC').putData(v)
                elif isinstance(v, (dict,)):
                    pn = parNode.addNode(k, 'ANY')
                    if not '['+str(len(v)-1)+']' in v.keys():
                        pn.putData(str(v))
                    else:
                        v = [v['['+str(i)+']'] for i in _ver.xrange(len(v))]
                        try:
                            pn.putData(v)
                        except:
                            pn.putData([str(i) for i in v])
            except:
                print(node.MinPath,k,v)
                _sup.error()
    return chanDescs


def addChannel(node, nname, idx, chan={}, url=None):
    node = node.addNode(nname, 'SIGNAL')
    node.putData(archive_channel(node))
    if url == None:
        url = archive_url(node)
    node.addNode('$URL', 'TEXT').putData(url)
    nameNode = node.addNode('$NAME', 'TEXT')
    node.addNode('$IDX', 'NUMERIC').putData(idx)
    for k, v in chan.items():
        try:
            if k == 'physicalQuantity':
                pass
            elif k == 'active':
                v = int(v)
                node.setOn(v != 0)
            elif k == 'name':
                nameNode.putData(_ver.tobytes(v))
            else:
                k = _sup.fixname12(k)
                if isinstance(v, (str, )):
                    node.addNode(k, 'TEXT').putData(_ver.tobytes(v))
                elif isinstance(v, (int, float, list)):
                    node.addNode(k, 'NUMERIC').putData(v)
                else:
                    node.addNode(k, 'TEXT').putData(_ver.tobytes(v))
        except:
            print(k)
            print(v)
            _sup.error()


def archive_url(node):
    return _mds.TdiCompile('archive_url($)', (node, ))


def archive_channel(channelNode):
    return _mds.TdiCompile('archive_signal($, _time)', (channelNode, ))


def archive_stream(streamNode):
    return _mds.TdiCompile('archive_signal($, _time)', (streamNode, ))


def archive_parlog(streamNode):
    return _mds.TdiCompile('archive_parlog($, _time)', (streamNode, ))


def archive_cfglog(streamgroupNode):
    return _mds.TdiCompile('archive_cfglog($, _time)', (streamgroupNode, ))
