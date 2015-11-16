"""
archive.interface
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import os as _os
import mmap as _mmap
import json as _json
import numpy as _np
from threading import Thread


try:
    import h5py as _h5
except:
    print('WARNING: "h5py" package not found.\nImage upload will not be available')
from . import base as _base
from . import cache as _cache
from . import support as _sup
from . import version as _ver

SQcache = _cache.cache()

class URLException(Exception):
    def __init__(self,value):
        return value


def write_logurl(url, parms, time):
    if url.endswith('CFGLOG'):
        label = 'configuration'
    else:
        label = 'parms'
    log = {'label' : label,
           'values': [parms],
           'dimensions': [_base.TimeInterval(time).fromT.ns,-1]
           }
    return(post(url, json=log))  # , data=json.dumps(cfg)


def write_data(path, data, dimof, t0=0):
    # path=Path, data=numpy.array, dimof=numpy.array
    if not isinstance(data, _np.ndarray):
        raise Exception('write_data: data must be numpy.ndarray')
    dimof = _np.array(dimof)
    if dimof.dtype == float:
        dimof = (dimof*1e9).astype('uint64')
    dimof = dimof + t0
    if dimof.ndim == 0:  # we need to add one level
        dimof = [dimof.tolist()]
        data  = data.reshape(*(list(data.shape)+[1]))
    else:
        dimof = dimof.tolist()
    if data.ndim > 2:
        return(_write_vector(_base.Path(path), data, dimof))
    else:
        return(_write_scalar(_base.Path(path), data, dimof))


def _write_scalar(path, data, dimof):
    # path=Path, data=numpy.array, dimof=list of long
    dtype = str(data.dtype)
    if dtype in ['bool','int8','uint8','int16','uint16']:
        datatype='short'
    elif dtype in ['int32','uint32']:
        datatype='int'
    elif dtype in ['int64','uint64']:
        datatype='long'
    else:
        datatype='float'
    jdict = {'values': data.tolist(), 'datatype':datatype, 'dimensions': dimof}
    return(post(path.url_datastream(), json=jdict))


def _write_vector(path, data, dimof):
    # path=Path, data=numpy.array, dimof=list of long
    dtype = str(data.dtype)
    stream = path.stream
    tmpfile = _ver.tmpdir+"archive_"+stream+'_'+str(dimof[0])+".h5"
    try:
        with _h5.File(tmpfile, 'w') as f:
            g = f.create_group('data')
            g.create_dataset(stream, data=data.transpose(*list(range(1,data.ndim)+[0])).tolist(), dtype=dtype,
                             compression="gzip")
            g.create_dataset('timestamps', data=list(dimof), dtype='int64',
                             compression="gzip")
        headers = {'Content-Type': 'application/x-hdf'}
        link = path.url_streamgroup()+'?dataPath=data/'+stream+'&timePath=data/timestamps'
        with open(tmpfile, 'rb') as f:
            return(post(link, headers=headers, data=f))
    finally:
        try:
            _os.remove(tmpfile)
        except:
            print('could not delete file "%s"' % tmpfile)
            pass

def read_signal(path, time, t0=0, **kwargs):
    path = _base.Path(path)
    time = _base.TimeInterval(time)
    _cache.cache().clean()
    chunk = kwargs.pop('chunk', None)
    if chunk:
        try:
            rawset = _readchunks(path, time, chunk, **kwargs)
        except Exception as exc:
            rawset = _readraw(path, time, **kwargs)
            rawset[2] = exc.message
    else:
        key = _cache.getkey(path, time, **kwargs)
        rawset = SQcache.get(key)
        if rawset is None:
            _sup.debug('get web-archive: '+key)
            cacheable = time.uptoT>_base.Time('now') or time.uptoT<0
            rawset = _readraw(path, time, **kwargs)
            if cacheable and rawset is not None:
                SQcache.set(key, rawset)
    if rawset is None: return None
    return _base.createSignal(rawset[0], rawset[1], t0, rawset[2])


def _readraw(path, time, **kwargs):
    try:
        stream = get_json(path.url_data(), time=time, **kwargs)
    except _ver.urllib.HTTPError:
        return [[],[],None]
    return [_base.tonumpy(stream['values']),_np.array(stream['dimensions']),str(stream.get('unit', 'unknown'))]


def _readchunks(path, time, chunk=None, **kwargs):
    path = _base.Path(path)
    hsh = hash(path)
    if chunk is None:
        chunk = SQcache.getchk(hsh)
    else:
        chunk = int(chunk)
    time = _base.TimeInterval(time)
    if chunk is None:
        return _readraw(path, time, **kwargs)
    else:
        def task(out,times,idx,i):
            while i<len(out):
                time = [ times[out[i][0]][0] , times[out[i][1]][1] ]
                out[i][2] = _readraw(path, time, **kwargs)
                i = idx[0] = idx[0]+1
        start= int(time.fromT.ns/chunk)*chunk
        times= [[s,s+chunk-1] for s in range(start,time.uptoT.ns+1,chunk)]
        idxs = list(range(len(times)))
        keys = [_cache.getkey(hsh, times[i], False, **kwargs) for i in idxs]
        sets = [SQcache.get(key) for key in keys]
        load = [s is None for s in sets]
        blck = []
        gab = True
        for i in idxs:
            if load[i]:
                if gab:
                    blck.append([i,i,[]])
                else:
                    blck[-1][1] = i
            gab = not load[i]
        tmax = 64
        idx = [tmax-1]
        threads = [Thread(target=task, args=(blck, times, idx, i)) for i in range(min(tmax,len(blck)))]
        last = getLast(path,time)['upto']
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for b in blck:
            for i in range(b[0],b[1]+1):
                sel = _np.where((b[2][1]>=times[i][0]) & (b[2][1]<=times[i][1]))[0]
                sets[i] = [b[2][0][sel],b[2][1][sel],b[2][2]]
        for i in idxs[:-1]:  # cache all but most recen
            if load[i] and sets[i] is not None and times[i][1]<last:
                SQcache.set(keys[i],sets[i],604800)  # last one week
        sets = [s for s in sets if s is not None]
    dat = _np.concatenate(tuple(s[0] for s in sets))
    dim = _np.concatenate(tuple(s[1] for s in sets))
    start= 0
    stop = len(dim)
    while dim[start]<time.fromT.ns: start+=1
    while dim[stop-1]>time.uptoT.ns: stop-=1
    dat = dat[start:stop]
    dim = dim[start:stop]
    SQcache.setchk(hsh,chunk)
    return [dat, dim, sets[-1][2]]


def get_json(url, **kwargs):
    class reader(object):
        def __init__(self, value):
            self.value = value
        def read(self,*argin):
            return _ver.tostr(self.value.read(*argin))
    url = _base.Path(url).url(-1, **kwargs)
    _sup.debug(url,5)
    headers = {'Accept': 'application/json'}
    handler = get(url, headers)
    if handler.getcode() != 200:
        raise Exception('request failed: code '+str(handler.getcode()))
    if handler.headers.get('content-type') != 'application/json':
        raise Exception('requested content-type mismatch: ' +
                        handler.headers.get('content-type'))
    return _json.load(reader(handler), strict=False)

def getLast(path, time):
    j = get_json(_base.filter(path, time))
    last = _ver.tostr(j['_links']['children'][-1]['href']).split('?')[1].split('&')
    last = [s.split('=') for s in last]
    last = dict((s[0],int(s[1])) for s in last)
    return last

def post(url, headers={}, data=None, json=None):
    if json is not None:
        data = _json.dumps(json)
        headers['content-type'] = 'application/json'
    if isinstance(data, (_ver.file)):
        _sup.debug(data.name)
        data = _mmap.mmap(data.fileno(), 0, access=_mmap.ACCESS_READ)
    else:
        _sup.debug(data)
        data = _ver.tobytes(data)
    _sup.debug(url)
    result = get(url, headers, data)
    if isinstance(data,(_mmap.mmap)):
        data.close()
    return result


def get(url, headers={}, *data):
    req = _ver.urllib.Request(url)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        handler = _ver.urllib.urlopen(req, *data)
    except _ver.urllib.HTTPError as err:
        print(err.reason)
        print(err.read())
        raise(err)
    return(handler)


def parseXML(toparse):
    import xml.etree.cElementTree as ET
    from re import compile as recmp
    re = recmp('(?:\{[^\}]*\}|)(.*)')

    def addvalue(res, name, value):
        if name in res.keys():
            if isinstance(res[name], list):
                res[name].append(value)
            else:
                res[name] = [res[name], value]
        else:
            res[name] = value

    def xmltodict(node):
        res = {}
        if len(node):
            for n in list(node):
                name = re.match(n.tag).group(1)
                addvalue(res, name, xmltodict(n))
        else:  # is leaf
            res = node.text
        return res
    tree = ET.fromstring(toparse.encode('utf-8'))
    return xmltodict(tree)


def read_parlog(path, time=[-1, 0], **kwargs):
    def rmfield(dic, field):
        if field in dic.keys():
            del(dic[field])

    def dict2list(d):
        l = len(d)*[None]
        for k, v in d.items():
            l[int(k[1:-1])] = v
        return l

    def integrateChLst(chans):
        chtmp = parseXML(chans['xmlDescription'])['channel']
        active = chans.get('active', None)
        pQ = chans.get('physicalQuantity', None)
        chans = [{} for i in chtmp]
        for ch in chtmp:
            ch['active'] = active
            ch['physicalQuantity'] = pQ
            i = int(ch['channelNumber'])
            rmfield(ch, 'channelNumber')
            for k, v in ch.items():
                if v is not None:
                    chans[i][k] = _sup.cp(v)
        return chans
    path =  _base.Path(path)
    par = get_json(path.url_parlog(), time=time, **kwargs)
    if not isinstance(par, dict):
        raise Exception('parlog not found!')
    par = par['values'][-1]
    if 'chanDescs' in par.keys():
        cD = par['chanDescs']
        if isinstance(cD, dict):
            cD = dict2list(cD)
        if isinstance(cD, list) and len(cD) == 1 and 'xmlDescription' in cD[0].keys():
            cD = integrateChLst(cD[0])
        par['chanDescs'] = cD
    return par


def read_cfglog(path, time=[-1, 0], **kwargs):
    path =  _base.Path(path)
    par = get_json(path.url_cfglog(), time=time, **kwargs)
    if not isinstance(par, dict):
        raise Exception('cfglog not found!')
    return par


def read_jpg_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.jpg?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    r = get(link)
    return r


def read_png_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.png?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    r = get(link)
    return r


def read_raw_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.json?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    r = get(link)
    return r
