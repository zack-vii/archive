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
import re as _re
import threading as _th
try:  # the java interface for the archive
    import archive_java.signal as _aj
    _use_threads  = False
    _defaultCache = False
except:
    _aj = None
    _use_threads  = True
    _defaultCache = True

try:
    import h5py as _h5
except:
    print('WARNING: "h5py" package not found.\nImage upload will not be available')
from . import base as _b
from . import cache as _cache
from . import support as _sup
from . import png as _png
from . import version as _ver
from . import process as _prc

class URLException(Exception):
    def __init__(self,value):
        return value

def write_logurl(url, parms, Tfrom, Tupto=-1, timeout=None, retry=0):
    if url.endswith('CFGLOG'):
        label = 'configuration'
    elif url.endswith('PARLOG'):
        label = 'parms'
    else:
        raise Exception('write_logurl: URL must refer to either a CFGLOG or a PARLOG.')
    log = {'label' : label,
           'values': [parms],
           'dimensions': [max(0,_b.Time(Tfrom).ns),_b.Time(Tupto).ns]
           }
    try:
        result = post(url, json=log, timeout=None, retry=0)
    except _ver.urllib.HTTPError as result:
        _sup.debug(result)
    return _sup.requeststr(result)
def _prep_data(data, dimof, t0=0):
    if not isinstance(data, _np.ndarray):
        raise Exception('write_data: data must be numpy.ndarray')
    dimof = _np.array(dimof)
    if dimof.dtype in ['float64','float32','float16']:
        dimof = _b.dimof2w7x(dimof,t0)
    if dimof.ndim == 0:  # we need to add one level
        dimof = [dimof.tolist()]
        data  = data.reshape([1]+list(data.shape))
    else:
        dimof = dimof.tolist()
    return data,dimof

def write_data(path, data, dimof, t0=0, one=False, name=None, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=numpy.array
    data,dimof = _prep_data(data, dimof, t0)
    if data.ndim > (1 if one else 2):
        return(_write_vector(_b.Path(path), data, dimof, timeout=timeout, retry=retry))
    else:
        return(_write_scalar(_b.Path(path), data, dimof, timeout=timeout, retry=retry))

def write_data_async(path, data, dimof, t0=0, one=False, name=None, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=numpy.array
    data,dimof = _prep_data(data, dimof, t0)
    if data.ndim > (1 if one else 2):
        return(_write_vector_async(name,_b.Path(path), data, dimof, timeout=timeout, retry=retry))
    else:
        return(_write_scalar_async(name,_b.Path(path), data, dimof, timeout=timeout, retry=retry))


def mapDType(data):
    dtype = str(data.dtype)
    if dtype in ['bool','int8','uint8','int16','uint16']: return 'short'
    if dtype in ['int32','uint32']:                       return 'int'
    if dtype in ['int64','uint64']:                       return 'long'
    if dtype in ['float16','float32']:                    return 'float'
    else:                                                 return 'double'

def _write_scalar(path, data, dimof, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=list of long
    jdict = {'values': data.tolist(), 'datatype':mapDType(data), 'dimensions': dimof}
    result = post(path.url_datastream(), json=jdict, timeout=timeout, retry=retry)
    return _sup.requeststr(result)

def _write_scalar_async(name, path, data, dimof, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=list of long
    data = _json.dumps({'values': data.tolist(), 'datatype':mapDType(data), 'dimensions': dimof})
    res = _prc.Worker(name).put(post, path.url_datastream(), headers={'content-type':'application/json'}, data=data, timeout=timeout, retry=retry)
    return res

def writeH5(path,data,dimof,t0=0,idx=None):
    stream = path.stream
    data,dimof = _prep_data(data, dimof, t0)
    dtype = str(data.dtype)
    tmpfile = _ver.tmpdir+"archive_"+stream+'_'+str(dimof[0])
    if idx:  tmpfile += '_%d'%(idx,)
    tmpfile += ".h5"
    if data.ndim<3:
        data = data.reshape(list(data.shape)+[1])
    else:
        data = data.transpose(range(1,data.ndim)+[0])
    with _h5.File(tmpfile, 'w') as f:
        g = f.create_group('data')  # requires [row,col,time]
        g.create_dataset('timestamps', data=list(dimof), dtype='uint64',
                         compression="gzip")
        g.create_dataset(stream, data=data.tolist(), dtype=dtype,
                         compression="gzip")
    return tmpfile

def uploadH5(path, h5file, delete=False, timeout=None, retry=0):
    # path=Path, h5file=h5-file
    stream = path.stream
    try:
        headers = {'Content-Type': 'application/x-hdf'}
        link = path.url_streamgroup()+'/?dataPath=data/'+stream+'&timePath=data/timestamps'
        f = open(h5file, 'rb')
        try:
            result = post(link, headers=headers, data=f, timeout=timeout, retry=retry)
        finally:
            f.close()
    finally:
        if delete:
            try:
                _os.remove(h5file)
            except KeyboardInterrupt as ki: raise ki
            except:
                print('could not delete file "%s"' % h5file)
                pass
    result = _sup.requeststr(result)
    _sup.debug(result,3)
    return result

def _write_vector(path, data, dimof, t0=0, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=list of long
    for i in range(max(retry,0)+1):
        try:
            h5file = writeH5(path, data, dimof, t0=0, idx=i)
            result = uploadH5(path, h5file, True, timeout=timeout)
        except KeyboardInterrupt as ki: raise ki
        except _ver.urllib.socket.timeout as result:
            _sup.debug('timeout: %d (%s)'%(i,path))
            continue
        break
    if isinstance(result,(_ver.urllib.socket.timeout,)):
        raise result
    return result
def _write_vector_async(name,path, data, dimof, t0=0, timeout=None, retry=0):
    # path=Path, data=numpy.array, dimof=list of long
    for i in range(max(retry,0)+1):
        try:
            h5file = writeH5(path, data, dimof, t0=0, idx=i)
            result = _prc.Worker(name).put(uploadH5, path, h5file, True, timeout=timeout)
        except KeyboardInterrupt as ki: raise ki
        except _ver.urllib.socket.timeout as result:
            _sup.debug('timeout: %d (%s)'%(i,path))
            continue
        break
    if isinstance(result,(_ver.urllib.socket.timeout,)):
        raise result
    return result

def read_signal(path, time, **kwargs):
    path = _b.Path(path)
    time = _b.TimeInterval(time)
    cache = kwargs.pop('cache', _defaultCache)
    if cache:
        _cache.cache().clean()
        try:
            rawset = _readchunks(path, time, **kwargs)
        except KeyboardInterrupt as ki: raise ki
        except:
            _sup.error()
            rawset = _readraw(path, time, **kwargs)
    else:
        _sup.debug('get web-archive: '+path.path())
        rawset = _readraw(path, time, **kwargs)
    if rawset is None: return None
    return _b.createSignal(rawset[0], rawset[1], time.t0T, rawset[2],**kwargs)


def _readraw(path, time, **kwargs):
    if _aj is None: return _readraw_json(path, time, **kwargs)
    else:           return _readraw_java(path, time, **kwargs)
def _readraw_java(path, time, **kwargs):
    try:
        path = path.path_data(**kwargs)
        if path.startswith('/ArchiveDB'): path = path[10:]
        return list(_aj.readfull(path, time[0], time[1], 0x7FFFFFFF))
    except KeyboardInterrupt as ki: raise ki
    except Exception as exc:
        print(exc)
        return [[],[],str(exc)]
def _readraw_json(path, time, **kwargs):
    try:
        stream = get_json(path.url_data(), time=time, **kwargs)
    except _ver.urllib.HTTPError as exc:
        _sup.debug(exc,2)
        return [[],[],str(exc)]
    data = _b.tonumpy(stream['values'])
    if len(data.shape)==2:  # multichannel
        data = data.T.copy()
    return [data,_np.array(stream['dimensions']),str(stream.get('unit', 'unknown'))]


def _readchunks(path, time, **kwargs):
    SQcache = _cache.cache()
    path = _b.Path(path)
    hsh = _cache.gethash(path, **kwargs)
    time = _b.TimeInterval(time).ns[0:2]
    def task(out, times, idxs, idx, i):
        while i<len(idxs):
            out[idxs[i]] = _readraw(path, times[i], **kwargs)
            i = idx[0] = idx[0]+1
    # load all chunk in interval from cache
    sets = SQcache.gets(_cache.getkey(hsh, time, False, **kwargs))
    # find missing intervals
    blck = []
    idxs = []
    times = []
    tfrom = time[0]
    for i in _ver.xrange(len(sets)):
        if sets[i][1] > tfrom:  # insert container for new data
            blck.append([])
            idxs.append(i)
            times.append([tfrom, sets[i][1]-1])
        blck.append(sets[i][0])
        tfrom = sets[i][2]+1 #  from
    if tfrom<=time[1]:
        idxs.append(len(blck))
        blck.append([])
        times.append([tfrom, time[1]])

    del(sets)
    if len(idxs):  # not all data in cache
        def _cachechunk(data,btime,last):
            """stores chunks in cache 250-500 samples"""
            def store(pos,end,time,data):
                key = _cache.getkey(hsh, time, False, **kwargs)
                SQcache.set(key,[data[0][pos:end],data[1][pos:end],data[2]],604800)
            pos = 0
            length = len(data[1])
            t = [btime[0],0]  # from
            while length-pos>7500:
                t[1] = data[1][pos+5000].tolist()-1  # to
                if t[1]>=last:
                    return
                store(pos,pos+5000,t,data)
                t[0] = t[1]+1  # from
                pos += 5000
            t[1] = btime[1]  # to
            if t[1]<last:
                store(pos,length,t,data)
        # read and cache new data
        last = getLast(path)['upto']
        tmax = 32
        idx = [tmax-1]
        if _use_threads:
            threads = [_th.Thread(target=task, args=(blck, times, idxs, idx, i)) for i in range(min(tmax,len(blck)))]
            for thread in threads: thread.start()
            for thread in threads: thread.join()
        else:
            task(blck, times, idxs, idx, 0)
        for i in _ver.xrange(len(idxs)):
            if len(blck[idxs[i]][0])>0:
                _cachechunk(blck[idxs[i]],times[i],last)
        blck = [b for b in blck if len(b[0])>0]
    # concatenate chunks to data
    if len(blck)==1:
        dat = blck[0][0]
        dim = blck[0][1]
    else:
        try:
            dat = _np.concatenate(tuple(b[0] for b in blck))
            dim = _np.concatenate(tuple(b[1] for b in blck))
        except KeyboardInterrupt as ki: raise ki
        except:
            dat = dim = _np.array([])
            return [dat, dim, 'NoData']
    unit = 'unknown'
    for b in blck:
        if isinstance(b[2],str):
            unit = b[2]
            break
    del(blck)
    # trim data to time window
    start= 0
    stop = len(dim)
    while dim[start]<time[0]: start+=1
    while dim[stop-1]>time[1]: stop-=1
    dat = dat[start:stop]
    dim = dim[start:stop]
    return [dat, dim, unit]

def get_json(url, **kwargs):
    class reader(object):
        def __init__(self, value):
            self.value = value
        def read(self,*argin):
            return _ver.tostr(self.value.read(*argin))
    handler = _get_json(url, **kwargs)
    return _json.load(reader(handler), strict=False)

def _get_json(url, timeout=None, retry=0, **kwargs):
    url = _b.parms(url, **kwargs)
    _sup.debug(url,5)
    headers = {'Accept': 'application/json'}
    for i in range(max(retry,0)+1):
        try:
            handler = get(url, headers, timeout=timeout)
        except _ver.urllib.socket.timeout as handler:
            _sup.debug('timeout: %d (%s)'%(i,url))
            continue
        break
    if isinstance(handler,(_ver.urllib.socket.timeout,)):
        raise handler
    if handler.getcode() != 200:
        raise Exception('request failed: code '+str(handler.getcode()))
    if handler.headers.get('content-type') != 'application/json':
        raise Exception('requested content-type mismatch: ' +
                        handler.headers.get('content-type'))
    return handler

def get_program(time=None):
    def convertProgram(j):
        trigger = range(7)
        for i in trigger:
            t = j['trigger'].get(str(i))
            trigger[i] = t[0] if len(t)>0 else 0
        return {
        'id':list(map(int,str(j['id']).split('.'))),
        'name':str(j['name']),
        'time':_b.TimeInterval([j['from'],j['upto'],j['trigger']['1'][0]]),
        'description':str(j['description']),
        'trigger':_b.TimeArray(trigger)}
    time = _b.TimeInterval(time)
    jlist = get_json(_b._rooturl+'/programs.json?'+str(time))
    return [convertProgram(j) for j in jlist.get('programs',[])]

def getLast(path, time=[1,-1]):
    j = get_json(_b.filter(path, time))
    last = _ver.tostr(j['_links']['children'][0]['href']).split('?')[1].split('&')
    last = [s.split('=') for s in last]
    last = dict((s[0],int(s[1])) for s in last)
    return last

def post(url, headers={}, data=None, json=None, timeout=None, retry=0):
    if json is not None:
        data = _json.dumps(json)
        headers['content-type'] = 'application/json'
    if isinstance(data, (_ver.file)):
        _sup.debug(data.name)
        data = _mmap.mmap(data.fileno(),0,access=_mmap.ACCESS_READ)
    else:
        _sup.debug(data,5)
        data = _ver.tobytes(data)
    _sup.debug(url)
    for i in range(max(retry,0)+1):
        try:
            result = get(url, headers, data, timeout=timeout)
        except _ver.urllib.socket.timeout as result:
            _sup.debug('timeout: %d (%s)'%(i,url))
            continue
        break
    if isinstance(result,(_ver.urllib.socket.timeout,)):
        raise result
    #if json is not None:
    #   _json.dump(json,data)
    if isinstance(data,(_mmap.mmap)):
        data.close()
    return result


def get(url, headers={}, *data, **kv):
    if 'timeout' not in kv.keys():
        _sup.debug('NO TIMEOUT: %s' % (url,))
        kv['timeout'] = 99
        retry = 1
    else:
        retry = 0
    req = _ver.urllib.Request(url)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        while True:
            try:
                handler = _ver.urllib.urlopen(req, *data, **kv)
            except _ver.urllib.socket.timeout as err:
                if retry==0: raise err
                _sup.debug('timeout: %d (%s)'%(retry,url))
                retry += 1
                continue
            break
    except _ver.urllib.HTTPError as err:
        _sup.debug(err.getcode())
        _sup.debug(err.reason,2)
        _sup.debug(err.read(),3)
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
    path =  _b.Path(path)
    par = get_json(path.url_parlog(), time=time, **kwargs)
    if not isinstance(par, dict):
        raise Exception('parlog not found!')
    par = par['values'][-1]
    if par is None: return None
    if 'chanDescs' in par.keys():
        cD = par['chanDescs']
        if isinstance(cD, dict):
            cD = dict2list(cD)
        if isinstance(cD, list) and len(cD) == 1 and 'xmlDescription' in cD[0].keys():
            cD = integrateChLst(cD[0])
        par['chanDescs'] = cD
    return par


def read_cfglog(path, time=[-1, 0], **kwargs):
    path =  _b.Path(path)
    par = get_json(path.url_cfglog(), time=time, **kwargs)
    if not isinstance(par, dict):
        raise Exception('cfglog not found!')
    return par


def read_jpg_url(url, time, skip=0):
    time = _b.TimeInterval(time)
    link = url + '/_signal.jpg?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    return get(link)


def read_png_url(url, time, skip=0):
    time = _b.TimeInterval(time)
    link = url + '/_signal.png?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    return _png.Reader(get(link)).read()


def read_pngs_url(url,time,ntreads=3):
    def task(S, url, dim, idx):
        while True:
            try:
                i = idx[0];idx[0]+=1
                time = _b.TimeInterval([dim[i]]*2)
                S[i] = read_png_url(url,time,0)[2]
            except KeyboardInterrupt as ki: raise ki
            except: break
    time = _b.TimeInterval(time)
    par = get_json(url+'/_signal.json?'+time.filter())['_links']['children']
    dim = [int(_re.search('(?<=from=)([0-9]+)',str(p['href'])).group()) for p in par]
    dim.reverse()
    idx=[0]
    S=[None]*len(dim)
    threads = [_th.Thread(target=task, args=(S, url, dim, idx)) for i in range(ntreads)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    return _b.createSignal(_np.array([s for s in S if s is not None]),dim,time.t0T,'unknown')


def read_raw_url(url, time, skip=0):
    time = _b.TimeInterval(time)
    link = url + '/_signal.json?' + str(time) + '&skip=' + str(skip)
    _sup.debug(link)
    r = get(link)
    return r
