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
import MDSplus as _mds
import numpy as _np

try:
    import h5py as _h5
except:
    print('WARNING: "h5py" package not found.\nImage upload will not be available')
from . import base as _base
from . import cache as _cache
from . import support as _sup
from . import version as _ver

SQCache =  _cache.cache(_ver.tmpdir+'archive_cache'+str(_ver.pyver[0]))

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


def read_signal(path, time, t0=0, *arg):
    path = _base.Path(path)
    time = _base.TimeInterval(time)
    sig = None
    if time.uptoT != -1:
        key = path.path()+'?'+','.join(map(str,[time.fromT,time.uptoT]+list(arg)))
        sig = SQCache.get(key)
    if sig is None:
        print('get web-archive: '+key)
        stream = get_json(path.url_data(), time, *arg)
        sig = _base.createSignal(stream['values'], stream['dimensions'], str(stream.get('unit', 'unknown')))
        SQCache.set(key, sig)
    if t0!=0:
        args = list(sig.args)
        time = _mds.Float64((args[2].args[1]-_mds.Int64(t0))*1E-9)
        wind = _mds.Window(time[0], time[time.shape[0]-1], t0)
        args[2] = _mds.Dimension(wind, time)
        args[2].setUnits('s')
        sig.args = tuple(args)
    return sig


def get_json(url, *arg):
    class reader(object):
        def __init__(self, value):
            self.value = value
        def read(self,*argin):
            return _ver.tostr(self.value.read(*argin))
    url = _base.Path(url).url(-1, *arg)
    _debug(url)
    headers = {'Accept': 'application/json'}
    handler = get(url, headers)
    if handler.getcode() != 200:
        raise Exception('request failed: code '+str(handler.getcode()))
    if handler.headers.get('content-type') != 'application/json':
        raise Exception('requested content-type mismatch: ' +
                        handler.headers.get('content-type'))

    return _json.load(reader(handler), strict=False)


def post(url, headers={}, data=None, json=None):
#    if url[-1] != '/':
 #       url += '/'
    if json is not None:
        data = _json.dumps(json)
        headers['content-type'] = 'application/json'
    if isinstance(data, (_ver.file)):
        _debug(data.name)
        data = _mmap.mmap(data.fileno(), 0, access=_mmap.ACCESS_READ)
    else:
        _debug(data)
        data = _ver.tobytes(data)
    _debug(url)
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
            if type(res[name]) is list:
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


def read_parlog(path, time=[1, 0], *args):
    def rmfield(dic, field):
        if field in dic.keys():
            del(dic[field])

    def dict2list(d):
        l = len(d.keys())*[None]
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
    url = _base.Path(path).url_parlog(time, *args)
    par = get_json(url)
    if type(par) is not dict:
        raise Exception('parlog not found:\n'+url)
    par = par['values'][-1]
    if 'chanDescs' in par.keys():
        cD = par['chanDescs']
        if isinstance(cD, dict):
            cD = dict2list(cD)
        if isinstance(cD, list) and len(cD) == 1 and 'xmlDescription' in cD[0].keys():
            cD = integrateChLst(cD[0])
        par['chanDescs'] = cD
    return par


def read_cfglog(path, time=[0, 0], *args):
    url = _base.Path(path).url_cfglog(time, *args)
    par = get_json(url)
    if type(par) is not dict:
        raise Exception('cfglog not found:\n'+url)
    return par


def read_jpg_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.jpg?' + str(time) + '&skip=' + str(skip)
    _debug(link)
    r = get(link)
    return r


def read_png_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.png?' + str(time) + '&skip=' + str(skip)
    _debug(link)
    r = get(link)
    return r


def read_raw_url(url, time, skip=0):
    time = _base.TimeInterval(time)
    link = url + '/_signal.png?' + str(time) + '&skip=' + str(skip)
    _debug(link)
    r = get(link)
    return r


def _debug(msg):
    import inspect
    try:
        print(inspect.stack()[1][3] + ': ' + str(msg))
    except:
        print(msg)
