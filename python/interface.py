"""
archive.interface
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
import os
from .base import TimeInterval, Path, createSignal
from .cache import cache
from .version import xrange, urllib

filebase = 'archive_cache'
isunix = os.name == 'posix'
if isunix:
    tmpdir = "/tmp/"
else:
    tmpdir = filebase = os.getenv('TEMP')+'\\'
filebase = tmpdir+filebase
SQCache = cache(filebase)


def write_logurl(url, parms, time):
    log = {'values': [parms],
           'dimensions': TimeInterval(time).ns,
           'label': 'parms',
           }
    return(post(url, json=log))  # , data=json.dumps(cfg)


def write_data(path, data, dimof):
    jdict = {'values': data, 'dimensions': dimof}
    return(post(Path(path).url_datastream(), json=jdict))


def write_image(path, data, dimof):
    data = [[[data[t][x][y] for t in xrange(len(data))]
            for x in xrange(len(data[0]))] for y in xrange(len(data[0][0]))]
    name = path.name_datastream()
    tmpfile = tmpdir+"archive_"+name+".h5"
    try:
        from h5py import File as h5file
        with h5file(tmpfile, 'w') as f:
            f.create_dataset(name, data=data, compression="gzip")
            f.create_dataset('timestamps', data=dimof, dtype='int64',
                             compression="gzip")
        headers = {'Content-Type': 'application/x-hdf'}
        link = path.url_streamgroup()+'?dataPath='+name+'&timePath=timestamps'
#        with requests_cache.disabled():
        with open(tmpfile, 'rb') as f:
            return(post(link, headers=headers, data=f))
    finally:
        os.remove(tmpfile)


def read_signal(path, time, t0=0, *arg):
    path = Path(path)
    time = TimeInterval(time)
    sig = None
    if time.uptoVal >= 0:
        key = path.path()+'?'+str(time)
        sig = SQCache.get(key)
    if sig is None:
        print('get web-archive: '+key)
        stream = get_json(path.url_data(), time, *arg)
        sig = createSignal(stream["values"], stream['dimensions'], t0,
                           str(stream.get('unit', 'unknown')))
        SQCache.set(key, sig)
    return sig


def get_json(url, *arg):
    url = Path(url).url(-1, *arg)
    _debug(url)
    import json
    import codecs
    reader = codecs.getreader("utf-8")
    headers = {'Accept': 'application/json'}
    handler = get(url, headers)
    if handler.getcode() != 200:
        raise Exception('request failed: code '+str(handler.getcode()))
    if handler.headers.get('content-type') != 'application/json':
        raise Exception('requested content-type mismatch: ' +
                        handler.headers.get('content-type'))
    return json.load(reader(handler), strict=False)


def post(url, headers={}, data=None, json=None):
    if json is not None:
        import json as j
        data = j.dumps(json)
    headers['content-type'] = 'application/json'
    return(get(url, headers, data, 'POST'))


def get(url, headers={}, data=None, method='GET'):
    req = urllib.Request(url)
    for k, v in headers.items():
        req.add_header(k, v)
    req.get_method = lambda: method
    handler = urllib.urlopen(req, timeout=3)
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


def read_parlog(path, time=TimeInterval([0, 0]), *args):
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
        chans = len(chtmp)*[{}]
        for ch in chtmp:
            ch['active'] = active
            ch['physicalQuantity'] = pQ
            i = int(ch['channelNumber'])
            rmfield(ch, 'channelNumber')
            for k, v in ch.items():
                if v is not None:
                    chans[i][k] = v
        return chans
    url = Path(path).url_parlog(time, *args)
    par = get_json(url)
    if type(par) is not dict:
        raise Exception('parlog not found:\n'+url)
    par = par['values'][-1]
    if 'chanDescs' in par.keys():
        cD = par['chanDescs']
        if type(cD) is dict:
            cD = dict2list(cD)
        if cD and len(cD) == 1 and 'xmlDescription' in cD[0].keys():
            cD = integrateChLst(cD[0])
        par['chanDescs'] = cD
    return par


def read_cfglog(path, time=[0, 0], *args):
    url = Path(path).url_cfglog(time, *args)
    par = get_json(url)
    if type(par) is not dict:
        raise Exception('cfglog not found:\n'+url)
    return par


def read_jpg_url(url, time, skip=0):
    time = TimeInterval(time)
    link = url + '/_signal.jpg?' + str(time) + '&skip=' + str(skip)
    _debug(link)
    r = get(link)
    return r


def read_png_url(url, time, skip=0):
    time = TimeInterval(time)
    link = url + '/_signal.png?' + str(time) + '&skip=' + str(skip)
    _debug(link)
    r = get(link)
    return r


def read_raw_url(url, time, skip=0):
    time = TimeInterval(time)
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
