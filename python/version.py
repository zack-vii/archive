"""
This is a helper module.
Its purpose is to supply tools that are used to generate version specific code.
Goal is to generate code that work on both python2x and python3x.
"""
from numpy import generic as npscalar
from numpy import ndarray as nparray
from sys import version_info as pyver
from sys import platform as platform
from os import name as osname


isposix = osname == 'posix'
isnt = osname == 'nt'
islinux = platform.startswith('linux')
iswin = platform.startswith('win')

if isposix:
    tmpdir = "/tmp/"
else:
    from os import getenv as tmpdir
    tmpdir = tmpdir('TEMP')+'\\'

ispy3 = pyver > (3,)
ispy2 = pyver < (3,)

# __builtins__ is dict
try:
    has_long = 'long' in __builtins__#.__dict__.keys()
    has_unicode = 'unicode' in __builtins__#.__dict__.keys()
    has_basestring = 'basestring' in __builtins__#.__dict__.keys()
    has_bytes = 'bytes' in __builtins__#.__dict__.keys()
    has_buffer = 'buffer' in __builtins__#.__dict__.keys()
    has_xrange = 'xrange' in __builtins__#.__dict__.keys()
except:
    has_long = 'long' in __builtins__.__dict__.keys()
    has_unicode = 'unicode' in __builtins__.__dict__.keys()
    has_basestring = 'basestring' in __builtins__.__dict__.keys()
    has_bytes = 'bytes' in __builtins__.__dict__.keys()
    has_buffer = 'buffer' in __builtins__.__dict__.keys()
    has_xrange = 'xrange' in __builtins__.__dict__.keys()

# substitute missing builtins
if has_long:
    long = long  # analysis:ignore
else:
    long = int
if has_basestring:
    basestring = basestring  # analysis:ignore
elif has_bytes:
    basestring = (str, bytes)
else:
    basestring = str
if has_unicode:
    unicode = unicode  # analysis:ignore
else:
    unicode = str
if has_bytes:
    bytes = bytes  # analysis:ignore
else:
    bytes = str
if has_buffer:
    buffer = buffer  # analysis:ignore
else:
    buffer = memoryview
if has_xrange:
    xrange = xrange  # analysis:ignore
else:
    xrange = range

if ispy3:
    import urllib.request as urllib
else:
    import urllib2 as urllib  # analysis:ignore

try:
    import cPickle as pickle
except:
    import pickle  # analysis:ignore

# helper variant string
if has_unicode:
    varstr = unicode
else:
    varstr = bytes


def _decode(string):
    try:
        return string.decode('utf-8', 'backslashreplace')
    except:
        return string.decode('CP1252', 'backslashreplace')


def _encode(string):
    try:
        return string.encode('utf-8')
    except:
        return string.encode('CP1252')

# numpy char types
npunicode = 'U'
npbytes = 'S'


if ispy2:
    npstr = npbytes
else:
    npstr = npunicode


def _tostring(string, targ, nptarg, conv):
    if isinstance(string, targ):  # short cut
        return targ(string)
    if isinstance(string, basestring):
        return targ(conv(string))
    if isinstance(string, (list, tuple)):
        return type(string)(_tostring(s, targ, nptarg, conv) for s in string)
    if isinstance(string, npscalar):
        return targ(string.astype(nptarg))
    if isinstance(string, nparray):
        string = string.astype(nptarg).tolist()
    return _tostring(str(string), targ, nptarg, conv)


def tostr(string):
    if ispy2:
        return tobytes(string)
    else:
        return tounicode(string)


def tobytes(string):
    return tounicode(string).encode('utf-8', 'backslashreplace')



def tounicode(string):
    return _tostring(string, unicode, npunicode, _decode)