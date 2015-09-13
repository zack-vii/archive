"""
This is a helper module.
Its purpose is to supply tools that are used to generate version specific code.
Goal is to generate code that work on both python2x and python3x.
"""
from sys import version_info as pyver
from sys import platform as platform
from os import name as osname


isposix = osname == 'posix'
isnt = osname == 'nt'
islinux = platform.startswith('linux')
iswin = platform.startswith('win')

ispy3 = pyver > (3,)
ispy2 = pyver < (3,)

# __builtins__ is dict
has_long = 'long' in __builtins__
has_unicode = 'unicode' in __builtins__
has_basestring = 'basestring' in __builtins__
has_bytes = 'bytes' in __builtins__
has_buffer = 'buffer' in __builtins__
has_xrange = 'xrange' in __builtins__

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
    unicode = type(None)
if has_bytes:
    bytes = bytes
else:
    bytes = type(None)
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


def tostr(string):
    if isinstance(string, basestring):
        if isinstance(string, str):
            return string
        elif isinstance(string, unicode):
            return string.encode('utf-8', 'backslashreplace')
        else:
            try:
                return string.decode('utf-8', 'backslashreplace')
            except:
                return string.decode('CP1252', 'backslashreplace')
    else:
        return str(string)


def tobytes(string):
    if isinstance(string, basestring):
        if isinstance(string, bytes):
            return string
        else:
            try:
                return string.decode('utf-8', 'backslashreplace')
            except:
                return string.decode('CP1252', 'backslashreplace')
    else:
        return bytes(string)


def tounicode(string):
    if isinstance(string, basestring):
        if isinstance(string, unicode):
            return string
        else:
            return string.decode('utf-8', 'backslashreplace')
    else:
        return unicode(string)
