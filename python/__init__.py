"""
CODAC
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
@copyright: 2015
@license: GNU GPL
"""
def PY3():
    import sys
    return(sys.version_info>=(3,))
PY3 = PY3()
if PY3:
    from .base import Time,TimeInterval,Unit,Path
    from .interface import read_signal,read_cfglog,read_parlog
    from .classes import datastream,browser
    from .mdsupload import uploadNode
    from .support import setTIME
    from .archiveaccess import *
    from .archiveadd import addW7X
else:
    from base import Time,TimeInterval,Unit,Path
    from interface import read_signal,read_cfglog,read_parlog
    from classes import datastream,browser
    from mdsupload import uploadNode
    from support import setTIME
    from archiveaccess import *
    from archiveadd import addW7X
