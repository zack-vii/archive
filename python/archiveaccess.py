"""
archive.accessArchiveDB
==========
@author: Cloud
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from . import base as _base
from . import interface as _if
from . import support as _sup
from . import version as _ver


def mds_signal(url, time, help=None, channel=None, cache=None):
    print('mds_signal')
    try:
        time = _base.TimeInterval(time)
        t0 = time.t0T
        kwargs = {}
        if not channel is None: kwargs['channel']= int(channel)
        try:
            cache = cache.data()
        except:
            pass
        if not cache is None:   kwargs['cache']= bool(cache)
        signal = _if.read_signal(url, time, t0, **kwargs)
        signal.setHelp(_ver.tostr(help))
        return signal
    except:
        import getpass
        user = getpass.getuser()
        return user+": "+_sup.error()


def mds_channel(streamURL, time, channelNr, help=None, cache=None):
    return mds_signal(streamURL, time, help, channelNr, cache=cache)


def mds_stream(streamURL, time, help=None, cache=None):
    return mds_signal(streamURL, time, help, cache=cache)


def mds_parlog(streamURL, time):
    try:
        return(str(_if.read_parlog(streamURL, time)))
        # return(Path(streamURL).url_parlog(time))
    except:
        return(_sup.error())


def mds_cfglog(strgrpURL, time):
    try:
        return(str(_if.read_cfglog(strgrpURL, time)))
        # return(Path(strgrpURL).url_cfglog( time ))
    except:
        return(_sup.error())
