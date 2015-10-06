"""
archive.accessArchiveDB
==========
@author: Cloud
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""
from .base import TimeInterval
from .support import error
from .interface import read_signal, read_cfglog, read_parlog


def mds_signal(url, time, help, channel):
    print('mds_signal')
    try:
        time = TimeInterval(time)
        t0 = time.t0T
        signal = read_signal(url, time, t0, 0, 0, channel.tolist())
        signal.setHelp(str(help))
        return signal
    except:
        import getpass
        user = getpass.getuser()
        return user+": "+error()


def mds_channel(streamURL, time, channelNr, help=None):
    return mds_signal(streamURL, time, help, [channelNr])


def mds_stream(streamURL, time, help=None):
    return mds_signal(streamURL, time, help)


def mds_parlog(streamURL, time):
    try:
        return(str(read_parlog(streamURL, time)))
        # return(Path(streamURL).url_parlog(time))
    except:
        return(error())


def mds_cfglog(strgrpURL, time):
    try:
        return(str(read_cfglog(strgrpURL, time)))
        # return(Path(strgrpURL).url_cfglog( time ))
    except:
        return(error())
