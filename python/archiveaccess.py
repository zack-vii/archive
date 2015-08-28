# -*- coding: utf-8 -*-
"""
codac.accessArchiveDB
==========
@author: Cloud
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""

from .base import TimeInterval,createSignal
from .support import error
from .interface import read_signal,read_cfglog,read_parlog
        
def mds_channel(streamURL, time, channelNr, e=None):
    try:
        time = TimeInterval(time)
        stream = read_signal( streamURL + '_DATASTREAM' , time, time[0], 0, 0, [channelNr])
#        dim  = stream['dimensions']
#        raw  = stream["values"][0]
#        unit = stream.get('unit','unknown')
#        N   = len(raw)-1 # removing None data
#        for i in xrange(len(raw)):
#            if raw[N-i] is None:
#                del(dim[N-i])
#                del(raw[N-i])
#        return(createSignal(raw, dim, time.getFrom(), unit))
        return(stream)
    except:
        err = error(e)
        print(err)
        return(err)
     
def mds_stream(streamURL, time, help=None, e=None):
    try:
        time = TimeInterval(time)
        stream = read_signal( streamURL + '_DATASTREAM', time, time[0] )
        return createSignal(stream['values'], stream['dimensions'], help=help )
    except:
        return error(e)

def mds_parlog(streamURL, time):
    try:
        return read_parlog( streamURL, time )
    except:
        return error()

def mds_cfglog(strgrpURL, time):
    try:
        return read_cfglog( strgrpURL, time )
    except:
        return error()