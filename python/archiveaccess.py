"""
archive.accessArchiveDB
==========
@author: Cloud
data rooturl database view    project strgrp stream idx    channel
lev  0       1        2       3       4      5      6      7
"""

from archive.base import TimeInterval,createSignal#,Path
from archive.support import error
from archive.interface import read_signal,read_cfglog,read_parlog

def mds_signal(url,time,help):
    print('mds_signal')
    try:
        time = TimeInterval(time);
        t0 = time.getFrom()
        signal = read_signal(url,time,t0,0,0,[])
        signal.setHelp(str(help))
        return(signal)
    except:
        import getpass
        user = getpass.getuser()
        return user+": "+error()

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

def mds_parlog(streamURL, time, e=None):
    try:
        return(str(read_parlog( streamURL, time )))
        #return(Path(streamURL).url_parlog( time ))
    except:
        return(error(e))

def mds_cfglog(strgrpURL, time, e=None):
    try:
        return(str(read_cfglog( strgrpURL, time )))
        # return(Path(strgrpURL).url_cfglog( time ))
    except:
        return(error(e))