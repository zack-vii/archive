"""
helper fuction that returns a human readable timestamp UTC if a W7X time
T2STR( time )
"""
from archive import base
def T2STR(time):
    time = base.TimeArray(time)
    if len(time)==1: return time[0].utc
    else:            return time.utc
