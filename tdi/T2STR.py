"""
helper fuction that returns a human readable timestamp UTC if a W7X time
T2STR( time )
"""
from archive import base
def T2STR(time, asarray=None):
    try: time = time.data()
    except: pass
    try:
        return base.Time(time).utc
    except:
        if asarray:
            return base.TimeArray(time).utc
        else:
            return repr(base.TimeArray(time))
