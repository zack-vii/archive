"""
helper fuction that set the _time variable or unsets it with TIME(0)
"""
from MDSplus import makeArray, TdiExecute, EmptyData, Tree
from archive import base
def TIME(*arg):
    def TimeToNs(v):
        if v<=1E16: v = int(v) * 1000000000
        return v
    if len(arg)==0:
        try:
            TdiExecute('PUBLIC("_time")')
            return TdiExecute('_time')
        except: return 'cleared'
    elif len(arg)==1:
        if arg[0] is None:
            EmptyData().setTdiVar('_time')
            TdiExecute('PUBLIC("_time")')
            return
        t = Tree('W7X',int(arg[0]))
        t = t.TIMING.record
        print(t)
    elif len(arg)<=3:
        arg = map(TimeToNs,arg)
        if len(arg)==2:
            t = [arg[0],arg[1],arg[0]]
        elif len(arg)==3:
            t = [arg[0],arg[1],arg[2]]
    else:
        raise Exception('Only upto 3 arguments allowed.')
    makeArray(t).setTdiVar('_time')
    TdiExecute('PUBLIC("_time")')
    return str(base.TimeArray(t))
