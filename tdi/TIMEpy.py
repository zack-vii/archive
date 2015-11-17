"""
helper fuction that set the _time variable or unsets it with TIME(0)
"""
from time import time
from MDSplus import Float64Array, TdiExecute, EmptyData
def TIMEpy(*arg):
    if len(arg)==0:
        t0 = time()
        t = [t0-3600.,t0,t0]
    elif arg[0] == 0:
        EmptyData().setTdiVar('_time')
        TdiExecute('PUBLIC("_time")')
        return
    elif len(arg)==1:
        t0 = time()
        t = [t0-arg[0],t0,t0]
    elif len(arg)==2:
        t = [arg[0],arg[1],arg[0]]
    elif len(arg)==3:
        t = [arg[0],arg[1],arg[2]]
    else:
        raise Exception('Only upto 3 arguments allowed.')
    Float64Array(t).setTdiVar('_time')
    TdiExecute('PUBLIC("_time")')
    return t