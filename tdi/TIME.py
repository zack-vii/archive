"""
helper fuction that set the _time variable or unsets it with TIME(*)
TIME( SHOT ); SHOT: MDSplus shot number; _time = [T0,T6,T1] given in SHOT
TIME( FROM, UPTO ); SHOT: MDSplus shot number; _time = [FROM,UPTO,FROM] converted to ns
TIME( FROM, UPTO, ORIGIN ); SHOT: MDSplus shot number; _time = [FROM,UPTO,ORIGIN] converted to ns
"""
from MDSplus import makeArray, TdiExecute, EmptyData
from archive import base
def TIME(*arg):
    def TimeToNs(v):
        try: v = v.data()
        except: pass
        if v<=1E16: v = int(v*1000)*1000000
        return v
    if len(arg)==0:
        pass
    elif len(arg)==1:
        if arg[0] is None:
            EmptyData().setTdiVar('_time')
        else:
            TdiExecute('TreeOpen($,$)',('W7X',int(arg[0])))
            TdiExecute("DATA(TIMING)").setTdiVar('_time')
    elif len(arg)<=3:
        arg = map(TimeToNs,arg)
        if len(arg)==2:
            t = [arg[0],arg[1],arg[0]]
        elif len(arg)==3:
            t = [arg[0],arg[1],arg[2]]
        makeArray(t).setTdiVar('_time')
    else:
        raise Exception('Only upto 3 arguments allowed.')
    try:
        time = TdiExecute('PUBLIC("_time")')
    except:
        print("cleared")
        return
    print('from %s upto %s, t=0 @ %s' % tuple(base.TimeArray(time.data()).utc))
    return time
