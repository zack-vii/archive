from MDSplus import Int32Array, Signal
def RGB(node,seg=None):
    if seg is None:
        signal = node.record
    else:
        signal = node.getSegment(seg)
    args   = list(signal.args)
    data   = Int32Array([d[0].astype('int32')<<16 
                       | d[1].astype('int32')<<8
                       | d[2].astype('int32') for d in signal.data()])
    return Signal(data,None,*args[2:])

