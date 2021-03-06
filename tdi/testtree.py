def testtree(node):
    from MDSplus import TreeNode
    try:
        print("python call: testtree("+repr(node)+")")
        if isinstance(node, TreeNode):
            node = node.getNodeName()
        return getSignal(node)
    except Exception as exc:#return debug information
        trace = 'python error:'+repr(exc)
        return(trace)
"""
SEGMENT
ntype = "ARR", "SEG"
ndims = N 1,2,3
dtype = 8,16,32,64,F,D
name  = ntype+ndims+"D"+dtype
"""
def getSignal(name,data=False):
    from MDSplus.mdsscalar import Int8,Int16,Int32,Int64,Float32,Float64,String
    if name=='TEXT':
        return String('Wooppp!!')
    from MDSplus._tdishr  import TdiCompile
    from MDSplus.mdsarray import Int8Array,Int16Array,Int32Array,Int64Array,Float32Array,Float64Array
    from MDSplus import Dimension
    if name=='IMAGE':
        if data:
            return TdiCompile('DATA:ARR1D32')
        else:
            name='ARR1D32'
    if name=='IMAGES':
        if data:
            return TdiCompile('DATA:ARR2D32')
        else:
            name='ARR2D32'

    from re import findall as parse
    # time, dim1, dim2, ...
    shapes=((10000,), (800,600), (64,64,64), (32,32,32,32), (16,16,16,16,16))
    dtypes= {"8"  : (Int8,       Int8Array,      'BYTE'     , 0x100,   False),
             "16" : (Int16,      Int16Array,     'WORD'     , 0x100,   False),
             "32" : (Int32,      Int32Array,     'LONG'     , 0x100,   False),
             "64" : (Int64,      Int64Array,     'QUADWORD' , 0x10000, False),
             "F"  : (Float32,    Float32Array,   'FLOAT'    , 2.,      True),
             "D"  : (Float64,    Float64Array,   'D_FLOAT'  , 2.,      True)}

    m = parse("(ARR|SEG|NUM)(?:([0-9]*)D|)([0-9FD]+)(?:_([0-9]+))?",name.upper())[0]
    ntype = m[0]
    ndims = int(m[1]) if not m[1]=='' else 1
    dtype = m[2]
    shape = shapes[ndims] if ndims<5 else (4,)*ndims
    pysfun = dtypes[dtype][0]
    pyafun = dtypes[dtype][1]
    tdifun = dtypes[dtype][2]
    factor = dtypes[dtype][3]
    isfloat= dtypes[dtype][4]
    from MDSplus.compound import Range,Signal
    from numpy import pi,cos,array

    if ntype=='NUM':
        return pysfun(pi)

    def tfun(x):
        x = cos(2*pi*x)
        if isfloat:
            return x
        return round(((x+1)/2.)*0x7F)
    def dfun(x):
        if isfloat:
            return x
        return round(x*0x7F)
    def mrange(N):
        return Range(0., 1., 1./(N-1))
    def time(r):
        return Dimension(None,r).setUnits("time")
    def axis(r,idx):
        return Dimension(None,r).setUnits("dim_of("+str(idx)+")")

    data  = TdiCompile(tdifun+'($VALUE)').setUnits('V')

    dims = [[]]*(ndims+1)
    dims[0] = mrange(shape[0])
    raw = pysfun(0).data()
    tfac = 1;
    for i in range(ndims,0,-1):
        dims[i] = X = mrange(shape[i])
        raw = array([raw*factor+dfun(x) for x in X.data().tolist()])
        tfac*= factor
        dims[i] = axis(dims[i],i)
    raw = array([raw+tfun(x)*tfac for x in dims[0].data().tolist()])
    dims[0] = time(dims[0])
    raw = pyafun(raw.tolist()).setUnits('data')
    return Signal(data,raw,*dims).setHelp('this is the help text')


def createTestTree(shot=-1,path=None):
    import MDSplus as m
    def checkpath(path):
        import os
        if not len(os.environ["test_path"]):
            if path is None:
                isunix = os.name=='posix';
                if isunix:
                    path = "/tmp"
                else:
                    path = os.getenv('TEMP')
            os.environ["test_path"] = path
        return path

    def populate(node):
        ntypes=["ARR","SEG","NUM"]
        dtypes=["8","16","32","64","F","D"]
        ndims =range(3)
        node.addNode('IMAGE','SIGNAL')
        node.addNode('IMAGES','SIGNAL')
        node.addNode('TEXT','TEXT')
        for nt in ntypes:
            for dt in dtypes:
                if nt=="NUM":
                    node.addNode(nt+dt,'NUMERIC')
                else:
                    for nd in ndims:
                        node.addNode(nt+str(nd)+"D"+dt,'SIGNAL')


    def evaluate_python(node):
        for n in node.getMembers():
            n.putData(m.TdiCompile('testtree($)',(n,)))


    def evaluate_data(node):
        try:
            segszs=(1000, 100)
            for n in node.getMembers():
                name = n.getNodeName()
                sig = getSignal(name,True)
                if name.startswith("SEG"):
                    m.tcl('SET NODE %s /COMPRESS_SEGMENTS' % n.getPath())
                    data= sig.data()
                    dims= sig.dim_of().data()
                    duns= sig.dim_of().units
                    segsz = segszs[data.ndim] if data.ndim<2 else 1
                    for i in range(int(data.shape[0]/segsz)):
                        ft  = (i*segsz,(i+1)*segsz)
                        img = data[ft[0]:ft[1]]
                        dim = m.Dimension(None,dims[ft[0]:ft[1]])
                        dim.setUnits(duns)
                        n.makeSegment(dims[ft[0]],dims[ft[1]-1],dim,img)
                    # n.setUnits(sig.units)
                    # n.setHelp(sig.getHelp())
                else:
                    n.putData(sig)
            name = None
        finally:
            if name is not None:
                print(name)

    path = checkpath(path)
    with m.Tree('test',shot,'new') as tree:
        populate(tree.addNode('DATA','STRUCTURE'))
        populate(tree.addNode('PYTHON','STRUCTURE'))
        tree.write()
        evaluate_data(tree.getNode('DATA'))
        evaluate_python(tree.getNode('PYTHON'))
    with m.Tree('test',shot) as tree:
        tree.compressDatafile()

if __name__ == '__main__': createTestTree()
