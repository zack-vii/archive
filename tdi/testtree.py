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

    def time(N):
        return Range(0., 1., 1./(N-1)).setUnits("time")
    def axis(N,idx):
        return Range(0., 1., 1./(N-1)).setUnits("dim_of("+str(idx)+")")

    data  = TdiCompile(tdifun+'($VALUE)').setUnits('V')

    dims = [[]]*(ndims+1)
    dims[0] = time(shape[0])
    raw = pysfun(0).data()
    tfac = 1;
    for i in range(ndims,0,-1):
        dims[i] = X = axis(shape[i],i)
        raw = array([raw*factor+dfun(x) for x in X.data()])
        tfac*= factor
    raw = array([raw+tfun(x)*tfac for x in dims[0].data()])
    raw = pyafun(raw).setUnits('data')
    return Signal(data,raw,*dims).setHelp('this is the help text')


def createTestTree(shot=-1,path=None):
    import MDSplus,os
    def populate(node):
        def py(n):
            n.putData(MDSplus.TdiCompile('testtree('+n.getPath()+')'))
        ntypes=["ARR","SEG","NUM"]
        dtypes=["8","16","32","64","F","D"]
        ndims =range(3)
        py(node.addNode('IMAGE','SIGNAL'))
        py(node.addNode('IMAGES','SIGNAL'))
        py(node.addNode('TEXT','TEXT'))
        for nt in ntypes:
            for dt in dtypes:
                if nt=="NUM":
                    py(node.addNode(nt+dt,'NUMERIC'))
                else:
                    for nd in ndims:
                        py(node.addNode(nt+str(nd)+"D"+dt,'SIGNAL'))
    def evaluate(node):
        from sys import version_info as pyver
        if pyver>(3,):
            _xrange = range
        else:
            _xrange = xrange
        try:
            segszs=(1000, 100)
            for n in node.getMembers():
                name = n.getNodeName()
                if name.startswith("SEG"):
                    sig = getSignal(name,True)
                    data= sig.data()
                    segsz = segszs[data.ndim] if data.ndim<2 else 1
                    for i in _xrange(int(data.shape[0]/segsz)):
                        ft  = (i*segsz,(i+1)*segsz)
                        dim = MDSplus.Dimension(sig.dim_of()[ft[0]:ft[1]]).setUnits(sig.dim_of().units)
                        img = data[ft[0]:ft[1]]
                        n.makeSegment(sig.dim_of()[ft[0]],sig.dim_of()[ft[1]-1],dim,img)
                    n.setHelp(sig.getHelp())
                else:
                    n.putData(getSignal(name,True))
            name = None
        finally:
            if name is not None:
                print(name)


    if not len(os.environ["test_path"]):
        if path is None:
            isunix = os.name=='posix';
            if isunix:
                path = "/tmp"
            else:
                path = os.getenv('TEMP')
        os.environ["test_path"] = path
    with MDSplus.Tree('test',shot,'new') as tree:
        datanode = tree.addNode('DATA','STRUCTURE')
        pynode   = tree.addNode('PYTHON','STRUCTURE')
        populate(pynode)
        populate(datanode)
        tree.write()
        evaluate(datanode)