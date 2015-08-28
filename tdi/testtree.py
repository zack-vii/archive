def testtree(node,data=False):
    try:
        print("python call: testtree("+str(node)+")")
        name=node.getNodeName()
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
        shapes=((10000,), (600,800), (64,64,64), (32,32,32,32), (16,16,16,16,16))
        dtypes= {"8"  : (Int8,       Int8Array,      'BYTE'     , 0x100),
                 "16" : (Int16,      Int16Array,     'WORD'     , 0x100),
                 "32" : (Int32,      Int32Array,     'LONG'     , 0x100),
                 "64" : (Int64,      Int64Array,     'QUADWORD' , 0x10000),
                 "F"  : (Float32,    Float32Array,   'FLOAT'    , 1.1),
                 "D"  : (Float64,    Float64Array,   'D_FLOAT'  , 1.01)}
      
        m = parse("(ARR|SEG|NUM)(?:([0-9]*)D|)([0-9FD]+)(?:_([0-9]+))?",name.upper())[0]
        ntype = m[0]
        ndims = int(m[1]) if not m[1]=='' else 1
        dtype = m[2]
        shape = shapes[ndims] if ndims<5 else (4,)*ndims
        pysfun = dtypes[dtype][0]
        pyafun = dtypes[dtype][1]
        tdifun = dtypes[dtype][2]
        factor = dtypes[dtype][3]
        from MDSplus.compound import Range,Signal
        from numpy import pi,cos,array
        
        if ntype=='NUM':
            return pysfun(pi)
    
        def fun(x):
            return((cos(2*pi*x)+1.)/2.)
        def time(N):
            return Range(0., 1., 1./(N-1)).setUnits("time")
        def axis(N,idx):
            return Range(0., 1., 1./(N-1)).setUnits("dim_of("+str(idx)+")")
    
        data  = TdiCompile(tdifun+'($VALUE)').setUnits('V')
    
        dims = [[]]*(ndims+1)
        dims[0] = time(shape[0])
        raw = pysfun(0).data()
        tfac = 1;
        for i in xrange(ndims):
            dims[ndims-i] = X = axis(shape[ndims-i],ndims-i)
            raw = array([raw*factor+round(x*0x7F) for x in X.data()])
            tfac*= factor
        raw = array([raw+round(fun(x)*0x7F)*tfac for x in dims[0].data()])
        raw = pyafun(raw).setUnits('data')
        return Signal(data,raw,*dims).setHelp('this is the help text')
    except:#return debug information
        import traceback
        trace = 'python error:\n'+traceback.format_exc()
        for line in trace.split('\n'):
            print(line)
        return(trace)