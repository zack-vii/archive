def ECE(node):
    from MDSplus import TdiCompile, Float32
    from archive import calibrations
    print('get calibrated ECE signal')
    try:
        sig = node.evaluate()
        ecechannel = int(node.getNode('$IDX').data())+1
        offset,factor,unit,info = calibrations.ECEcalib(sig,ecechannel)
        if offset is None: return sig
        args = list(sig.args)
        args[1] = args[0]
        args[0] = TdiCompile('Build_With_Units(($VALUE-$)*$,$)',(Float32(offset),Float32(factor),unit))
        sig.args = tuple(args)
        return sig
    except:
        import getpass
        from archive import support
        user = getpass.getuser()
        print(user+": "+support.error())
