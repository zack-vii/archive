def codac_signal(node):
    import codac
    try:
        time = codac.TimeInterval(node.getNode('\TIME').data())
#        try:
#            t0 = codac.Time(node.getNode('\TIME.T1:IDEAL')).ns()
#        except:
        t0 = time.getFrom()
#        try:
#            idx = node.getNode('$IDX').data()
#            url  = node.getParent().getNode('$URL').data()
#            signal = codac.read_signal(url,time,t0,0,0,[idx])
#        except:
        url  = node.getNode('$URL').data()
        signal = codac.read_signal(url,time,t0,0,0,[])
        try:
            help = node.getNode('.DESCRIPTION').data()
        except:
            help = None
        if help is None: 
            help = node.getNode('$NAME').data()
        signal.setHelp(str(help))
        return(signal)
    except:
        import getpass
        user = getpass.getuser()
        return user+": "+codac.support.error()