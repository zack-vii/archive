import archive
def archive_signal(node,time=None):
    print('archive_signal')
    try:
        try:
            time = time.data();            
        except:
            time = None
        if time is None:
            time = archive.TimeInterval(node.getNode('\TIME').data())
        else:
            time = archive.TimeInterval(time)
#        try:
#            t0 = codac.Time(node.getNode('\TIME.T1:IDEAL')).ns
#        except:
        t0 = time.fromVal
        url= node.getNode(':$URL').data()
        signal = archive.read_signal(url,time,t0,0,0,[])
        try:
            help = node.getNode(':DESCRIPTION').data()
        except:
            help = None
        if help is None: 
            help = node.getNode(':$NAME').data()
        signal.setHelp(str(help))
        return(signal)
    except:
        import getpass
        user = getpass.getuser()
        return user+": "+archive.support.error()