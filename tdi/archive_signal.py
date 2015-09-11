import archive,MDSplus
def archive_signal(node,time=MDSplus.EmptyData()):
    print('archive_signal')
    try:
        time = time.data;
        if time is None:
            time = archive.TimeInterval(time)
        except:
            time = archive.TimeInterval(node.getNode('\TIME').data())
#        try:
#            t0 = codac.Time(node.getNode('\TIME.T1:IDEAL')).ns()
#        except:
        t0 = time.getFrom()
        url  = node.getNode(':$URL').data()
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