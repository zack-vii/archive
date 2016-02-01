def ARCHIVE(url,time):
    from archive import base, interface
    try:
        """ use _time variable if Tree is ARCHIVE """
        try:    time = base.TimeInterval(time)
        except: time = base.TimeInterval([-1800.,0,0])
        path = base.Path(url)
        return interface.read_signal(path, time)
    except:
        """ generate dummy signal with error message as help text """
        import getpass,sys
        user = getpass.getuser()
        e = sys.exc_info()[1]
        help = user+": "+str(e)
        try:
            from MDSplus import Signal
            signal = Signal([6,66,666])
            signal.setHelp(help.split('\n')[-1])
            return signal
        except:
            return help
