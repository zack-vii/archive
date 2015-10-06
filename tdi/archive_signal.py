import archive


def archive_signal(node, time=None):
    """ use time if tree is archive """
    """ else use TIME node """
    print('archive_signal')
    try:
        if node.tree.upper() == 'ARCHIVE':
            try:
                time = time.data()
            except:
                time = archive.TimeInterval(time)
        else:
            time = archive.TimeInterval(node.getNode('\TIME').data())
        url = node.getNode('$URL').data()
        signal = archive.read_signal(url, time, time.t0T, 0, 0, [])
        try:
            help = node.getNode('HELP').data()
        except:
            try:
                help = node.getNode('DESCRIPTION').data()
            except:
                help = node.getNode('$NAME').data()
        signal.setHelp(str(help))
        return(signal)
    except:
        import getpass
        user = getpass.getuser()
        help = user+": "+archive.support.error()
        try:
            from MDSplus import Signal
            signal = Signal([6,66,666])
            signal.setHelp(help.split('\n')[-1])
            return(signal)
        except:
            return(help)
