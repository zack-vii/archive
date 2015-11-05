import archive


def archive_signal(node, time=None):
    """ use time if tree is archive """
    """ else use TIME node """
    print('archive_signal')
    try:
        # use _time variable if Tree is ARCHIVE
        if str(node.tree.tree) == 'ARCHIVE':
            try:
                time = archive.TimeInterval(time)
            except:
                time = archive.TimeInterval([-1800.,0])
        else:
            time = archive.TimeInterval(node.getNode('\TIME').data())
        # load channels by datastream + index
        try:
            idx = [node.getNode('$IDX').data()]
            url = node.getParent().getNode('$URL').data()
        except:
            idx = []
            url = node.getNode('$URL').data()
        # request signal
        signal = archive.read_signal(url, time, time.t0T, 0, 0, idx)
        # generate help text (HELP, DESCRIPTION, $NAME)
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
        # generate dummy signal with error message as help text
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
