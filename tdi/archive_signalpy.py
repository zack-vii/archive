def archive_signalpy(node, time=None, cache=None):
    """ use time if tree is archive """
    """ else use TIME node """
    from archive import base, interface
    from MDSplus import TreeNode, Tree
    print('archive_signal')
    try:
        if not isinstance(node, (TreeNode)):
           node = Tree('archive',-1).getNode(node)
        # use _time variable if Tree is ARCHIVE
        if node.tree.shot == -1:
            try:
                time = base.TimeInterval(time)
            except:
                time = base.TimeInterval([-1800.,0,0])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        # load channels by datastream + index
        try:
            idx = node.getNode('$IDX').data()
            url = node.getParent().getNode('$URL').data()
        except:
            idx = None
            url = node.getNode('$URL').data()
        # request signal
        signal = interface.read_signal(url, time, time.t0T, cache=cache, channel=idx)
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
        from archive import support
        user = getpass.getuser()
        help = user+": "+support.error()
        try:
            from MDSplus import Signal
            signal = Signal([6,66,666])
            signal.setHelp(help.split('\n')[-1])
            return(signal)
        except:
            return(help)
