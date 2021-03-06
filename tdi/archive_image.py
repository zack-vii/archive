def archive_image(node, time=None):
    """ use time if tree is archive """
    """ else use TIME node """
    print('archive_image')
    try:
        from archive import base, interface
        from MDSplus import TreeNode, Tree
        if not isinstance(node, (TreeNode)):
            node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        if node.tree.shot == -1:
            try:    time = base.TimeInterval(time)
            except: time = base.TimeInterval([-1800.,0,0])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        url = base.Path(node.getNode('$URL').data()).url_datastream()
        """ request signal """
        signal = interface.read_pngs_url(url, time, 3)
        """ generate help text (HELP, DESCRIPTION, $NAME) """
        try:        help = node.getNode('HELP').data()
        except:
            try:    help = node.getNode('DESCRIPTION').data()
            except: help = node.getNode('$NAME').data()
        signal.setHelp(str(help))
        return signal
    except:
        local = locals()
        """ generate dummy signal with error message as help text """
        import getpass,sys
        e = sys.exc_info()
        user = getpass.getuser()
        help = user+': %s, %d' % (repr(e[1]), e[2].tb_lineno)
        print(help)
        print(local)
        try:
            from MDSplus import Signal
            signal = Signal([6,66,666])
            signal.setHelp(help.split('\n')[-1])
            return(signal)
        except:
            return help
