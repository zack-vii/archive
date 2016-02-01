def archive_signalpy(node, time=None, cache=None):
    """ use time if tree is archive """
    """ else use TIME node """
    from archive import base, interface
    from MDSplus import TreeNode, Tree, Data, mdsExceptions
    print('archive_signalpy')
    try:
        if not isinstance(node, (TreeNode)):
           node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        if node.tree.shot == -1:
            try:    time = base.TimeInterval(time)
            except: time = base.TimeInterval([-1800.,0,0])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        """handle arguments"""
        kwargs = {}
        if str(node.node_name)[0]=='$':
            kwargs['scaled'] = str(node.node_name)[1:].lower()
            node = node.getParent()
        else:
            try:    kwargs['scaling'] = node.getNode('AIDEVSCALING').data()
            except: pass
            try:
                kwargs['value'] = node.getNode('$VALUE').data()
            except:
                try:
                    gain = node.getNode('GAIN').data()
                    zero = node.getNode('ZEROOFFSET').data()
                    if gain!=1. or zero!=0.:
                        kwargs['value'] = Data.compile('$VALUE*$+$',(gain,zero))
                except Exception as exc:
                    if not isinstance(exc,mdsExceptions.TreeNNF):
                        print(exc)
        if cache is not None: kwargs['cache'] = cache
        try: # load channels by datastream + index
            kwargs['channel'] = node.getNode('$IDX').data()
            url = node.getParent().getNode('$URL').data()
        except:
            url = node.getNode('$URL').data()
        """ request signal """
        signal = interface.read_signal(url, time, **kwargs)
        """ generate help text (HELP, DESCRIPTION, $NAME) """
        try:        help = node.getNode('HELP').data()
        except:
            try:    help = node.getNode('DESCRIPTION').data()
            except: help = node.getNode('$NAME').data()
        signal.setHelp(str(help))
        return(signal)
    except:
        """ generate dummy signal with error message as help text """
        """ generate dummy signal with error message as help text """
        import getpass,sys
        user = getpass.getuser()
        e = sys.exc_info()[1]
        help = user+": "+str(e)
        try:
            from MDSplus import Signal
            signal = Signal([6,66,666])
            signal.setHelp(help.split('\n')[-1])
            return(signal)
        except:
            return(help)
