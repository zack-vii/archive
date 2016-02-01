def archive_log(node, time=None, cache=None):
    """ use time if tree is archive """
    """ else use TIME node """
    from archive import base, interface
    from MDSplus import TreeNode, Tree
    print('archive_log')
    try:
        if not isinstance(node, (TreeNode)):
           node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        if node.tree.shot == -1:
            try:    time = base.TimeInterval(time)
            except: time = base.TimeInterval(['now'])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        if node.node_name=='$CFGLOG':
           url = base.Path(node.getNode('$URL').data()).url_cfglog()
        else:  # node_name=='$PARLOG'
           url = base.Path(node.getNode('$URL').data()).url_parlog()
        """ request signal """
        return interface.get_json(url, time)
    except:
        """ generate error message"""
        import getpass,sys
        user = getpass.getuser()
        e = sys.exc_info()[1]
        help = user+": "+str(e)
        return(help)
