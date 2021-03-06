def archive_log(node, time=None, cache=None):
    """ use time if tree is archive """
    """ else use TIME node """
    print('archive_log')
    try:
        from archive import base, interface
        from MDSplus import TreeNode, Tree
        if not isinstance(node, (TreeNode)):
            node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        try:
            node.getNode('$CFGLOG')
            url = base.Path(node.getNode('$URL').data()).url_cfglog()
        except:
          try:
            node.getNode('$PARLOG')
            url = base.Path(node.getNode('$URL').data()).url_parlog()
          except:
              raise Exception('could not specify log type. No Member named $CFGLOG or $PARLOG')
        if node.tree.shot == -1:
            try:    time = base.TimeInterval(time)
            except: time = base.TimeInterval(['now'])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        """ request signal """
        return str(interface._get_json(url, time=time).read())
    except:
        """ generate error message"""
        import getpass,sys
        user = getpass.getuser()
        e = sys.exc_info()
        help = user+': '+str(e[1])+', %d' % e[2].tb_lineno
        print(help)
        return help
