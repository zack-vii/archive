def archive_program(node, time=None):
    """ use time if tree is archive """
    """ else use TIME node """
    from archive import base, interface
    from MDSplus import TreeNode, Tree
    print('archive_program')
    try:
        if not isinstance(node, (TreeNode)):
            node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        if node.tree.shot == -1:
            try:    time = base.TimeInterval(time)
            except: time = base.TimeInterval([-1800.,0,0])
        else:
            time = base.TimeInterval(node.getNode('\TIME').data())
        prog = interface.get_program(time)[-1]
        return prog[node.node_name.lower()]
    except:
        import getpass
        from archive import support
        user = getpass.getuser()
        print(user+": "+support.error())
