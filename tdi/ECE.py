from MDSplus import TreeNode, Tree, TdiCompile
from archive import TimeInterval, calibrations
def ECE(node, time=None):
    """ if tree is archive use time """
    """ else use TIME node """
    print('archive_program')
    try:
        if not isinstance(node, (TreeNode)):
            node = Tree('archive',-1).getNode(node)
        """ use _time variable if Tree is ARCHIVE """
        if node.tree.shot == -1:
            try:    time = TimeInterval(time)
            except: time = TimeInterval([-1800.,0,0])
        else:
            time = TimeInterval(node.getNode('\TIME').data())
        ecechannel = int(node.getNode('$IDX').data())+1
        timestamp = time.t0T.ns
        sig = node.evaluate()
        caldata = calibrations.ECEcalib(sig,ecechannel,timestamp)
        offset = caldata['offset']
        factor = caldata['factor']
        args = list(sig.args)
        args[1] = args[0]
        args[0] = TdiCompile('Build_With_Units(($VALUE-$)*$,"eV")',(offset,factor))
        sig.args = tuple(args)
        return sig
    except:
        import getpass
        from archive import support
        user = getpass.getuser()
        print(user+": "+support.error())
