def opttest(node,time=None):
    if time is None:
        time=[0,1]
    return(time)        


def optNode(treename='test',shot=3):
    import MDSplus
    with MDSplus.Tree(treename,shot,'new') as tree:
        tree.addNode('opt','Numeric')
        tree.write()
def setNode(treename='test',shot=3):
    import MDSplus
    with MDSplus.Tree(treename,shot) as tree:
        node = tree.getNode('opt')
        MDSplus.TdiExecute('_time=*') 
        node.putData(MDSplus.TdiCompile(b'opttest($,IF_ERROR(_time,*))',(node,)))
    
setNode()