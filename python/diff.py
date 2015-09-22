from __future__ import print_function
from archive.deepdiff import DeepDiff as diffdict
from archive.support import getFlags, obj
from pprint import pprint  # analysis:ignore
from MDSplus import Tree, TdiDecompile


def difftree(treename1, shot1, treename2, shot2, exclude):
    """
    dd = difftree('W7X', -1, 'W7X', 100, '\ARCHIVE::TOP')
    pprint(dd[0])
    """
    treedict1 = treeToDict(Tree(treename1, shot1), exclude)
    treedict2 = treeToDict(Tree(treename2, shot2), exclude)
    treediff = diffdict(treedict1, treedict2)
    return treediff, obj(treedict1), obj(treedict2)

def treeToDict(tree, exclude=[]):
    def nodeToDict(node, exclude=['ARCHIVE','TIMING']):
        dic = {}
        dic["usage"] = str(node.usage)
        if dic["usage"] != "SIGNAL":
            try:
                dic["record"] = TdiDecompile(node.record)
            except:
                dic["record"] = '*' # No data stored
        else:
            dic["record"] = '<signal>'
        dic["flags"] = getFlags(node)
        dic["tags"] = list(map(str,node.tags))
        for desc in node.getDescendants():
            if not str(desc.getPath()) in exclude:
                dic[str(desc.getNodeName())] = nodeToDict(desc, exclude)
        return dic
    if isinstance(tree, Tree):
        tree = tree.getNode('\TOP')
    return nodeToDict(tree, exclude)
