from __future__ import print_function
from archive import deepdiff, version
from pprint import pprint


def diff(treename1, shot1, treename2, shot2):
    import MDSplus
    tree1 = MDSplus.Tree(treename1, shot1)
    tree2 = MDSplus.Tree(treename2, shot2)
    return treeToDict(tree1),treeToDict(tree2)


def treeToDict(tree):
    global i
    i = 0
    def nodeToDict(node):
        global i
        i += 1
        dic = {}
        dic["usage"] = version.tostr(node.usage)
        try:
            dic["record"] = node.record
        except:
            pass  # No data stored
        dic["state"] = node.state
        dic["on"] = node.on
        dic["compressible"] = node.compressible
        dic["compress_on_put"] = node.compress_on_put
        dic["do_not_compress"] = node.do_not_compress
        dic["include_in_pulse"] = node.include_in_pulse
        dic["setup_information"] = node.setup_information
        dic["tags"] = list(map(version.tostr,node.tags.tolist()))
        if (i % 1000) == 0: print(node.getFullPath())
        descdic = {}
        for desc in list(node.getDescendants()):
            descdic[str(desc.getNodeName())] = nodeToDict(desc)
        dic['descendants'] = descdic
        return dic
    return nodeToDict(tree.getNode('\Top'))

d = diff('w7x', 1, 'w7x', 2)
res = deepdiff.DeepDiff(*d)
pprint(res)