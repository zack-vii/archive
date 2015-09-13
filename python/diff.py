from __future__ import print_function


def diff(treename1, shot1, treename2, shot2):
    from archive import deepdiff as dd
    from pprint import pprint
    import MDSplus
    tree1 = MDSplus.Tree(treename1, shot1)
    tree2 = MDSplus.Tree(treename2, shot2)
    res = dd.DeepDiff(treeToDict(tree1), treeToDict(tree2))
    pprint(res)
    return res


def treeToDict(tree):
    def nodeToDict(node):
        dic = {}
        dic["usage"] = node.usage
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
        dic["tags"] = node.tags.tolist()
        descdic = {}
        for desc in list(node.getDescendants())[0:5]:
            descdic[str(desc.getNodeName())] = nodeToDict(desc)
        dic['descendants'] = descdic
        return dic
    return nodeToDict(tree.getNode('\Top'))

d = diff('test', -1, 'test', 1)
