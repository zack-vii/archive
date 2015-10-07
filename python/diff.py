import MDSplus as _mds
from . import support as _sup
from . import version as _ver


def difftree(treename1, shot1, treename2, shot2, exclude):
    """
    dd = difftree('W7X', -1, 'W7X', 100, '\ARCHIVE::TOP')
    pprint(dd[0])
    """
    treedict1 = treeToDict(_mds.Tree(treename1, shot1), exclude)
    treedict2 = treeToDict(_mds.Tree(treename2, shot2), exclude)
    treediff = DeepDiff(treedict1, treedict2)
    return treediff, _sup.obj(treedict1), _sup.obj(treedict2)

def treeToDict(tree, exclude=[]):
    def nodeToDict(node, exclude):
        dic = {}
        dic["usage"] = str(node.usage)
        if dic["usage"] != "SIGNAL":
            try:
                dic["record"] = _mds.TdiDecompile(node.record)
            except:
                dic["record"] = '*' # No data stored
        else:
            dic["record"] = '<signal>'
        dic["flags"] = _sup.getFlags(node)
        dic["tags"] = list(map(str,node.tags))
        for desc in node.getDescendants():
            if not str(desc.getPath()) in exclude:
                dic[str(desc.getNodeName())] = nodeToDict(desc, exclude)
        return dic
    if isinstance(tree, _mds.Tree):
        tree = tree.getNode('\TOP')
    return nodeToDict(tree, exclude)


class _ListItemRemovedOrAdded(object):
    pass


class DeepDiff(object):

    r"""
    **based on DeepDiff v 0.5.7**

    Deep Difference of dictionaries, iterables, strings and almost any other object. It will recursively look for all the changes.

    **Parameters**

    t1 : A dictionary, list, string or any python object that has __dict__ or __slots__
        This is the first item to be compared to the second item

    t2 : dictionary, list, string or almost any python object that has __dict__ or __slots__
        The second item is to be compared to the first one

    ignore_order : Boolean, default=False ignores orders and duplicates for iterables if it they have hashable items

    **Returns**

        A DeepDiff object that has already calculated the difference of the 2 items.

    **Supported data types**

    int, string, unicode, dictionary, list, tuple, set, frozenset, OrderedDict, NamedTuple and custom objects!

    """
    class _change(object):
        def __init__(self, key, old, new):
            self.key = key
            self.old = old
            self.new = new

        def __repr__(self):
            return str(self.key)+' : '+str(self.old)+' => '+str(self.new)


    def __init__(self, t1, t2, ignore_order=False, case=True):
        self.ignore_order = ignore_order
        self.case = case
        self.value = []
        self.type = []
        self.dict_add = []
        self.dict_rem = []
        self.iterable_add = []
        self.iterable_rem = []
        self.attribute_add = []
        self.attribute_rem = []
        self.set_add = []
        self.set_rem = []
        self.unprocessed = []
        self.__diff(t1, t2, parents_ids=frozenset({id(t1)}))

    def __repr__(self):
        lines = ['<type DeepDiff>']
        for i in self.value:
            lines.append('value:         '+repr(i))
        for i in self.type:
            lines.append('type:          '+repr(i))
        for i in self.dict_add:
            lines.append('dict_add:      '+repr(i))
        for i in self.dict_rem:
            lines.append('dict_rem:      '+repr(i))
        for i in self.iterable_add:
            lines.append('iterable_add:  '+repr(i))
        for i in self.iterable_rem:
            lines.append('iterable_rem:  '+repr(i))
        for i in self.attribute_add:
            lines.append('attribute_add: '+repr(i))
        for i in self.attribute_rem:
            lines.append('attribute_rem: '+repr(i))
        for i in self.set_add:
            lines.append('set_add:       '+repr(i))
        for i in self.set_rem:
            lines.append('set_rem:       '+repr(i))
        for i in self.unprocessed:
            lines.append('unprocessed:   '+repr(i))
        return('\n'.join(lines))

    def all(self):
        return(self.value + self.type +
               self.dict_add + self.dict_rem +
               self.iterable_add + self.iterable_rem +
               self.attribute_add + self.attribute_rem +
               self.set_add + self.set_rem +
               self.unprocessed)

    @staticmethod
    def __gettype(obj):
        '''
        python 3 returns <class 'something'> instead of <type 'something'>.
        For backward compatibility, we replace class with type.
        '''
        return str(type(obj)).replace('class', 'type')

    def __diff_obj(self, t1, t2, parent, parents_ids=frozenset({})):
        ''' difference of 2 objects '''

        try:
            t1 = t1.__dict__
            t2 = t2.__dict__
        except AttributeError:
            try:
                t1 = {i: getattr(t1, i) for i in t1.__slots__}
                t2 = {i: getattr(t2, i) for i in t2.__slots__}
            except AttributeError:
                self.unprocessed.append(DeepDiff._change(parent, t1, t2))
                return

        self.__diff_dict(t1, t2, parent, parents_ids, print_as_attribute=True)

    def __diff_dict(self, t1, t2, parent, parents_ids=frozenset({}), print_as_attribute=False):
        ''' difference of 2 dictionaries '''

        if not self.case:
            t1 = dict([k.upper(), v] for k, v in t1.items())
            t2 = dict([k.upper(), v] for k, v in t2.items())


        t1_keys, t2_keys = [
            set(d.keys()) for d in (t1, t2)
        ]

        t_keys_intersect = t2_keys.intersection(t1_keys)

        t_keys_added = list(t2_keys - t_keys_intersect)
        t_keys_removed = list(t1_keys - t_keys_intersect)

        if print_as_attribute:
            for k in t_keys_added:
                self.attribute_add.append(parent+[k])
            for k in t_keys_removed:
                self.attribute_rem.append(parent+[k])
        else:
            for k in t_keys_added:
                self.dict_add.append(parent+[k])
            for k in t_keys_removed:
                self.dict_rem.append(parent+[k])

        self.__diff_common_children(t1, t2, t_keys_intersect, print_as_attribute, parents_ids, parent)

    def __diff_common_children(self, t1, t2, t_keys_intersect, print_as_attribute, parents_ids, parent):
        ''' difference between common attributes of objects or values of common keys of dictionaries '''
        for item_key in t_keys_intersect:
            t1_child = t1[item_key]
            t2_child = t2[item_key]

            item_id = id(t1_child)

            if parents_ids and item_id in parents_ids:
                # print ("Warning, a loop is detected in {}.\n".format(parent_text % (parent, item_key_str)))
                continue

            parents_added = set(parents_ids)
            parents_added.add(item_id)
            parents_added = frozenset(parents_added)

            self.__diff(t1_child, t2_child, parent=parent+[item_key], parents_ids=parents_added)

    def __diff_set(self, t1, t2, parent=[]):
        ''' difference of sets '''
        items_added = list(t2 - t1)
        items_removed = list(t1 - t2)

        for i in items_added:
            self.set_add.append(parent+[i])

        for i in items_removed:
            self.set_rem.append(parent+[i])

    def __diff_iterable(self, t1, t2, parent=[], parents_ids=frozenset({})):
        '''
        difference of iterables except dictionaries, sets and strings.
        '''
        items_removed = []
        items_added = []

        for i, (x, y) in enumerate(_ver.zip_longest(t1, t2, fillvalue=_ListItemRemovedOrAdded)):

            if y is _ListItemRemovedOrAdded:
                items_removed.append(x)
            elif x is _ListItemRemovedOrAdded:
                items_added.append(y)
            else:
                self.__diff(x, y, parent+[i], parents_ids)

        for i in items_added:
            self.iterable_add.append(parent+[i])

        for i in items_removed:
            self.iterable_rem.append(parent+[i])

    def __diff_tuple(self, t1, t2, parent, parents_ids):
        # Checking to see if it has _fields. Which probably means it is a named tuple.
        try:
            t1._fields
        # It must be a normal tuple
        except:
            self.__diff_iterable(t1, t2, parent, parents_ids)
        # We assume it is a namedtuple then
        else:
            self.__diff_obj(t1, t2, parent, parents_ids)

    def __diff(self, t1, t2, parent=[], parents_ids=frozenset({})):
        ''' The main diff method '''

        if isinstance(t1, _ver.basestring):
            t1 = str(t1)

        if isinstance(t2, _ver.basestring):
            t2 = str(t2)

        if t1 is t2:
            return

        if type(t1) != type(t2):
            self.type.append(DeepDiff._change(parent, self.__gettype(t1), self.__gettype(t2)))

        elif isinstance(t1, _ver.basestring):
            if t1 != t2:
                self.value.append(DeepDiff._change(parent, t1, t2))

        elif isinstance(t1, _ver.numbers):
            if t1 != t2:
                self.value.append(DeepDiff._change(parent, t1, t2))

        elif isinstance(t1, dict):
            self.__diff_dict(t1, t2, parent, parents_ids)

        elif isinstance(t1, tuple):
            self.__diff_tuple(t1, t2, parent, parents_ids)

        elif isinstance(t1, (set, frozenset)):
            self.__diff_set(t1, t2, parent)

        else:
            self.__diff_obj(t1, t2, parent, parents_ids)

        return
