"""
Created on Thu Oct  1 14:17:28 2015

@author: Cloud
"""
import re as _re

getlength = _re.compile('([^\[]+)(?:\[([0-9A-Z]+)\])?')
_dtypes = {
           'float':'f',#4
           'double':'d',#8
           'char':'b',#1
           'byte':'B',#1
           'word':'H',#2
           'dword':'L',#4
           'short':'h',#2
           'ushort':'H',#2
           'unsigned int':'L',#4
           'long':'l',#4
           }

_lengths = {
            'HDRNAMEMAX':120,  # Max char str length for file name
            'USERINFOMAX':1000,  # User information space
            'COMMENTMAX':80,  # User comment string max length(5 comments)
            'LABELMAX':16,  # Label string max length
            'FILEVERMAX':16,  # File version string max length
            'DATEMAX':10,  # String length of file creation date string as ddmmmyyyy\0
            'ROIMAX':10,  # Max size of roi array of structures
            'TIMEMAX':7,  # Max time store as hhmmss\0
            }

def generate(filepath='M:\header.txt'):
    with open(filepath,'r') as f:
        for line in f:
            try:double
                transline(line.rstrip('\n'))
            except:
                print(line)




def transline(line):
    # split = datatype,name,offset,comment
    split = line.split(' ',3)
    rematch = getlength.search(split[1])
    dtype = _dtypes[split[0].lower()]
    name = rematch.group(1)
    if name.startswith('Spare_') or name.startswith('reserved'):
        com = '# '
    else:
        com = ''
    offset = int(split[2])
    if len(split)<4:
        comment = name
    else:
        comment = split[3]
    length = rematch.group(2)
    if length is None:
        length=1
    elif length.isdecimal():
        length = int(length)
    else:
        length = _lengths[length]
    print("        #  %s" % (comment))
    if length==1:
        if comment.startswith('T/F') or comment.startswith('On/Off'):
            print("        %sself.parlog['%s'] = _struct.unpack_from('%s', header, offset=%d)[0]>0" % (com, name, dtype.upper(), offset))
        else:
            print("        %sself.parlog['%s'] = _struct.unpack_from('%s', header, offset=%d)[0]" % (com, name, dtype, offset))
    else:
        if dtype=='b':
            print("        %sself.parlog['%s'] = _ver.tostr(_struct.unpack_from('%ds', header, offset=%d)[0]).rstrip('\\x00')" % (com, name, length, offset))
        else:
            print("        %sself.parlog['%s'] = _struct.unpack_from('%d%s', header, offset=%d)" % (com, name, length, dtype, offset))
