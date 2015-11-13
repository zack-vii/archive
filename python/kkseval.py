"""
Created on Wed Aug 05 12:31:36 2015
kkseval
@author: cloud
"""
import MDSplus
from .base import createSignal  # analysis:ignore


def createSectionNode(KKS, section, shot=-1):
    try:
        with MDSplus.Tree(KKS+'_EVAL', shot, 'edit') as tree:
            tree.DATA.addNode(section, 'STRUCTURE')
            tree.write()
        if shot > -1:
            try:
                createSectionNode(KKS, section, -1)
            except:
                print('in the model -1 the node probably already existed')
    except MDSplus._treeshr.TreeException:
        print('Section could not be created. Check your KKS.')
        raise


def createSignalNode(KKS, section, signal, shot=-1):
    try:
        with MDSplus.Tree(KKS+'_EVAL', shot, 'edit') as tree:
            tree.DATA.getNode(section).addNode(signal, 'SIGNAL')
            tree.write()
        if shot > -1:
            try:
                createSignalNode(KKS, section, signal, -1)
            except:
                print('in the model -1 the node probably already existed')
    except MDSplus._treeshr.TreeException:
        print('Signal could not be created. ' +
              'Check your input or create section.\ntype:')
        print('createSectionNode(\'%s\', \'%s\', %d)' % (KKS, section, shot))
        raise


def writeSignalNode(node, data):
    try:
        if isinstance(node, (list, tuple)):
            (KKS, section, signal, shot) = node
        with MDSplus.Tree(KKS+'_EVAL', shot, 'edit') as tree:
            tree.DATA.getNode(section).getNode(signal).putData(data)
    except MDSplus._treeshr.TreeException:
        print('Signal data could not be written.' +
              'Check your KKS or create signal.\ntype:')
        print('createSignalNode(\'%s\', \'%s\', \'%s\', %d)' % (KKS, section, signal, shot))  # analysis:ignore
        raise
