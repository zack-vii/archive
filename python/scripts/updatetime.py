#!/usr/bin/python
"""
Created on Fri Jul 10 05:29:47 2015
updateTime
@author: Cloud
"""
from archive import TimeInterval,setTIME
from MDSplus import Tree, Event
from time import sleep
from sys import argv
timer = 10
if len(argv)>1:
    timer = int(argv[1])

try:
    tree=Tree('archive',7)
except:
    tree=Tree('archive',-1).createPulse(7)
    setTIME(TimeInterval(-1800.,0,-1))
while True:
    Event.seteventRaw('archive7')
    sleep(timer)
