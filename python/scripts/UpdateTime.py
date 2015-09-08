"""
Created on Fri Jul 10 05:29:47 2015
updateTime
@author: Cloud
"""
from codac import Time,setTIME
from MDSplus import Tree
from time import sleep
try:
    tree=Tree('archivesb',7)
except:
    tree=Tree('archivesb',-1).createPulse(7)
while True:
    t=Time();
    setTIME([t-1800000000000,t])
    sleep(10)