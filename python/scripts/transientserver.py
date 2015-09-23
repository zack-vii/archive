#!/usr/bin/python
"""
Created on Mon Sep 21 19:01:21 2015
transientserver
@author: Cloud
"""
from archive import transient
from sys import argv
timeout = 60
if len(argv)>1:
    timeout = int(argv[1])

#try:
transient.server().autorun(timeout)
#except Exception as exc:
#    print(exc)
