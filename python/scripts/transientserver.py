# -*- coding: utf-8 -*-
"""
Created on Mon Sep 21 19:01:21 2015
transientserver
@author: Cloud
"""
import archive
import sys
timeout = 60
if len(sys.argv)>1:
    timeout = int(sys.argv[1])

server = archive.transient.server()
server.autorun(timeout)