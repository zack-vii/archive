#!/usr/bin/python
"""
Created on Mon Sep 21 10:27:18 2015

@author: Cloud
"""
import archive
import numpy
import time
import sys

name = 'TEST1'
rate = 1.1
period = 60.
if len(sys.argv)>1:
    name = sys.argv[1]
if len(sys.argv)>2:
    rate = float(sys.argv[2])
if len(sys.argv)>3:
    period = float(sys.argv[3])

client = archive.transient.client(name)
client.config = {'samplingrate': rate, 'signal':{'function': 'sine', 'period': period} }
client.units = 'V'
client.description = 'this is a transient test signal'
client.notify()
while True:
    try:
        now  = time.time()  # time in s
        data = numpy.sin(now/period*2*numpy.pi)  # artificial signal
        dim  = int(now*1e9)  # time in ns
        print(client.putFloat64(data,dim))
    except:
        archive.support.error()
    time.sleep(rate-(time.time() % rate))