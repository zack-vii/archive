"""
Created on Fri Sep 11 13:08:04 2015
testarchive
@author: Cloud
"""
import archive
print('<utc time>')
print(archive.Time().utc)
print('<local time>')
print(archive.Time().local)
print('1234567890123456789')
print(archive.Time('2009-02-13T23:31:30.123456789Z'))
print('<utc now> - 30min , <utc now>')
print(repr(archive.TimeInterval()))
print('Thu Aug 12 13:14:15 2010')
print(archive.Time('2010-08-12 13:14:15', local=True).local)
