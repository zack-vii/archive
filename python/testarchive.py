"""
Created on Fri Sep 11 13:08:04 2015
testarchive
@author: Cloud
"""
import archive
print('<utc time>  ', archive.Time().utc)
print('<local time>', archive.Time().local)
print('<utc now> - 30min , <utc now>')
print(repr(archive.TimeInterval()))
print(1,archive.Time('2009-02-13T23:31:30.123456789Z')==1234567890123456789)
print(2,'Thu Aug 12 13:14:15 2010'==archive.Time('2010-08-12 13:14:15', local=True).local)
print(3,archive.Path()._set_streamgroup('streamgroup')._set_stream('stream').path_data(channel=7,scaled='scaled',time=archive.TimeInterval())=='/ArchiveDB/codac/W7X/streamgroup/stream_DATASTREAM/7/scaled')
