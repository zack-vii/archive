"""
Created on Fri Sep 11 13:08:04 2015
testarchive
@author: Cloud
"""
from archive import *
print('<utc time>  ', Time().utc)
print('<local time>', Time().local)
print('<utc now> - 30min , <utc now>')
print(repr(TimeInterval()))
print(1,Time('2009-02-13T23:31:30.123456789Z')==1234567890123456789)
print(2,'Thu Aug 12 13:14:15 2010'==Time('2010-08-12 13:14:15', local=True).local)
print(3,Path()._set_streamgroup('streamgroup')._set_stream('stream').path_data(channel=7,scaled='scaled',time=TimeInterval())=='/ArchiveDB/codac/W7X/streamgroup/stream_DATASTREAM/7/scaled')
sig=interface.read_signal('/ArchiveDB/codac/W7X/CoDaStationDesc.16007/DataReductionProcessDesc.17547_DATASTREAM',time=[1458645620907337340,1458645621902337341],channel=17);
print(4,sig.data()[0]==4645)
print(5,calibrations.ECEcalib(sig,17)[1]==0.0007008221583731498)
print(6,support.getTiming(160310007)==support.getTiming('XP:20160310.7'))