"""
archive
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
@ionspired by Kasey Russell (krussell@post.harvard.edu)
          and James Battat (jbattat@post.harvard.edu)
@license: GNU GPL
import archive as a;S=a.winspec.read("M:\Test109 1frame full image.SPE")
a.winspec.generateNode(S.parlog)

import archive as a;a.winspec.putSPE('CAMERA0', "M:\Test107 10frames full image.SPE", list(range(10)), -1)
"""
import struct as _struct
import numpy as _np
import MDSplus as _mds
from . import version as _ver

def putSPE(nodename, filepath, dim, shot=0):
    content = read(filepath)
    writedata(nodename, content.data, dim, shot)
    writeparlog(nodename, content.parlog, shot)


def writedata(nodename, data, dims, shot=0):
    """ data: list of images """
    """ dims: array of double seconds based on T1 """
    w7x   = _mds.Tree('W7X', shot)
    triax  = w7x.getNode('.QSQ.HARDWARE.TRIAX')
    node = triax.getNode(nodename)
    node.deleteData()
    for i in range(len(data)):
        dim = _mds.Float64Array(dims[i])
        dim.setUnits('s')
        node.makeSegment(dims[i], dims[i], dim, _mds.makeArray(data[i]), -1)

def writeparlog(parlog, shot=0, nodename='IMAGES', treename='QSQ'):
    w7x   = _mds.Tree('W7X', shot)
    tree  = w7x.getNode(treename)
    triax = tree.DATA.HEBEAM
    node = triax.getNode(nodename)
    def dicttotree(dic, node):
        for k,v in dic.items():
            print(node.getPath()+':'+k[0:12])
            newnode = node.getNode(k[0:12])
            if isinstance(v, dict):
                dicttotree(v, newnode)
            elif isinstance(v, (tuple, list)):
                newnode.putData(_mds.makeArray(v))
            else:
                newnode.putData(_mds.makeScalar(v))

    dicttotree(parlog, node)

def generateNode(parlog, shot=-1, nodename='IMAGES', treename='QSQ'):
    """Creates the DATA node of the IMAGES with its sub-structure using parlog"""
    with _mds.Tree(treename, shot, 'edit') as tree:
        # triax = tree.getNodeWild('HARDWARE.TRIAX')
        triax = tree.getNodeWild('DATA.HEBEAM')
        if len(triax):
            triax = triax[0]
        else:
            # triax = tree.getNode('HARDWARE').addNode('TRIAX','STRUCTURE')
            triax = tree.DATA.addNode('HEBEAM','STRUCTURE')
        node = triax.getNodeWild(nodename)
        if len(node):
            node = node[0]
            #raise Exception('Node '+str(node[0].getPath())+' already exists.' )
        else:
            node = triax.addNode(nodename,'SIGNAL')
        node.putData(tree.getNode('HARDWARE.TRIAX:CAMERA'+nodename[-1]))

        def dicttotree(dic, path):
            def addnode(path, usage):
                try:
                    node.addNode(path, usage)
                    print('creating '+path.upper())
                except Exception as exc:
                    if not str(exc).startswith('%TREE-W-ALREADY_THERE'):
                        raise exc
                    print('updating '+path.upper())
            for k,v in dic.items():
                newpath = path+':'+k[0:12]
                if isinstance(v, dict):
                    addnode(newpath, 'STRUCTURE')
                    dicttotree(v, newpath)
                    tree.write()
                else:
                    v = _np.array(v)
                    if v.dtype.descr[0][1][1] in 'SU':
                        addnode(newpath, 'TEXT')
                    elif v.dtype.descr[0][1][1] in 'if':
                        addnode(newpath, 'NUMERIC')
                    else:
                        addnode(newpath, 'ANY')
                    # node.getNode(newpath).putData(v.tolist())
        dicttotree(parlog, '')
        tree.write()



class read(object):
    def __init__(self, spefilename):
        # open SPE file as binary input
        spe = open(spefilename, "rb")

        # Header length is a fixed number
        nBytesInHeader = 4100

        # Read the entire header
        header = spe.read(nBytesInHeader)

        #  check the magic number of the header
        magic = _struct.unpack_from('h', header, offset=4098)[0]
        if magic != 21845:
            import warnings
            warnings.warn('invalid magic number')

        pi_max = {}
        version = {}
        adc = {}
        shutter = {}
        flatfield = {}
        background = {}
        exptime = {}
        absorbance = {}
        threshold = {}
        blemish = {}
        cosmic = {}
        trigger = {}
        timing = {}
        cleans = {}
        minblk = {}
        data = {}
        detector = {}
        virtchip = {}
        async = {}
        readout = {}
        avalanche = {}
        roix = {}
        roiy = {}
        roi = {'x':roix, 'y':roiy}

        specglue = {}
        specslit = {}
        specmirr = {}
        spec = {'glue':specglue, 'slit':specslit, 'mirror':specmirr}

        repetitive = {}
        seqstart = {}
        seqend = {}
        sequential = {'start':seqstart, 'end':seqend}
        pulser = {'repetitive':repetitive, 'sequential':sequential}
        xpolynom = {}
        ypolynom = {}
        calib_x = {'polynom':xpolynom}
        calib_y = {'polynom':ypolynom}
        calib = {'x':calib_x,'y':calib_y}
        self.parlog = {'adc':adc, 'pi_max':pi_max, 'roi':roi, 'calib':calib, 'async':async,
                       'pulser':pulser, 'spectrograph':spec, 'shutter':shutter, 'timing':timing,
                       'background':background, 'cleans':cleans, 'datetime':exptime,
                       'threshold':threshold, 'blemish':blemish, 'cosmic':cosmic,
                       'absorbance':absorbance, 'data':data, 'detector':detector,
                       'trigger':trigger, 'flat_field':flatfield, 'virtual_chip':virtchip,
                       'readout':readout, 'min_block':minblk, 'avalanche':avalanche}


        #  Hardware Version
        version['hardware'] = _struct.unpack_from('h', header, offset=0)[0]
        #  Definition of Output BNC
        self.parlog['logic_output'] = _struct.unpack_from('h', header, offset=2)[0]
        #  Amp Switching Mode (AmpHiCapLoNo)
        self.parlog['amp_mode'] = _struct.unpack_from('H', header, offset=4)[0]
        #  Detector x dimension of chip.
        detector['x_dimension'] = _struct.unpack_from('H', header, offset=6)[0]
        #  timing mode
        timing['mode'] = _struct.unpack_from('h', header, offset=8)[0]
        #  alternative exposure, in sec.
        timing['alt_exposure'] = _struct.unpack_from('f', header, offset=10)[0]
        #  Virtual Chip X dim
        virtchip['x_dimension'] = _struct.unpack_from('h', header, offset=14)[0]
        #  Virtual Chip Y dim
        virtchip['y_dimension'] = _struct.unpack_from('h', header, offset=16)[0]
        #  y dimension of CCD or detector.
        detector['y_dimension'] = _struct.unpack_from('H', header, offset=18)[0]
        #  date
        exptime['date'] = _ver.tostr(_struct.unpack_from('10s', header, offset=20)[0]).rstrip('\x00')
        #  On/Off
        virtchip['enabled'] = _struct.unpack_from('H', header, offset=30)[0]>0
        #  Spare_1
        # self.parlog['Spare_1'] = _ver.tostr(_struct.unpack_from('2s', header, offset=32)[0]).rstrip('\x00')
        #  Old number of scans - should always be -1
        # self.parlog['noscan'] = _struct.unpack_from('h', header, offset=34)[0]
        #  Detector Temperature Set
        detector['temperature'] = _struct.unpack_from('f', header, offset=36)[0]
        #  CCD/DiodeArray type
        detector['type'] = _struct.unpack_from('h', header, offset=40)[0]
        #  actual # of pixels on x axis
        data['x_dimension'] = _struct.unpack_from('H', header, offset=42)[0]
        #  trigger diode
        trigger['diode'] = _struct.unpack_from('h', header, offset=44)[0]
        #  Used with Async Mode
        async['delay'] = _struct.unpack_from('f', header, offset=46)[0]
        #  Normal, Disabled Open, Disabled Closed
        shutter['control'] = _struct.unpack_from('H', header, offset=50)[0]
        #  On/Off
        absorbance['live'] = _struct.unpack_from('H', header, offset=52)[0]>0
        #  Reference Strip or File
        absorbance['mode'] = _struct.unpack_from('H', header, offset=54)[0]
        #  T/F Cont/Chip able to do Virtual Chip
        virtchip['available'] = _struct.unpack_from('H', header, offset=56)[0]>0
        #  On/Off
        threshold['min_live'] = _struct.unpack_from('H', header, offset=58)[0]>0
        #  Threshold Minimum Value
        threshold['min_value'] = _struct.unpack_from('f', header, offset=60)[0]
        #  On/Off
        threshold['max_live'] = _struct.unpack_from('H', header, offset=64)[0]>0
        #  Threshold Maximum Value
        threshold['max_value'] = _struct.unpack_from('f', header, offset=66)[0]

        #  T/F Spectrograph Used
        spec['enabled'] = _struct.unpack_from('H', header, offset=70)[0]>0
        #  Center Wavelength in Nm
        spec['center_wavel'] = _struct.unpack_from('f', header, offset=72)[0]
        #  T/F File is Glued
        specglue['enabled'] = _struct.unpack_from('H', header, offset=76)[0]>0
        #  Starting Wavelength in Nm
        specglue['start_wavel'] = _struct.unpack_from('f', header, offset=78)[0]
        #  ending Wavelength in Nm
        specglue['end_wavel'] = _struct.unpack_from('f', header, offset=82)[0]
        #  Minimum Overlap in Nm
        specglue['min_overlap'] = _struct.unpack_from('f', header, offset=86)[0]
        #  Final Resolution in Nm
        specglue['final_res'] = _struct.unpack_from('f', header, offset=90)[0]

        #  0=None, PG200=1, PTG=2, DG535=3
        pulser['type'] = _struct.unpack_from('h', header, offset=94)[0]
        #  T/F Custom Chip Used
        self.parlog['custom_chip'] = _struct.unpack_from('H', header, offset=96)[0]>0

        #  Pre Pixels in X direction
        data['x_pre_pixels'] = _struct.unpack_from('h', header, offset=98)[0]
        #  Post Pixels in X direction
        data['x_pst_pixels'] = _struct.unpack_from('h', header, offset=100)[0]
        #  Pre Pixels in Y direction
        data['y_rep_pixels'] = _struct.unpack_from('h', header, offset=102)[0]
        #  Post Pixels in Y direction
        data['y_pst_pixels'] = _struct.unpack_from('h', header, offset=104)[0]
        #  asynchronous enable flag 0 = off
        async['enabled'] = _struct.unpack_from('H', header, offset=106)[0]>0
        #  experiment datatype 0 = FLOATING POINT,1 = LONG INTEGER,2 = INTEGER,3 = UNSIGNED INTEGER
        data['type'] = _struct.unpack_from('h', header, offset=108)[0]

        #  Repetitive/Sequential
        pulser['mode'] = _struct.unpack_from('h', header, offset=110)[0]
        #  Num PTG On-Chip Accums
        pulser['chip_accums'] = _struct.unpack_from('H', header, offset=112)[0]

        #  Num Exp Repeats (Pulser SW Accum)
        repetitive['repetitions'] = _struct.unpack_from('L', header, offset=114)[0]
        #  Width Value for Repetitive pulse (usec)
        repetitive['width'] = _struct.unpack_from('f', header, offset=118)[0]
        #  Width Value for Repetitive pulse (usec)
        repetitive['delay'] = _struct.unpack_from('f', header, offset=122)[0]

        #  Start Width for Sequential pulse (usec)
        seqstart['width'] = _struct.unpack_from('f', header, offset=126)[0]
        #  End Width for Sequential pulse (usec)
        seqend['width'] = _struct.unpack_from('f', header, offset=130)[0]
        #  Start Delay for Sequential pulse (usec)
        seqstart['delay'] = _struct.unpack_from('f', header, offset=134)[0]
        #  End Delay for Sequential pulse (usec)
        seqend['delay'] = _struct.unpack_from('f', header, offset=138)[0]
        #  Increments: 1=Fixed, 2=Exponential
        sequential['increments'] = _struct.unpack_from('h', header, offset=142)[0]

        #  PI-Max type controller flag
        pi_max['available'] = _struct.unpack_from('H', header, offset=144)[0]>0
        #  PI-Max mode
        pi_max['mode'] = _struct.unpack_from('h', header, offset=146)[0]
        #  PI-Max Gain
        pi_max['gain'] = _struct.unpack_from('h', header, offset=148)[0]

        #  1 if background subtraction done
        background['applied'] = _struct.unpack_from('H', header, offset=150)[0]>0
        #  T/F PI-Max 2ns Board Used
        pi_max['2nsBrd'] = _struct.unpack_from('H', header, offset=152)[0]>0
        #  min. # of strips per skips
        minblk['min_strips'] = _struct.unpack_from('H', header, offset=154)[0]
        #  # of min-blocks before geo skps
        minblk['pre_geo_skip'] = _struct.unpack_from('H', header, offset=156)[0]
        #  Spectro Mirror Location, 0=Not Present
        specmirr['location'] = _struct.unpack_from('2h', header, offset=158)
        #  Spectro Slit Location, 0=Not Present
        specslit['location'] = _struct.unpack_from('4h', header, offset=162)
        #  T/F Custom Timing Used
        timing['is_custom'] = _struct.unpack_from('H', header, offset=170)[0]>0

        #  ExperimentTimeLocal
        exptime['local'] = _ver.tostr(_struct.unpack_from('7s', header, offset=172)[0]).rstrip('\x00')
        #  Experiment UTC Time as hhmmss\0
        exptime['utc'] = _ver.tostr(_struct.unpack_from('7s', header, offset=179)[0]).rstrip('\x00')

        #  User Units for Exposure
        timing['exposr_units'] = _struct.unpack_from('h', header, offset=186)[0]

        #  ADC offset
        adc['offset'] = _struct.unpack_from('H', header, offset=188)[0]
        #  ADC rate
        adc['rate'] = _struct.unpack_from('H', header, offset=190)[0]
        #  ADC type
        adc['type'] = _struct.unpack_from('H', header, offset=192)[0]
        #  ADC resolution
        adc['resolution'] = _struct.unpack_from('H', header, offset=194)[0]
        #  ADC bit adjust
        adc['bit_adjust'] = _struct.unpack_from('H', header, offset=196)[0]


        #  gain
        self.parlog['gain'] = _struct.unpack_from('H', header, offset=198)[0]
        #  File Comments
        self.parlog['comments'] = _ver.tostr(_struct.unpack_from('5s', header, offset=200)[0]).rstrip('\x00')
        #  geometric ops: rotate 0x01,reverse,0x02, flip 0x04
        data['geometric'] = _struct.unpack_from('H', header, offset=600)[0]
        #  intensity display string
        data['x_label'] = _ver.tostr(_struct.unpack_from('16s', header, offset=602)[0]).rstrip('\x00')
        #  cleans
        cleans['cleans'] = _struct.unpack_from('H', header, offset=618)[0]
        #  number of skips per clean.
        cleans['num_skips'] = _struct.unpack_from('H', header, offset=620)[0]

        #  Spectrograph Mirror Positions
        specmirr['position'] = _struct.unpack_from('2h', header, offset=622)
        #  Spectrograph Slit Positions
        specslit['position'] = _struct.unpack_from('4f', header, offset=626)
        #  T/F
        cleans['auto'] = _struct.unpack_from('H', header, offset=642)[0]>0
        #  T/F
        cleans['cont_inst'] = _struct.unpack_from('H', header, offset=644)[0]>0
        #  Absorbance Strip Number
        absorbance['strips'] = _struct.unpack_from('h', header, offset=646)[0]
        #  Spectrograph Slit Position Units
        specslit['positionunit'] = _struct.unpack_from('h', header, offset=648)[0]
        #  Spectrograph Grating Grooves
        spec['grooves'] = _struct.unpack_from('f', header, offset=650)[0]
        #  number of source comp. diodes
        self.parlog['src_comp_d'] = _struct.unpack_from('h', header, offset=654)[0]
        #  y dimension of raw data.
        data['y_dimension'] = _struct.unpack_from('H', header, offset=656)[0]
        #  0=scrambled, 1=unscrambled
        self.parlog['scrambled'] = _struct.unpack_from('H', header, offset=658)[0]==0
        #  T/F Continuous Cleans Timing Option
        cleans['continuous'] = _struct.unpack_from('H', header, offset=660)[0]>0
        #  T/F External Trigger Timing Option
        trigger['external'] = _struct.unpack_from('H', header, offset=662)[0]>0
        #  Number of scans (Early WinX)
        self.parlog['scans'] = _struct.unpack_from('l', header, offset=664)[0]
        #  Number of Accumulations
        self.parlog['average'] = _struct.unpack_from('l', header, offset=668)[0]
        #  Experiment readout time
        readout['time'] = _struct.unpack_from('f', header, offset=672)[0]
        #  T/F Triggered Timing Option
        trigger['mode'] = _struct.unpack_from('H', header, offset=676)[0]>0
        #  Spare_2
        # self.parlog['Spare_2'] = _ver.tostr(_struct.unpack_from('10s', header, offset=678)[0]).rstrip('\x00')
        #  Version of SW creating this file
        version['software'] = _ver.tostr(_struct.unpack_from('16s', header, offset=688)[0]).rstrip('\x00')
        #  1 = new120 (Type II),2 = old120 (Type I ),3 = ST130,4 = ST121,5 = ST138,6 = DC131 (PentaMax),7 = ST133 (MicroMax/SpectroMax),8 = ST135 (GPIB),9 = VICCD,10 = ST116 (GPIB),11 = OMA3 (GPIB),12 = OMA4,
        self.parlog['type'] = _struct.unpack_from('h', header, offset=704)[0]
        #  1 if flat field was applied.
        flatfield['applied'] = _struct.unpack_from('H', header, offset=706)[0]>0
        #  Spare_3
        # self.parlog['Spare_3'] = _ver.tostr(_struct.unpack_from('16s', header, offset=708)[0]).rstrip('\x00')
        #  Kinetics Trigger Mode
        trigger['kinetic_mode'] = _struct.unpack_from('h', header, offset=724)[0]
        #  Data label.
        data['d_label'] = _ver.tostr(_struct.unpack_from('16s', header, offset=726)[0]).rstrip('\x00')
        #  Spare_4
        # self.parlog['Spare_4'] = _ver.tostr(_struct.unpack_from('436s', header, offset=742)[0]).rstrip('\x00')
        #  Name of Pulser File with Pulse Widths/Delays (for Z-Slice)
        pulser['filename'] = _ver.tostr(_struct.unpack_from('120s', header, offset=1178)[0]).rstrip('\x00')
        #  Name of Absorbance File (if File Mode)
        absorbance['filename'] = _ver.tostr(_struct.unpack_from('120s', header, offset=1298)[0]).rstrip('\x00')
        #  Number of Times experiment repeated
        self.parlog['exp_repeats'] = _struct.unpack_from('L', header, offset=1418)[0]
        #  Number of Time experiment accumulated
        self.parlog['exp_accums'] = _struct.unpack_from('L', header, offset=1422)[0]
        #  Set to 1 if this file contains YT data
        YT = _struct.unpack_from('H', header, offset=1426)[0]>0
        #  Vert Clock Speed in micro-sec
        timing['vert_clock'] = _struct.unpack_from('f', header, offset=1428)[0]
        #  set to 1 if accum done by Hardware.
        self.parlog['hw_accum'] = _struct.unpack_from('H', header, offset=1432)[0]>0
        #  set to 1 if store sync used
        async['store_sync'] = _struct.unpack_from('h', header, offset=1434)[0]
        #  set to 1 if blemish removal applied
        blemish['applied'] = _struct.unpack_from('h', header, offset=1436)[0]
        #  set to 1 if cosmic ray removal applied
        cosmic['applied'] = _struct.unpack_from('h', header, offset=1438)[0]
        #  if cosmic ray applied, this is type
        cosmic['type'] = _struct.unpack_from('h', header, offset=1440)[0]
        #  Threshold of cosmic ray removal.
        cosmic['threshold'] = _struct.unpack_from('f', header, offset=1442)[0]
        #  number of frames in file.
        data['frames'] = _struct.unpack_from('l', header, offset=1446)[0]
        #  max intensity of data (future)
        # data['maximum'] = _struct.unpack_from('f', header, offset=1450)[0]
        #  min intensity of data (future)
        # data['minimum'] = _struct.unpack_from('f', header, offset=1454)[0]
        #  y axis label.
        data['y_label'] = _ver.tostr(_struct.unpack_from('16s', header, offset=1458)[0]).rstrip('\x00')
        #  shutter type.
        shutter['type'] = _struct.unpack_from('H', header, offset=1474)[0]
        #  shutter compensation time.
        shutter['compensation'] = _struct.unpack_from('f', header, offset=1476)[0]
        #  readout mode, full, kinetics, etc.
        readout['mode'] = _struct.unpack_from('H', header, offset=1480)[0]
        #  window size for kinetics only.
        self.parlog['window_size'] = _struct.unpack_from('H', header, offset=1482)[0]
        #  clock speed for kinetics & frame transfer
        timing['clock_speed'] = _struct.unpack_from('H', header, offset=1484)[0]
        #  computer interface(isa, taxi, pci, eisa, etc.)
        self.parlog['interface'] = _struct.unpack_from('H', header, offset=1486)[0]
        #  May be more than the 10 allowed in this header (if 0, assume 1)
        roi['total'] = _struct.unpack_from('h', header, offset=1488)[0]
        #  Spare_5
        # self.parlog['Spare_5'] = _ver.tostr(_struct.unpack_from('16s', header, offset=1490)[0]).rstrip('\x00')
        #  if multiple controller system will have controller number data came from. This is a future item.
        self.parlog['controller'] = _struct.unpack_from('H', header, offset=1506)[0]
        #  Which software package created this file
        version['program'] = _struct.unpack_from('H', header, offset=1508)[0]


        #  ROI information
        #  number of ROIs used. if 0 assume 1.
        roi['stored'] = max(1,_struct.unpack_from('h', header, offset=1510)[0])
        roilist = range(roi['stored'])
        roix['start'] = [(_struct.unpack_from('H', header, offset=1512 + i*12)[0]) for i in roilist]
        roix['end']   = [(_struct.unpack_from('H', header, offset=1514 + i*12)[0]) for i in roilist]
        roix['group'] = [(_struct.unpack_from('H', header, offset=1516 + i*12)[0]) for i in roilist]
        roiy['start'] = [(_struct.unpack_from('H', header, offset=1518 + i*12)[0]) for i in roilist]
        roiy['end']   = [(_struct.unpack_from('H', header, offset=1520 + i*12)[0]) for i in roilist]
        roiy['group'] = [(_struct.unpack_from('H', header, offset=1522 + i*12)[0]) for i in roilist]

        #  Flat field file name.
        flatfield['filename'] = _ver.tostr(_struct.unpack_from('120s', header, offset=1632)[0]).rstrip('\x00')
        #  background sub. file name.
        background['filename'] = _ver.tostr(_struct.unpack_from('120s', header, offset=1752)[0]).rstrip('\x00')
        #  blemish file name.
        blemish['filename'] = _ver.tostr(_struct.unpack_from('120s', header, offset=1872)[0]).rstrip('\x00')
        #  version of this file header
        version['header'] = _struct.unpack_from('f', header, offset=1992)[0]
        #  Reserved for YT information
        if YT:
            self.parlog['yt_info'] = _struct.unpack_from('1000b', header, offset=1996)
        else:
            self.parlog['yt_info'] = []
        #  == 0x01234567L if file created by WinX
        self.parlog['winview_id'] = _struct.unpack_from('l', header, offset=2996)[0]


        #  offset for absolute data scaling
        calib_x['offset'] = _struct.unpack_from('d', header, offset=3000)[0]
        #  factor for absolute data scaling
        calib_x['factor'] = _struct.unpack_from('d', header, offset=3008)[0]
        #  selected scaling unit
        calib_x['current_unit'] = _struct.unpack_from('b', header, offset=3016)[0]
        #  reserved
        #  calib_x['reserved1'] = _struct.unpack_from('b', header, offset=3017)[0]
        #  special string for scaling
        calib_x['string'] = _ver.tostr(_struct.unpack_from('40s', header, offset=3018)[0]).rstrip('\x00')
        #  reserved
        #  calib_x['reserved2'] = _ver.tostr(_struct.unpack_from('40s', header, offset=3058)[0]).rstrip('\x00')
        #  flag if calibration is valid
        calib_x['valid'] = _struct.unpack_from('B', header, offset=3098)[0]>0
        #  current input units for "calib_value"
        calib_x['input_unit'] = _struct.unpack_from('b', header, offset=3099)[0]
        #  linear UNIT and used in the "polynom_coeff"
        xpolynom['unit'] = _struct.unpack_from('b', header, offset=3100)[0]
        #  ORDER of calibration POLYNOM
        xpolynom['order'] = _struct.unpack_from('b', header, offset=3101)[0]
        #  valid calibration data pairs
        calib_x['count'] = _struct.unpack_from('b', header, offset=3102)[0]
        #  pixel pos. of calibration data
        calib_x['pixel_pos'] = _struct.unpack_from('10d', header, offset=3103)
        #  calibration VALUE at above pos
        calib_x['value'] = _struct.unpack_from('10d', header, offset=3183)
        #  polynom COEFFICIENTS
        xpolynom['coefficients'] = _struct.unpack_from('6d', header, offset=3263)
        #  laser wavenumber for relativ WN
        calib_x['laser_pos'] = _struct.unpack_from('d', header, offset=3311)[0]
        #  reserved
        #  calib_x['reserved3'] = _struct.unpack_from('b', header, offset=3319)[0]
        #  If set to 200, valid label below
        calib_x['label_valid'] = _struct.unpack_from('B', header, offset=3320)[0]==200
        #  Calibration label (NULL term'd)
        calib_x['label'] = _ver.tostr(_struct.unpack_from('81s', header, offset=3321)[0]).rstrip('\x00')
        #  Calibration Expansion area
        calib_x['expansion'] = _ver.tostr(_struct.unpack_from('87s', header, offset=3402)[0]).rstrip('\x00')


        #  offset for absolute data scaling
        calib_y['offset'] = _struct.unpack_from('d', header, offset=3489)[0]
        #  factor for absolute data scaling
        calib_y['factor'] = _struct.unpack_from('d', header, offset=3497)[0]
        #  selected scaling unit
        calib_y['current_unit'] = _struct.unpack_from('b', header, offset=3505)[0]
        #  reserved
        #  calib_y['reserved1'] = _struct.unpack_from('b', header, offset=3506)[0]
        #  special string for scaling
        calib_y['string'] = _ver.tostr(_struct.unpack_from('40s', header, offset=3507)[0]).rstrip('\x00')
        #  reserved
        #  calib_y['reserved2'] = _ver.tostr(_struct.unpack_from('40s', header, offset=3547)[0]).rstrip('\x00')
        #  flag if calibration is valid
        calib_y['valid'] = _struct.unpack_from('B', header, offset=3587)[0]>0
        #  current input units for "calib_value"
        calib_y['input_unit'] = _struct.unpack_from('b', header, offset=3588)[0]
        #  linear UNIT and used in the "polynom_coeff"
        ypolynom['unit'] = _struct.unpack_from('b', header, offset=3589)[0]
        #  ORDER of calibration POLYNOM
        ypolynom['order'] = _struct.unpack_from('b', header, offset=3590)[0]
        #  valid calibration data pairs
        calib_y['count'] = _struct.unpack_from('b', header, offset=3591)[0]
        #  pixel pos. of calibration data
        calib_y['pixel_pos'] = _struct.unpack_from('10d', header, offset=3592)
        #  calibration VALUE at above pos
        calib_y['value'] = _struct.unpack_from('10d', header, offset=3672)
        #  polynom COEFFICIENTS
        ypolynom['coefficients'] = _struct.unpack_from('6d', header, offset=3752)
        #  laser wavenumber for relativ WN
        calib_y['laser_pos'] = _struct.unpack_from('d', header, offset=3800)[0]
        #  reserved
        #  calib_y['reserved3'] = _struct.unpack_from('b', header, offset=3808)[0]
        #  If set to 200, valid label below
        calib_y['label_valid'] = _struct.unpack_from('B', header, offset=3809)[0]==200
        #  Calibration label (NULL term'd)
        calib_y['label'] = _ver.tostr(_struct.unpack_from('81s', header, offset=3810)[0]).rstrip('\x00')
        #  Calibration Expansion area
        calib_y['expansion'] = _ver.tostr(_struct.unpack_from('87s', header, offset=3891)[0]).rstrip('\x00')

        #  special Intensity scaling string
        # calib['int_string'] = _ver.tostr(_struct.unpack_from('40s', header, offset=3978)[0]).rstrip('\x00')
        #  empty block to reach 4100 bytes
        #  self.parlog['Spare_6'] = _ver.tostr(_struct.unpack_from('76s', header, offset=4018)[0]).strip('\x00')
        #  avalanche gain was used
        avalanche['enabled'] = _struct.unpack_from('H', header, offset=4094)[0]>0
        #  avalanche gain value
        avalanche['gain'] = _struct.unpack_from('h', header, offset=4096)[0]




        data_type = data['type']
        # Determine the data type format string for
        # upcoming _struct.unpack_from() calls
        if data_type == 0:  # float (4 bytes) untested
            dataTypeStr = "f"
            bytesPerPixel = 4
            dtype = "float32"
        elif data_type == 1:  # long (4 bytes) untested
            dataTypeStr = "l"
            bytesPerPixel = 4
            dtype = "int32"
        elif data_type == 2:  # short (2 bytes) untested
            dataTypeStr = "h"
            bytesPerPixel = 2
            dtype = "int16"
        elif data_type == 3:  # unsigned short (2 bytes)
            dataTypeStr = "H"  # 16 bits in python on intel mac
            bytesPerPixel = 2
            dtype = "uint16"
            # other options include:
            # IntN, UintN, where N = 8,16,32 or 64
            # and Float32, Float64, Complex64, Complex128
            # but need to verify that pyfits._ImageBaseHDU.ImgCode cna handle it
            # right now, ImgCode must be float32, float64, int16, int32, int64 or uint8
        else:
            raise Exception("unknown data type")

        # Number of pixels on x-axis and y-axis
        nx = data_type = data['x_dimension']
        ny = data_type = data['y_dimension']

        # Number of image frames in this SPE file
        nframes = data['frames']
        npixels = nx*ny
        fmtStr  = "="+str(npixels)+dataTypeStr

        # How many bytes per image?
        nbytesPerFrame = npixels*bytesPerPixel

        # Create a dictionary that holds some header information
        # and contains a placeholder for the image data
        self.data = []
        # Now read in the image data
        # Loop over each image frame in the image
        for ii in range(nframes):
            data = spe.read(nbytesPerFrame)

            # read pixel values into a 1-D _np array. the "=" forces it to use
            # standard python datatype size (4bytes for 'l') rather than native
            # (which on 64bit is 8bytes for 'l', for example).
            # See http://docs.python.org/library/struct.html
            dataArr = _np.array(_struct.unpack_from(fmtStr, data, offset=0), dtype=dtype)

            # Resize array to nx by ny pixels
            # notice order... (y,x)
            dataArr.resize((ny, nx))

            # Push this image frame data onto the end of the list of images
            self.data.append( dataArr )
