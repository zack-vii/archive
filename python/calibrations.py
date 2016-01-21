def ECEcalib(sig, ecechannel, timestamp, freq=None, calibdata=None):
    def findSuitableFile(tstamp, dirname):
        from archive import Time
        from os import listdir
        from json import load
        # selects the json file that is valid for the time of interest
        jsonfiles =  [f for f in listdir(dirname) if f.endswith('.json')]
        jsonfiles.reverse()  # newest first
        valid = False
        # read valid_since fields
        for i,filename in enumerate(jsonfiles):
            fullpath = dirname+'\\'+filename
            with open(fullpath) as f:
                out = load(f)
            try:
                valid = Time(out['Valid_Since']).ns<tstamp
            except:
                pass
            if valid: break
        # get the latest version that is anterior to the time of interest
        if valid:
            return fullpath,out
        return None,{}

    def getOffset(sig):
        dim = sig.dim_of()
        time = dim.data()
        data = sig.data()
        value = 0
        if   dim.getUnits()=='ns': tlim = time[0]+5e6
        elif dim.getUnits()=='s':  tlim = time[0]+5e-3
        else:
            for i in range(50): value+= data[i]
            return value/50
        for i,t in enumerate(time):
            if t>tlim: break  # this way i is number of accumulated samples
            value+= data[i]
        return value/i

    # paths to configuration files
    path_conf = '\\\\x-drive\\Diagnostic-logbooks\\QME-ECE\\Config-files'
    path_cali = '\\\\x-drive\\Diagnostic-logbooks\\QME-ECE\\Calibration-files'
    path_para = '\\\\x-drive\\Diagnostic-logbooks\\QME-ECE\\Parameter-files'
    # find the files that apply to the time of interest
    conf_file,cfg = findSuitableFile(timestamp, path_conf)
    if conf_file is None: return
    cali_file,cal = findSuitableFile(timestamp, path_cali)
    if cali_file is None: return
    para_file,par = findSuitableFile(timestamp, path_para)
    if para_file is None: return
    # get frequencies and bittovolt
    freqGHz     = float(cfg['Data']['Channel%02d'%ecechannel]['Frequency'])
    conf_factor = float(cfg['Data']['Channel%02d'%ecechannel]['Bit_To_Volt'])
    # get factor
    cali_factor = float(cal['Data']['Channel%02d'%ecechannel]['Factor'])
    # get attenuation factors (in dB) and Amplification
    qme30 = float(par['QME30_Attenuator'])
    qme32 = float(par['QME32_Attenuator'])
    qme33 = float(par['QME33_Attenuator'])
    para_factor = 10**(qme30/10);
    if ecechannel < 17:
        para_factor = para_factor*10**(qme32/10)
    else:
        para_factor = para_factor*10**(qme33/10)
    # amplification
    amplification = float(par['Data']['Channel%02d'%ecechannel]['Amplification'])
    para_factor = para_factor/amplification;
    offset = getOffset(sig)
    factor = conf_factor*cali_factor*para_factor
    # construct output
    out = {'freq':freqGHz,
    'Cfiles':{'configuration':conf_file,'calibration':cali_file,'parameter':para_file}}
    return [offset,factor],out
