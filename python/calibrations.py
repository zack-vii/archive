def ECEcalib(sig, ecechannel, freq=None, calibdata=None):
    from archive import Time
    from os import listdir, sys
    # paths to configuration files
    path_win = '//x-drive/Diagnostic-logbooks/QME-ECE'
    path_lx  = '/smb/QME-ECE'
    conf_dir = 'Config-files'
    cali_dir = 'Calibration-files'
    para_dir = 'Parameter-files'
    path_root = path_win if sys.platform=='win32' else path_lx    
        
    def findSuitableFile(tstamp, dirname):
        from json import load
        dirpath = path_root+'/'+dirname
        # selects the json file that is valid for the time of interest
        jsonfiles =  [f for f in listdir(dirpath) if f.endswith('.json')]
        jsonfiles.reverse()  # newest first
        valid = False
        # read valid_since fields
        for i,filename in enumerate(jsonfiles):
            with open(dirpath+'/'+filename) as f:
                out = load(f)
            try:
                valid = Time(out['Valid_Since']).ns<tstamp
            except:
                pass
            if valid: break
        # get the latest version that is anterior to the time of interest
        if valid:
            return dirname+'/'+filename,out
        return None,{}

    def getOffset(sig):
        dim = sig.dim_of()
        time = dim.data()
        data = sig.data()
        value = 0.
        if   dim.getUnits()=='ns': tlim = time[0]+5e6
        elif dim.getUnits()=='s':  tlim = time[0]+5e-3
        else:
            for i in range(50): value+= data[i]
            return value/50
        for i,t in enumerate(time):
            if t>tlim: break  # this way i is number of accumulated samples
            value+= data[i]
        return value/i
    timestamp = Time(sig.args[2][0][2])
    print(timestamp.local)
    # find the files that apply to the time of interest
    conf_file,cfg = findSuitableFile(timestamp, conf_dir)
    if conf_file is None: return
    cali_file,cal = findSuitableFile(timestamp, cali_dir)
    if cali_file is None: return
    para_file,par = findSuitableFile(timestamp, para_dir)
    if para_file is None: return None,None,None,{}
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
    'cfg':path_root+'/'+conf_file,
    'cal':path_root+'/'+cali_file,
    'par':path_root+'/'+para_file}
    return offset,factor,'keV',out
