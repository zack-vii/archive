def ECEcalib(sig, ecechannel):
    from archive import Time
    from os import listdir, sys
    from json import load
    # paths to configuration files
    path_win = '//x-drive/Diagnostic-logbooks/QME-ECE'
    path_lx  = '/smb/QME-ECE'
    conf_dir = 'Config-files'
    cali_dir = 'Calibration-files'
    para_dir = 'Parameter-files'
    # mapp_dir = 'Mapping-files'
    # zoom_dir = 'Zoom-Parameter-files'
    path_root = path_win if sys.platform=='win32' else path_lx

    def pickECEfile( utcin, search_dir ):
        def parseECEfilname(filname):
            from datetime import datetime
            datestr = filname.split('_')[1]
            dtobject = datetime.strptime( datestr,'%y%m%d-%H%M')
            return Time(datetime.strftime( dtobject, '%Y-%m-%d %H:%M:%S:%f'))
        dirs = listdir(path_root+'/'+search_dir)
        filenames = []
        for filen in dirs:
            if filen.startswith('QME-'):
                tfil = parseECEfilname(filen)
                if tfil<utcin:
                    filenames.append(search_dir+'/'+filen)
        filenames.sort()
        return filenames[-1]

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
    timestamp = Time(sig.args[2][0][0])
    # find the files that apply to the time of interest
    conf_file = pickECEfile(timestamp, conf_dir)
    if conf_file is None: return
    cali_file = pickECEfile(timestamp, cali_dir)
    if cali_file is None: return
    para_file = pickECEfile(timestamp, para_dir)
    if para_file is None: return None,None,None,{}
    # get frequencies and bittovolt
    with open(path_root+'/'+conf_file) as f: cfg = load(f)
    freqGHz     = float(cfg['Data']['Channel%02d'%ecechannel]['Frequency'])
    conf_factor = float(cfg['Data']['Channel%02d'%ecechannel]['Bit_To_Volt'])
    # get factor
    with open(path_root+'/'+cali_file) as f: cal = load(f)
    cali_factor = float(cal['Data']['Channel%02d'%ecechannel]['Factor'])
    # get attenuation factors (in dB) and Amplification
    with open(path_root+'/'+para_file) as f: par = load(f)
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
