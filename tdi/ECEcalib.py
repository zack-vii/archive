def ECEcalib(node, _unit=None, _freq=None, _calib=None):
    from archive import calibrations
    from MDSplus import Ident, String, Float32, StringArray
    sig = node.evaluate()
    ecechannel = int(node.getNode('$IDX').data())+1
    offset,factor,unit,info = calibrations.ECEcalib(sig,ecechannel)
    # construct output
    if isinstance(_unit,Ident):
        String(unit).setTdiVar(_unit.name)
    if isinstance(_freq,Ident):
        Float32(info['freq']).setUnits('GHz').setTdiVar(_freq.name)
    if isinstance(_calib,Ident):
        StringArray([info['cfg'],info['cal'],info['par']]).setTdiVar(_calib.name)
    return [offset,factor]
