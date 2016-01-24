import os
def getShotDB(expt,i=None):
    expt = str(expt)
    def getshots(expt_path):
        files = [f[len(expt):-5].split('_') for f in os.listdir(expt_path) if f.endswith('.tree') and f.startswith(expt+'_')]
        return [int(f[1]) for f in files if len(f)==2 and f[1]!='model']
    expt_paths = os.getenv(expt+'_path').replace('~t','w7x').split(';')
    if i is not None:
        shots = getshots(expt_paths[int(i)])
    else:
        shots = []
        for expt_path in expt_paths:
            shots += getshots(expt_path)
    shots.sort()
    return shots
