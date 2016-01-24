import os
def getShotDB(expt,i=None):
    def getshots(expt_path):
        files = [f[len(expt):-5].split('_') for f in os.listdir(expt_path) if f.endswith('.tree') and f.startswith(expt+'_')]
        return [int(f[1]) for f in files if len(f)==2]
    expt_paths = os.getenv(expt+'_path').replace('~t','w7x').split(';')
    if i is not None:
        return getshots(expt_paths[i]).sort()
    shots = []
    for expt_path in expt_paths:
        shots += getshots(expt_paths[i])
    return shots.sort()
