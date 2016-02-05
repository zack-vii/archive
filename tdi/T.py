"""
helper fuction to read triggern times based on MDSplus shot numbers
T() retruns T1 from the current shot=0
T(N) retruns TN from the current shot=0
T(N,SHOT) retruns T1 from shot=SHOT
"""
from MDSplus import TdiExecute
def T(N=1,SHOT=0):
    TdiExecute('TreeOpen($,$)',('W7X',int(SHOT)))
    return TdiExecute('DATA(TIMING:T%d:IDEAL)' % int(N))
