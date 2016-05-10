"""
archive
==========
@authors: timo.schroeder@ipp-hgw.mpg.de
@copyright: 2015
@license: GNU GPL
"""
__version__ = '2016.05.10.17.17'
from . import version
if version.has_mds:
    import MDSplus as _mds  # analysis:ignore
    from . import mdsupload, transient, winspec, calibrations  # analysis:ignore
    from .archivebuild import build  # analysis:ignore
import numpy as _np  # analysis:ignore
import os as _os  # analysis:ignore
import re as _re  # analysis:ignore
from . import base, interface, support  # analysis:ignore
from . import diff, process  # analysis:ignore
from .base import Time, TimeArray, TimeInterval, Units, Path  # analysis:ignore
from .interface import write_signal, write_signals, write_images, read_signal, read_cfglog, read_parlog  # analysis:ignore
from .classes import datastream, browser  # analysis:ignore
