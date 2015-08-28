#!/usr/bin/env python

from setuptools import setup#, Extension
version='0.1'
setup(name='codac',
     version=version,
     description='WebAPI Device support',
     long_description = """
     This module provides the WebAPI device support classes
     """,
     author='Timo Schroeder',
     author_email='timo.schroeder@ipp-hgw.mpg.de',
     url='http://archive-webapi.ipp-hgw.mpg.de',
     package_dir = {'codac':'.',},
     packages = ['codac',],
     platforms = ('Any',),
     classifiers = [ 'Development Status :: 4 - Beta',
     'Programming Language :: Python',
     'Intended Audience :: Science/Research',
     'Environment :: Console',
     'Topic :: Scientific/Engineering',
     ],
     keywords = ('physics','mdsplus','archive-webapi','codac'),
     zip_safe = False,
    )
