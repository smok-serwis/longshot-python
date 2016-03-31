#!/usr/bin/env python

from distutils.core import setup

setup(name='longshot',
      version='0.1alpha',
      description='SMOK client connectivity library',
      author='smok-serwis.pl',
      author_email='admin@smok.co',
      url='https://github.com/smok-serwis/longshot-python',
      packages=['longshot', 'longshot.persistence'],
     )