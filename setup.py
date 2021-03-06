#!/usr/bin/env python

# Remove .egg-info directory if it exists, to avoid dependency problems with
# partially-installed packages (20160119/dphiffer)

import os
import sys
import shutil

setup = os.path.abspath(sys.argv[0])
parent = os.path.dirname(setup)
pkg = os.path.basename(parent)

if pkg.startswith("py-mapzen"):
    pkg = pkg.replace("py-", "")
    pkg = pkg.replace("-", ".")

    egg_info = "%s.egg-info" % pkg
    egg_info = os.path.join(parent, egg_info)

    if os.path.exists(egg_info):
        shutil.rmtree(egg_info)

from setuptools import setup, find_packages

packages = find_packages()
desc = open("README.md").read()
version = open("VERSION").read()

setup(
    name='mapzen.whosonfirst.brands',
    namespace_packages=[ 'mapzen', 'mapzen.whosonfirst' ],
    version=version,
    description='A package for working with Who\'s On First Python brands',
    author='Mapzen',
    url='https://github.com/whosonfirst/py-mapzen-whosonfirst',
    packages=packages,
    scripts=[
        'scripts/brands-es-index',
        'scripts/brands-update-venues'
        ],
    download_url='https://github.com/whosonfirst/py-mapzen-whosonfirst-brands/releases/tag/' + version,
    license='BSD')
