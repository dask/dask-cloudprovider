#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

import versioneer

setup(
    name="dask-cloudprovider",
    cmdclass=versioneer.get_cmdclass(),
    version=versioneer.get_version(),
    description="Native Cloud Provider integration for Dask",
    url="https://github.com/dask/dask-cloudprovider",
    keywords="dask,cloud,distributed",
    license="BSD",
    packages=find_packages(),
    include_package_data=True,
    long_description=(open("README.md").read() if exists("README.md") else ""),
    zip_safe=False,
    install_requires=list(open("requirements.txt").read().strip().split("\n")),
    entry_points="""
    [console_scripts]
    dask-ecs=dask_cloudprovider.cli.ecs:go
    """,
    python_requires=">=3.5",
)
