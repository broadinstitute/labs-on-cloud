#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

requirements = [
    'jira>=1.0.15',
    'python-dateutil>=2.7.3'
]

setup_requirements = [
    # put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'unittest'
]

setup(
    name='loc',
    version='0.1.0',
    description="Labs On Clouds",
    long_description="Labs On Clouds",
    author="Asma Bankapur, Joshua Gould",
    url='https://github.com/broadinstitute/labs-on-cloud',
    packages=find_packages(include=['loc']),
    include_package_data=True,
    install_requires=requirements,
    license="BSD license",
    zip_safe=False,
    keywords='loc',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Natural Language :: English',
        'Topic :: Scientific/Engineering :: Bio-Informatics'
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
    python_requires='~=3.1',
    entry_points={}
)
