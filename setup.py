import os
import re
from setuptools import find_packages

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = '1.0.0'


setup(
    name='FALCON-pbsmrtpipe',
    version=version,
    package_dir={'': '.'},
    packages=find_packages(),
    license='See LICENSE file.',
    author='PacBio',
    author_email='cdunn@pacificbiosciences.com',
    description='FALCON using pbsmrtpipe.',
    #setup_requires=['nose>=1.0'],
    # Maybe the pbtools-* should really be done in a subparser style
    entry_points={'console_scripts': [
        'pb-falcon = pbfalcon.run:main',
        'pb-ini2xml = pbfalcon.ini2xml:main',
    ]},
    install_requires=[
        'falcon_kit',
        'falcon_polish',
        'pbcommand',
    ],
    tests_require=['nose'],
    long_description='pbsmrtpipe adapter for Falcon',
    #classifiers=['Development Status :: 4 - Beta'],
    include_package_data=True,
    zip_safe=False,
    #package_data={'pbfalcon': ['reg-tcs/*.json', ]},
)
