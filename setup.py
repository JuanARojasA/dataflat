'''
setup.py - a setup script

Copyright (C) 2023 Juan ROJAS
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Authors:
    Juan ROJAS <jarojasa97@gmail.com>
'''

import setuptools
from codecs import open
from os import path

PATH = path.abspath(path.dirname(__file__))

with open(path.join(PATH, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
 
setuptools.setup(
    name="dataflat",
    version="1.0.3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/JuanARojasA/dataflat',
    author="Juan Rojas",
    author_email="jarojasa97@gmail.com",
    license='Apache License 2.0',
    platforms='any',
    packages=setuptools.find_packages(),
    classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'Intended Audience :: Information Technology',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python :: 3',
            'Topic :: Scientific/Engineering',
            'Topic :: Software Development :: Libraries',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Topic :: Software Development :: Pre-processors'
        ],
    python_requires='>=3.9',
    install_requires=["pandas","typeguard"],
    include_package_data=True
)