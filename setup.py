#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

from setuptools import setup
from pathlib import Path


requirements_path = Path('requirements.txt').resolve()
if requirements_path.is_file():
    with open(
        requirements_path,
        errors='ignore',
        encoding='utf-8',
        buffering=1,
        mode='r',
    ) as fp:
        requirements = fp.read().splitlines()
        fp.close()
else:
    print('No requirements.txt found. Skipping.')
    # Empty requirements list to avoid error
    requirements = []

setup(
    name='peloptimize',
    use_scm_version={
        'local_scheme': 'dirty-tag',
        'write_to': 'src/pelopt/_version.py',
        'fallback_version': '0.01',
    },
    version='0.01',
    description="Criação de modelos de Machine Learning para predição de "
                "custos relacionados ao processo de pelotização da Vale S/A e "
                "otimização de custos através da aplicação de um modelo de "
                "otimização baseado nos outputs dos algoritmos preditivos.",
    license="Proprietary",
    author="Erik Ingwersen",
    author_email='erik.ingwersen@br.ey.com',
    url='https://github.com/ingwersen-erik/peloptimize',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    package_dir={'': 'src'},
    packages=['pelopt'],
    entry_points={
        'console_scripts': [
            'pelopt=pelopt.cli:main'
        ]
    },
    install_requires=requirements,
    keywords='peloptimize',
)
