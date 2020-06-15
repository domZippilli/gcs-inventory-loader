# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Setup module for GCS Inventory Loader.
"""

from os import path
from setuptools import setup, find_packages

PWD = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(PWD, 'README.md'), encoding='utf-8') as f:
    README = f.read()

setup(
    name='gcs_inventory_loader',
    version='0.1.0',
    description='Quickly load your GCS bucket inventory into BigQuery.',
    long_description=README,
    long_description_content_type='text/markdown',
    url='https://cloud.google.com',  # TODO: final repo URL
    author='Google, LLC',
    author_email='domz@google.com',  # TODO: group
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='google cloud storage nearline coldline archive',
    packages=find_packages(), exclude=["tests"],
    python_requires='>=3.6, <4',
    install_requires=[
        'google-cloud-bigquery',
        'google-cloud-storage',
        'google-cloud-pubsub',
        'click',
    ],
    entry_points={
        'console_scripts': [
            'gcs_inventory = gcs_inventory_loader:main',
        ],
    },
    project_urls={},  # TODO: Reference guide
)
