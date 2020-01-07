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
"""
Module containing some GCS utility functions.
"""

# This is a mapping of storage classes understood by this program to storage
# classes which may be encountered when describing blobs. In general,
# the blob API will use more verbose names, or a handful of legacy names.
# See https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.storage_class
# for more information about the storage class names used by
# Blob.storage_class.
# The storage class names used by this program are also included in the mapping,
# so this can be used with storage class descriptions from other sources, like
# the catchup table.
STORAGE_CLASS_MAPPING = {
    'STANDARD': [
        'STANDARD', 'STANDARD_STORAGE_CLASS', 'REGIONAL_LEGACY_STORAGE_CLASS',
        'MULTI_REGIONAL_LEGACY_STORAGE_CLASS',
        'DURABLE_REDUCED_AVAILABILITY_STORAGE_CLASS'
    ],
    'NEARLINE': ['NEARLINE', 'NEARLINE_STORAGE_CLASS'],
    'COLDLINE': ['COLDLINE', 'COLDLINE_STORAGE_CLASS']
}


def check_redundant_rewrite(destination_class: str,
                            origination_class: str) -> bool:
    """Check whether a requested rewrite is redundant.
    
    Arguments:
        destination_class {str} -- The destination storage class, set in the program configuration by the user.
        origination_class {str} -- The origination storage class, given by a blob description.
    
    Returns:
        bool -- True if the write is redundant.
    """
    destination_class = destination_class.upper()
    origination_class = origination_class.upper()
    return origination_class in STORAGE_CLASS_MAPPING[destination_class]