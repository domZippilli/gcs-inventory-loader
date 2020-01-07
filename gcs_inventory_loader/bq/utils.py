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
Various utility functions related to BigQuery queries and results.
"""

from typing import Tuple

def get_bucket_and_object(resource_name: str) -> Tuple[str, str]:
    """
    Given an audit log resourceName, parse out the bucket name and object
    path within the bucket.

    Arguments:
        resource_name {str} -- The resourceName.

    Returns:
        Tuple[str, str] -- Bucket, object name.
    """
    pathparts = resource_name.split("buckets/", 1)[1].split("/", 1)

    bucket_name = pathparts[0]
    object_name = pathparts[1].split("objects/", 1)[1]

    if object_name.endswith("/"):
        # can happen when catch up table has been populated naively
        object_name = None

    return (bucket_name, object_name)
