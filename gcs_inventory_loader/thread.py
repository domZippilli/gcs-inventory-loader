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
Custom threading code.
"""

from concurrent.futures import ThreadPoolExecutor
from queue import Queue


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    """A wrapper around concurrent.futures.thread.py to add a bounded
    queue to ThreadPoolExecutor.
    """

    def __init__(self, *args, queue_size: int = 1000, **kwargs):
        """Construct a slightly modified ThreadPoolExecutor with a
        bounded queue for work. Causes submit() to block when full.

        Arguments:
            ThreadPoolExecutor {[type]} -- [description]
        """
        super().__init__(*args, **kwargs)
        self._work_queue = Queue(queue_size)
