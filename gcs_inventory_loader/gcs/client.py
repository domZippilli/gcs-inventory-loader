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
Module containing GCS code for this program.
"""

import logging
from threading import Lock

from google.cloud import storage

from gcs_inventory_loader.config import get_config

LOG = logging.getLogger(__name__)


class GCSClientPool():
    """
    A round-robin GCS client pool.
    """

    def __init__(self, size=32):
        self.clients = []
        self.pool_size = size
        self.next_up = 0
        self.lock = Lock()

    def get_client(self) -> storage.client:
        """Get a client from the pool. Automatically makes new ones until
        the pool is full. Threadsafe.

        Returns:
            storage.client -- A configured GCS client.
        """
        config = get_config()
        self.lock.acquire()
        if len(self.clients) < self.pool_size:
            LOG.debug("Making new GCS client.")
            self.clients.append(
                storage.Client(
                    config.get('GCP',
                               'GCS_PROJECT',
                               fallback=config.get('GCP', 'PROJECT'))))
        client = self.clients[self.next_up]
        self.next_up += 1
        if self.next_up >= self.pool_size - 1:
            self.next_up = 0
        self.lock.release()
        return client


CLIENTS = GCSClientPool()


def get_gcs_client() -> storage.Client:
    """
    Get a GCS client. This function presently returns the same client
    for every invocation.

    Returns:
        google.cloud.storage.Client -- A GCS client.
    """
    return CLIENTS.get_client()
