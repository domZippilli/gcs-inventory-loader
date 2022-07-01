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
Module containing BigQuery code for this program.
"""

import logging

from google.cloud import bigquery

from gcs_inventory_loader.config import get_config

LOG = logging.getLogger(__name__)


class SingleBQClient():
    """
    A cached single BQ client.
    """

    def __init__(self):
        self.client = None

    def get_client(self) -> bigquery.client:
        """Get a client.

        Returns:
            storage.client -- A configured BQ client.
        """
        if not self.client:
            LOG.debug("Making new BQ client.")
            config = get_config()
            self.client = bigquery.Client(
                project=config.get('BIGQUERY',
                                   'JOB_PROJECT',
                                   fallback=config.get('GCP', 'PROJECT')))
        return self.client


CLIENTS = SingleBQClient()


def get_bq_client() -> bigquery.client:
    """
    Get a BQ client. This function presently returns the same client
    for every invocation.

    Returns:
        google.cloud.storage.Client -- A BQ client.
    """
    return CLIENTS.get_client()
