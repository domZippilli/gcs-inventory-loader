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

import json
import logging

from threading import Lock
from typing import Iterable

from google.api_core.exceptions import BadRequest

from gcs_inventory_loader.bq.client import get_bq_client
from gcs_inventory_loader.bq.tables import Table
from gcs_inventory_loader.config import get_config

LOG = logging.getLogger(__name__)


class BigQueryOutput():
    """
    A queue-like output stream to a BigQuery table.
    """

    def __init__(self, table: Table, create_table: bool = True):
        self.config = get_config()
        self.lock = Lock()
        self.rows = list()
        self.tablename = table.get_fully_qualified_name()
        self.batch_size = int(
            self.config.get('BIGQUERY', 'BATCH_WRITE_SIZE', fallback=100))
        self.insert_count = 0
        self.insert_bytes = 0
        if create_table:
            table.initialize()

    def put(self, row) -> None:
        """
        Enqueue a message for streaming to BigQuery. Function usually places
        the row in memory, but may block until the current buffer of rows is
        transmitted.

        Raises:
            error: Errors raised by bigquery.client.insert_rows_json

        Arguments:
            row {string} -- A JSON string representing row data.

        Returns:
            None
        """
        self.rows.append(row)
        if len(self.rows) >= self.batch_size and self.lock.acquire(False):
            self.flush()
            self.lock.release()

    def flush(self) -> None:
        """
        Flush all enqueued rows to BigQuery.

        Raises:
            error: Errors raised by bigquery.client.insert_rows_json

        Returns:
            None
        """
        if self.rows:
            if LOG.level <= logging.DEBUG:
                sending_bytes = sum([len(json.dumps(x)) for x in self.rows])
                LOG.debug("Flushing %s rows to %s, %s bytes.", len(self.rows),
                          self.tablename, sending_bytes)
            client = get_bq_client()
            try:
                insert_errors = client.insert_rows_json(
                    self.tablename, self.rows)
                if insert_errors:
                    LOG.error("Insert errors! %s",
                              [x for x in flatten(insert_errors)])
            except BadRequest as error:
                if not error.message.endswith(
                        "No rows present in the request."):
                    LOG.error("Insert error! %s", error.message)
                    raise error
            finally:
                self.insert_count += len(self.rows)
                self.rows = list()

    def stats(self) -> str:
        """
        Produce a string describing statistics about this BigQueryOutput,
        including the number of rows inserted into which table.

        Returns:
            str -- [description]
        """
        return "{} rows inserted into {}. {} rows in queue.".format(
            self.insert_count, self.tablename, len(self.rows))


def flatten(iterable, iter_types=(list, tuple)) -> Iterable:
    """
    Flattens nested iterables into a flat iterable.

    Arguments:
        iterable {iterable} -- An iterable, which may contain iterable
        elements.

    Keyword Arguments:
        iter_types {tuple} -- The types to recognize as iterables and recurse
        upon. (default: {(list, tuple)})

    Returns:
        iterable -- A one dimensional iterable with iter_types in the source
        flattened into the top level.
    """
    for i in iterable:
        if isinstance(i, iter_types):
            for j in flatten(i, iter_types):
                yield j
        else:
            yield i
