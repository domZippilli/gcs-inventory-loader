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
Definitions of BigQuery tables used by this program.
"""
import logging

from enum import Enum

from google.cloud import bigquery

from gcs_inventory_loader.bq.client import get_bq_client
from gcs_inventory_loader.config import get_config

LOG = logging.getLogger(__name__)


class Table:
    """
    BigQuery table information and common methods.
    """

    def __init__(self, name: str, schema: str = None):
        self.short_name = name
        self.schema = schema

    def drop(self) -> bigquery.table.RowIterator:
        """DROPs (deletes) the table. This cannot be undone.

        Returns:
            google.cloud.bigquery.table.RowIterator -- Result of the query.
            Since this is a DDL query, this will always be empty if
            it succeeded.

        Raises:
            google.cloud.exceptions.GoogleCloudError –- If the job failed.
            concurrent.futures.TimeoutError –- If the job did not complete
            in the default BigQuery job timeout.
        """
        bq_client = get_bq_client()

        LOG.info("Deleting table %s", self.get_fully_qualified_name())

        querytext = "DROP TABLE `{}`".format(self.get_fully_qualified_name())

        LOG.debug("Running query: \n%s", querytext)

        query_job = bq_client.query(querytext)
        return query_job.result()

    def initialize(self) -> bigquery.table.RowIterator:
        """Creates, if not found, a table.

        Returns:
            google.cloud.bigquery.table.RowIterator -- Result of the query.
            Since this is a DDL query, this will always be empty if
            it succeeded.

        Raises:
            google.cloud.exceptions.GoogleCloudError –- If the job failed.
            concurrent.futures.TimeoutError –- If the job did not complete
            in the default BigQuery job timeout.
        """
        if not self.schema:
            raise ValueError(
                "No schema provided for table {}; writing is not supported.".
                format(self.short_name))

        bq_client = get_bq_client()

        LOG.info("Creating table %s if not found.",
                 self.get_fully_qualified_name())

        querytext = """
            CREATE TABLE IF NOT EXISTS `{}` (
            {}
            )""".format(self.get_fully_qualified_name(), self.schema)

        LOG.debug("Running query: \n%s", querytext)

        query_job = bq_client.query(querytext)
        return query_job.result()

    def get_fully_qualified_name(self) -> str:
        """Return a table name with project and dataset names prefixed.

        Arguments:
            name {str} -- Short name of the table.

        Returns:
            str -- Fully qualified name of the table.
        """
        config = get_config()
        return "{}.{}.{}".format(
            config.get("BIGQUERY",
                       "JOB_PROJECT",
                       fallback=config.get("GCP", "PROJECT")),
            config.get("BIGQUERY", "DATASET_NAME"), self.short_name)


class TableDefinitions(Enum):
    """
    Definitions of known tables.

    Where tables have schema = None, they are presumed to be read-only.
    """
    INVENTORY = {
        "schema":
        """
                acl ARRAY<STRUCT<kind STRING, object STRING, generation INT64, id STRING, selfLink STRING, bucket STRING, entity STRING, entityId STRING, role STRING, email STRING, domain STRING, etag STRING, projectTeam STRUCT<projectNumber STRING, team STRING>>>,
                bucket STRING,
                cacheControl STRING,
                componentCount INT64,
                contentDisposition STRING,
                contentEncoding STRING,
                contentLanguage STRING,
                contentType STRING,
                crc32c STRING,
                customerEncryption STRUCT<encryptionAlgorithm STRING, keySha256 STRING>,
                customTime STRING,
                etag STRING,
                eventBasedHold BOOL,
                generation INT64,
                id STRING,
                kind STRING,
                kmsKeyName STRING,
                md5Hash STRING,
                mediaLink STRING,
                metadata ARRAY<STRUCT<key STRING, value STRING>>,
                metageneration INT64,
                name STRING,
                owner STRUCT<entity STRING, entityId STRING>,
                retentionExpirationTime TIMESTAMP,
                selfLink STRING,
                size INT64,
                storageClass STRING,
                temporaryHold BOOL,
                timeCreated TIMESTAMP,
                timeDeleted TIMESTAMP,
                timeStorageClassUpdated TIMESTAMP,
                updated TIMESTAMP
            """  # noqa: E501
    }


def get_table(table: TableDefinitions, name: str = None) -> Table:
    """    Get a Table object using one of the TableDefinitions enum
    definitions.

    Arguments:
        table {TableDefinitions} -- Enum name of the table.

    Keyword Arguments:
        name {str} -- A name for the table, overriding the name set in
        the enum value. (default: {None})

    Returns:
        Table -- The table object representing the table.
    """
    kwargs = table.value
    if name:
        kwargs["name"] = name
    return Table(**kwargs)
