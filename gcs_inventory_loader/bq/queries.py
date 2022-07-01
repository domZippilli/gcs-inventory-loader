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
Definitions of BigQuery queries used by this program.
"""
import logging

from google.cloud.bigquery.job import QueryJob, QueryJobConfig, WriteDisposition  # noqa: E501

from gcs_inventory_loader.bq.client import get_bq_client
from gcs_inventory_loader.bq.tables import get_table, TableDefinitions, Table
from gcs_inventory_loader.config import get_config

LOG = logging.getLogger(__name__)


def run_query_job(
    querytext: str,
    temp_table: str = None,
    query_job_config: QueryJobConfig = QueryJobConfig()
) -> QueryJob:
    """
    Set up and run a query job.

    Arguments:
        querytext {str} -- The querytext for the job.

    Keyword Arguments:
        temp_table {str} -- A temporary table in which to materialize results.
        The results will be streamed from this table when done. This is
        required for all large queries, and strongly recommended.
        (default: {None})

        query_job_config {QueryJobConfig} -- A QueryJobConfig to start from.
        (default: {QueryJobConfig()})

    Returns:
        QueryJob -- The resulting job.
    """
    LOG.debug("Running query: %s", querytext)
    client = get_bq_client()
    if temp_table:
        query_job_config.destination = temp_table
        query_job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
    return client.query(query=querytext, job_config=query_job_config)


def _compose_catch_up_union() -> str:
    """
    Compose a UNION ALL statement and secondary query to extend the
    access log with items that predate access logging, if the
    configuration contains BIGQUERY.CATCHUP_TABLE. Otherwise, return an
    empty string, having no effect on any composed queries.

    The REGEXP_REPLACE function serves to format the object URL the same
    way the audit log resourceName is formatted.

    Returns:
        str -- The UNION ALL statement, or empty string.
    """
    config = get_config()
    catchup_table_name = config.get("BIGQUERY", "CATCHUP_TABLE", fallback=None)
    if catchup_table_name:
        catchup_table = Table(catchup_table_name)
        return """
            UNION ALL
            SELECT
                REGEXP_REPLACE(url,"gs://(.*)/(.*)","projects/_/buckets/{0}1/objects/{0}2") AS resourceName,
                created AS timestamp
            FROM `{1}`
        """.format("\\\\", catchup_table.get_fully_qualified_name())
    return ""


def _calculate_day_partitions() -> int:
    """Calculate the daily partitions to query. This is the sum of how far
    you need to look back (COLD_THRESHOLD_DAYS) and how often you look
    (DAYS_BETWEEN_RUNS).

    Returns:
        int -- The sum of cold threshold days and days between runs.
    """
    config = get_config()
    return config.getint('RULES', 'COLD_THRESHOLD_DAYS') + config.getint(
        'RULES', 'DAYS_BETWEEN_RUNS')


def _get_cold_threshold_days() -> int:
    """Retrieve the warm threshold days from the configuration.

    Returns:
        int -- Warm threshold days.
    """
    config = get_config()
    return config.getint('RULES', 'COLD_THRESHOLD_DAYS')


def _get_warm_threshold_days() -> int:
    """Retrieve the warm threshold days from the configuration.

    Returns:
        int -- Warm threshold days.
    """
    config = get_config()
    return config.getint('RULES', 'WARM_THRESHOLD_DAYS')


def _get_warm_threshold_accesses() -> int:
    """Retrieve the warm threshold accesses from the configuration.

    Returns:
        int -- Warm threshold accesses.
    """
    config = get_config()
    return config.getint('RULES', 'WARM_THRESHOLD_ACCESSES')


def compose_access_query() -> str:
    """Compose the query to get access information for all objects.

    Returns:
        str -- The query text.
    """
    access_log = get_table(TableDefinitions.DATA_ACCESS_LOGS)
    moved_objects = get_table(TableDefinitions.OBJECTS_MOVED)
    excluded_objects = get_table(TableDefinitions.OBJECTS_EXCLUDED)

    # First, find the most recent move for each object. Then, join the full
    # move records on that aggregated data to find full move info for the most
    # recent move.
    # TODO: Eliminate the JOIN?
    most_recent_moves = """
        SELECT full_move_info.*
            FROM `{0}` AS full_move_info
        INNER JOIN (
            SELECT
                resourceName,
                MAX(moveTimestamp) as timestamp
            FROM `{0}`
            GROUP BY resourceName
        ) AS most_recent
        ON most_recent.timestamp       = full_move_info.moveTimestamp
        AND most_recent.resourceName   = full_move_info.resourceName
    """.format(moved_objects.get_fully_qualified_name())

    # Perform a bounded query of n days of access logs, possibly with a UNION
    # of a catch-up table. REGEXP_REPLACE is to unify the representation of
    # resourceName between create and get events, which differ slightly.
    raw_access_records = """
    SELECT
        REGEXP_REPLACE(protopayload_auditlog.resourceName, "gs://.*/", "") AS resourceName,
        timestamp
    FROM `{0}`
    WHERE
        _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {1} DAY))
        AND FORMAT_DATE("%Y%m%d", CURRENT_DATE())
    {2}
    """.format(access_log.get_fully_qualified_name(),
               _calculate_day_partitions(), _compose_catch_up_union())

    # Aggregate the raw access records, in order to calculate most
    # recent access (coldness) as well as the count of accesses within a
    # specified period (hotness).
    aggregated_access_records = """
    SELECT
        resourceName,
        MAX(timestamp) AS lastAccess,
        COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, DAY) <= {0}) AS recent_access_count
    FROM raw_access_records
    GROUP BY resourceName
    """.format(_get_warm_threshold_days())

    # Final query text. Joins most_recent_moves in order to determine
    # the latest known storage class (avoiding a GET per object to find this
    # out from GCS), and joins excluded_objects to remove them from the results.
    querytext = """
    WITH most_recent_moves AS ({0}), raw_access_records AS ({1}), aggregated_access_records AS ({2})

    SELECT
        access_records.resourceName,
        most_recent_moves.storageClass,
        access_records.lastAccess,
        access_records.recent_access_count
    FROM aggregated_access_records as access_records

    LEFT JOIN most_recent_moves ON access_records.resourceName = most_recent_moves.resourceName

    LEFT JOIN `{3}` as excluded ON access_records.resourceName = excluded.resourceName
    WHERE excluded.resourceName IS NULL
    """.format(most_recent_moves, raw_access_records,
               aggregated_access_records,
               excluded_objects.get_fully_qualified_name())

    return querytext


def compose_warmup_query() -> str:
    """
    Compose a query to get only objects that are warm-up candidates.

    This is a slight modification of compose_access_query, as all the same
    analysis must be done to compute these values, this simply filters the
    results to focus the work on warmup candidates.
    """
    return compose_access_query() + """
        AND recent_access_count >= {}
    """.format(_get_warm_threshold_accesses())


def compose_cooldown_query() -> str:
    """
    Compose a query to get only objects that are cool-down candidates.

    This is a slight modification of compose_access_query, as all the same
    analysis must be done to compute these values, this simply filters the
    results to focus the work on warmup candidates.
    """
    return compose_access_query() + """
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), lastAccess, DAY) >= {}
    """.format(_get_cold_threshold_days())
