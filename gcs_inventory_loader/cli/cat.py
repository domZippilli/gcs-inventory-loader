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
Implementation of "catchup" command.
"""

import logging
from configparser import ConfigParser
from time import sleep
from typing import List

from google.cloud.storage import Bucket, Client
from google.api_core.page_iterator import Page
from gcs_inventory_loader.config import get_config
from gcs_inventory_loader.gcs.client import get_gcs_client
from gcs_inventory_loader.thread import BoundedThreadPoolExecutor

LOG = logging.getLogger(__name__)


def cat_command(buckets: List[str] = None, prefix: str = None) -> None:
    """Implementation of the cat command.

    This function dispatches each bucket listed into an executor thread for
    parallel processing of the bucket list.

    Keyword Arguments:
        buckets {[str]} -- A list of buckets to use instead of the
        project-wide bucket listing. (default: {None})
        prefix {str} -- A prefix to use when listing. (default: {None})
    """
    config = get_config()
    gcs = get_gcs_client()

    # if buckets is given, get each bucket object; otherwise, list all bucket
    # objects
    if buckets:
        buckets = [gcs.get_bucket(x) for x in buckets]
    else:
        buckets = [x for x in gcs.list_buckets()]

    total_buckets = len(buckets)
    buckets_listed = 0
    bucket_blob_counts = dict()

    # Use at most 2 workers for this part, as it won't be many
    workers = min(config.getint('RUNTIME', 'WORKERS'), 2)
    size = int(config.getint('RUNTIME', 'WORK_QUEUE_SIZE') * .25)
    with BoundedThreadPoolExecutor(max_workers=workers,
                                   queue_size=size) as executor:
        for bucket in buckets:
            buckets_listed += 1
            executor.submit(bucket_lister, config, gcs, bucket, prefix,
                            buckets_listed, total_buckets, bucket_blob_counts)

    LOG.info("Stats: \n\t%s", bucket_blob_counts)
    LOG.info("Total rows: \n\t%s",
             sum([v for _, v in bucket_blob_counts.items()]))


def bucket_lister(config: ConfigParser, gcs: Client, bucket: Bucket,
                  prefix: str, bucket_number: int, total_buckets: int,
                  stats: dict) -> bool:
    """List a bucket, sending each page of the listing into an executor pool
    for processing.

    Arguments:
        config {ConfigParser} -- The program config.
        gcs {Client} -- A GCS client object.
        bucket {Bucket} -- A GCS Bucket object to list.
        bucket_number {int} -- The number of this bucket (out of the total).
        total_buckets {int} -- The total number of buckets that will be listed.
        stats {dict} -- A dictionary of bucket_name (str): blob_count (int)
    """
    LOG.info("Listing %s. %s of %s total buckets", bucket.name, bucket_number,
             total_buckets)
    stats[bucket] = 0

    # Check config to determine whether to retrieve ACL for each blob
    get_acl = config.getboolean("GCP", "ACLS", fallback=False)
    projection = ''
    if get_acl is True:
        projection = 'full'
    else:
        projection = 'noAcl'

    # Use remaining configured workers, or at least 2, for this part
    workers = max(config.getint('RUNTIME', 'WORKERS') - 2, 2)
    size = int(config.getint('RUNTIME', 'WORK_QUEUE_SIZE') * .75)
    with BoundedThreadPoolExecutor(max_workers=workers,
                                   queue_size=size) as sub_executor:
        blobs = gcs.list_blobs(bucket, prefix=prefix, projection=projection)
        for page in blobs.pages:
            sub_executor.submit(page_outputter, config, bucket, page, stats)
            sleep(0.02)  # small offset to avoid thundering herd


def page_outputter(config: ConfigParser, bucket: Bucket, page: Page,
                   stats: dict) -> bool:
    """Write a page of blob listing to LDJSON.

    Arguments:
        config {ConfigParser} -- The program config.
        bucket {Bucket} -- The bucket where this list page came from.
        page {Page} -- The Page object from the listing.
        stats {dict} -- A dictionary of bucket_name (str): blob_count (int)
    """
    blob_count = 0

    for blob in page:
        blob_count += 1
        # pylint: disable=protected-access
        blob_metadata = blob._properties
        if "metadata" in blob_metadata:
            blob_metadata["metadata"] = [{
                "key": k,
                "value": v
            } for k, v in blob_metadata["metadata"].items()]
        print(blob_metadata)

    if blob_count:
        stats[bucket] += blob_count
        LOG.info("%s blob records written for bucket %s.", stats[bucket],
                 bucket.name)
