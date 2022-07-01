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

import atexit
from concurrent.futures import TimeoutError
import json
import logging

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1 as pubsub
from google.cloud.pubsub_v1.subscriber.message import Message
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture

from gcs_inventory_loader.bq.output import BigQueryOutput
from gcs_inventory_loader.bq.tables import TableDefinitions, get_table
from gcs_inventory_loader.config import get_config
from gcs_inventory_loader.bq.client import get_bq_client

LOG = logging.getLogger(__name__)


def listen_command() -> None:
    """
    Implementation of the listen command.
    """
    config = get_config()
    # Call this once to initialize the table.
    _ = BigQueryOutput(
        get_table(TableDefinitions.INVENTORY,
                  config.get("BIGQUERY", "INVENTORY_TABLE")))

    subscriber = pubsub.SubscriberClient()
    topic_name = 'projects/{}/topics/{}'.format(
        config.get("GCP", "PROJECT"), config.get("PUBSUB", "TOPIC_SHORT_NAME"))
    subscription_name = 'projects/{}/subscriptions/{}'.format(
        config.get("GCP", "PROJECT"),
        config.get("PUBSUB", "SUBSCRIPTION_SHORT_NAME"))
    LOG.info("Creating or adopting subscription {}.".format(subscription_name))
    try:
        subscriber.create_subscription(name=subscription_name,
                                       topic=topic_name,
                                       ack_deadline_seconds=60)
    except AlreadyExists:
        pass

    output = BigQueryOutput(
        get_table(TableDefinitions.INVENTORY,
                  config.get("BIGQUERY", "INVENTORY_TABLE")), False)

    def handle(message):
        """Callback for handling new PubSub messages. Effectively, this just
        "partially applies" the output stream above to unpack_and_insert.
        """
        unpack_and_insert(output, message)

    def shutdown(sub_future: StreamingPullFuture) -> None:
        """Close subscriptions and flush rows to BQ.
        """
        LOG.info("Cancelling subscription pull.")
        sub_future.cancel()
        LOG.info("Flushing rows to BigQuery.")
        output.flush()

    LOG.info("Subscribing...")
    subscription_future = subscriber.subscribe(subscription_name, handle)

    atexit.register(shutdown, subscription_future)

    timeout = config.getint("PUBSUB", "TIMEOUT", fallback=10)

    with subscriber:
        while True:
            try:
                subscription_future.result(timeout=timeout)
            except TimeoutError:
                LOG.debug("No messages in {} seconds, flushing rows (if any).".
                          format(timeout))
                output.flush()
            except Exception:
                LOG.info("Quitting...")
                break


def unpack_and_insert(output: BigQueryOutput, message: Message) -> None:
    """Unpack a PubSub message regarding a GCS object change, and insert it into
    a BigQueryOutput.

    Args:
        output (BigQueryOutput): The output to use. In most cases, you will
        want to use a single output object per program.
        message (Message): The PubSub message.
    """
    bq_client = get_bq_client()
    config = get_config()
    table = get_table(TableDefinitions.INVENTORY,
                      config.get("BIGQUERY", "INVENTORY_TABLE"))
    table_name = table.get_fully_qualified_name()

    try:
        LOG.debug("Message data: \n---DATA---\n{}\n---DATA---".format(
            message.data))

        # Decode and deserialize
        message_string = bytes.decode(message.data, "UTF-8")
        object_info = json.loads(message_string)

        LOG.debug(message)
        LOG.debug(object_info)

        # Get important attributes
        event_type = message.attributes['eventType']
        publish_time = message.publish_time.isoformat()
        LOG.info("Got a message: {} {} {}".format(
            publish_time, event_type,
            object_info['bucket'] + "/" + object_info['name']))

        # For deletes, use the publish time to approximate deleted time
        if event_type == "OBJECT_DELETE":
            object_info["timeDeleted"] = publish_time
            if object_info.get("metadata"):
                object_info["metadata"] = [{
                    "key": k,
                    "value": v
                } for k, v in object_info["metadata"].items()]

        if event_type == "OBJECT_METADATA_UPDATE":

            def generate_structs(arr):
                res = '['
                for s in arr:
                    res += "STRUCT(\"{key}\" as key, \"{value}\" as value),".format(  # noqa: E501
                        key=s['key'], value=s['value'])
                res = res[:-1]
                res += ']'
                return res

            querytext = "UPDATE `{table_name}`\
                SET metadata = {new_metadata}\
                WHERE id = '{id}'".format(
                table_name=table_name,
                new_metadata=generate_structs([{
                    "key": k,
                    "value": v
                } for k, v in object_info["metadata"].items()]),
                id=object_info["id"])
            LOG.info("Running query: \n%s", querytext)
            query_job = bq_client.query(querytext)
            LOG.info(query_job.result())
        else:
            # Enqueue for writing
            output.put(object_info)

        message.ack()

    except Exception:
        LOG.exception(
            "Error processing message! ---DATA---\n{}\n---DATA---".format(
                message.data))
        # TODO: A retry / DLQ policy would be useful, if not already present
        # by default.
        message.nack()
