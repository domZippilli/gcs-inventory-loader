#!/usr/bin/env python3
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
Google Cloud Storage smart archiver main entry point.
"""
import logging
import sys
import warnings
from typing import List

import click

from gcs_inventory_loader.cli.cat import cat_command
from gcs_inventory_loader.cli.listen import listen_command
from gcs_inventory_loader.cli.load import load_command
from gcs_inventory_loader.config import config_to_string, set_config
from gcs_inventory_loader.utils import set_program_log_level

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")

logging.basicConfig()
LOG = logging.getLogger(__name__)


@click.group()
@click.option("-c",
              "--config_file",
              required=False,
              help="Path to the configuration file to use.",
              default="./default.cfg")
@click.option("-l",
              "--log_level",
              required=False,
              help="Set log level.",
              default=None)
@click.pass_context
def main(context: object = object(), **kwargs) -> None:
    """
    Inventory loader for GCS. Streams your bucket listing(s) into BigQuery.
    """
    context.obj = kwargs


def init(config_file: str = "./default.cfg", log_level: str = None) -> None:
    """
    Top-level initialization.
    Keyword Arguments:
        config_file {str} -- Path to config file. (default: {"./default.cfg"})
        log_level {str} -- Desired log level. (default: {None})
    """
    config = set_config(config_file)
    print("Configuration parsed: \n{}".format(config_to_string(config)),
          file=sys.stderr)
    set_program_log_level(log_level, config)


@main.command()
@click.option(
    '-p',
    '--prefix',
    required=False,
    help="A prefix to restrict the bucket listing(s) by. This is useful if you"
    " have a very large bucket and want to shard the listing work.",
    default=None)
@click.argument('buckets', nargs=-1, required=False, default=None)
@click.pass_context
def load(context: object,
         buckets: List[str] = None,
         prefix: str = None) -> None:
    """
    Build the inventory table with all objects in your bucket(s). The table
    will be named whatever you set to the INVENTORY_TABLE value in the
    configuration file. The table will be created if not found. If it is found,
    records will be appended.

    Optionally, you can provide a list of buckets (without gs://) to limit the
    scope. By default, all buckets in the configured project will be processed
    into the table.
    """
    init(**context.obj)
    return load_command(buckets, prefix)


@main.command()
@click.option(
    '-p',
    '--prefix',
    required=False,
    help="A prefix to restrict the bucket listing(s) by. This is useful if you"
    " have a very large bucket and want to shard the listing work.",
    default=None)
@click.argument('buckets', nargs=-1, required=False, default=None)
@click.pass_context
def cat(context: object,
        buckets: List[str] = None,
        prefix: str = None) -> None:
    """
    Write delimited JSON with all objects in your bucket(s) to stdout.

    Optionally, you can provide a list of buckets (without gs://) to limit the
    scope. By default, all buckets in the configured project will be processed
    into the table.
    """
    init(**context.obj)
    return cat_command(buckets, prefix)


@main.command()
@click.pass_context
def listen(context: object) -> None:
    """
    Listen to a PubSub subscription for object change events, and update the
    inventory table accordingly.

    Configuration for the PubSub topic and subscription should be set in the
    config file.
    """
    init(**context.obj)
    return listen_command()


if __name__ == "__main__":
    main()
