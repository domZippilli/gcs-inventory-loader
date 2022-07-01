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
Utility functions not specific to any submodule.
"""

import logging
import sys
from configparser import ConfigParser

from gcs_inventory_loader.constants import PROGRAM_ROOT_LOGGER_NAME


def validate_log_level(level: str) -> bool:
    """Test whether a log level is valid.

    Arguments:
        level {str} -- The log level to test.

    Returns:
        bool -- True if the log level is valid.
    """
    return hasattr(logging, level)


def set_program_log_level(command_line_arg, config: ConfigParser) -> None:
    """Set the log level for the root logger for this program.

    Arguments:
        args {Namespace} -- Arguments given to program execution.
        config {ConfigParser} -- Configuration given to the program.

    Returns:
        None
    """
    program_root_logger = logging.getLogger(PROGRAM_ROOT_LOGGER_NAME)
    level = 'INFO'  # Default log level
    set_by = 'default'
    if config.get('RUNTIME', 'LOG_LEVEL', fallback=None):
        # Config file should override the default
        candidate = config['RUNTIME']['LOG_LEVEL']
        if validate_log_level(candidate):
            level = candidate
            set_by = 'config file'
        else:
            print("Invalid log level from config file: {}".format(candidate))
    if command_line_arg:
        # Argument should override the config file and the default
        candidate = command_line_arg
        if validate_log_level(candidate):
            level = candidate
            set_by = 'command line argument'
        else:
            print("Invalid log level from command line: {}".format(candidate))
    program_root_logger.setLevel(level)
    print("Log level is {}, set by {}".format(level, set_by), file=sys.stderr)
