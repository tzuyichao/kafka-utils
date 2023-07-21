import argparse
import itertools
import logging
import math
import sys
import time

from kafka_utils.util import config

def parse_opts():
    parser = argparse.ArgumentParser(
        description=('Performs a rolling restart of the specified '
                     'kafka cluster.'))
    parser.add_argument(
        '--cluster-type',
        '-t',
        required=True,
        help='cluster type, e.g. "standard"',
    )
    parser.add_argument(
        '--cluster-name',
        '-c',
        help='cluster name, e.g. "uswest1-devc" (defaults to local cluster)',
    )
    parser.add_argument(
        '--broker-ids',
        '-b',
        required=False,
        type=int,
        nargs='+',
        help='space separated broker IDs to restart (optional, will restart all Kafka brokers in cluster if not specified)',
    )
    parser.add_argument(
        '--discovery-base-path',
        dest='discovery_base_path',
        type=str,
        help='Path of the directory containing the <cluster_type>.yaml config',
    )
    parser.add_argument(
        '--no-confirm',
        help='proceed without asking confirmation. Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '-v',
        '--verbose',
        help='print verbose execution information. Default: %(default)s',
        action="store_true",
    )
    parser.add_argument(
        '--task',
        type=str,
        action='append',
        help='Module containing an implementation of Task.'
        'The module should be specified as path_to_include_to_py_path. '
        'ex. --task kafka_utils.kafka_rolling_restart.version_precheck'
    )
    parser.add_argument(
        '--task-args',
        type=str,
        action='append',
        help='Arguements which are needed by the task(prestoptask or poststoptask).'
    )
    parser.add_argument(
        '--start-command',
        type=str,
        help=('Override start command for kafka (do not include sudo)'
              'Default: %(default)s'),
        default=DEFAULT_START_COMMAND,
    )
    parser.add_argument(
        '--stop-command',
        type=str,
        help=('Override stop command for kafka (do not include sudo)'
              'Default: %(default)s'),
        default=DEFAULT_STOP_COMMAND,
    )
    parser.add_argument(
        '--ssh-password',
        type=str,
        help=('SSH passowrd to use if needed'),
    )
    return parser.parse_args()

def run():
    opts = parse_opts()
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)
    cluster_config = config.get_cluster_config(
        opts.cluster_type,
        opts.cluster_name,
        opts.discovery_base_path,
    )