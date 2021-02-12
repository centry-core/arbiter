#!/usr/bin/python
# coding=utf-8

#!/usr/bin/python3
# coding=utf-8

#   Copyright 2020 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Worker """

import os

from argparse import ArgumentParser


def arg_parse():
    parser = ArgumentParser(description='Arbiter worker processot')
    parser.add_argument('-a', '--app', type=str, default=os.environ("ARBITER_TASKS"),
                        help="Name of installed package to get tasks from")
    parser.add_argument('-w', '--workers', type=int, default=os.environ("ARBITER_WORKER_COUNT", 1),
                        help="Quantity of workers to be spawned. Default: 1")
    parser.add_argument('-t', '--worker_type', type=str, default=os.environ("ARBITER_WORKER_TYPE"),
                        help="Type of a worker for arbiter [heavy, light], can be set in ARBITER_WORKER_TYPE env var")
    args, _ = parser.parse_known_args()
    return args


def main():
    """ Main thread: start all and listen for broadcasts """
    # Parse settings
    settings = arg_parse()


if __name__ == "__main__":
    main()
