# Copyright (c) 2013 Rackspace Hosting, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Redis storage driver configuration options."""

from marconi.common import config

# All options line up with the arguments list for
# the python-redis client StrictRedis constructor
OPTIONS = {
    # Database URI
    'host': 'localhost',

    # Database port
    'port': 6379,

    # For a local server: unix socket path to use to communicate
    # If specified, takes precedence over host:port.
    'unix_socket_path': None,

    # Password to access DB
    'password': None,

    # How long to wait before timing out a request to the DB
    'socket_timeout': None,

    # Default charset of communication to/fro DB
    'charset': 'utf-8',

    # How to handle encoding errors
    'errors': 'strict',

    # https://github.com/andymccurdy/redis-py#connection-pools
    'connection_pool': None,

    # ???
    'db': 0,

    # ???
    'decode_responses': False
}

CFG = config.namespace('drivers:storage:redis').from_options(**OPTIONS)
