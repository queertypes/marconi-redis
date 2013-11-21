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
# See the License for the specific language governing permissions and
# limitations under the License.

"""Redis storage driver configuration options."""

from oslo.config import cfg

# All options line up with the arguments list for
# the python-redis client StrictRedis constructor
REDIS_OPTIONS = [
    # Database URI
    cfg.StrOpt('host', default='localhost',
               help='Address of redis server'),

    # Database port
    cfg.IntOpt('port', default=6379,
               help='Port to communicate with redis server.'),

    # For a local server: unix socket path to use to communicate
    # If specified, takes precedence over host:port.
    cfg.StrOpt('unix_socket_path',
               help=('Useful for redis on localhost - takes precedence ',
                     'over host:port.')),

    # Password to access DB
    cfg.StrOpt('password',
               help='If redis requires auth, place the password here.'),
]

REDIS_GROUP = 'queues:drivers:storage:redis'
