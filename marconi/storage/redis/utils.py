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

import functools

import redis

import marconi.openstack.common.log as logging
from marconi.storage import exceptions as storage_exceptions


LOG = logging.getLogger(__name__)


def raises_conn_error(func):
    """Handles redis ConnectionError

    This decorator catches redis' ConnectionError
    exceptions and raises Marconi's ConnectionError instead.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except redis.ConnectionError:
            # NOTE(flaper87): Raise the error
            msg = "ConnectionFailure caught"
            LOG.error(msg)
            raise storage_exceptions.ConnectionError(msg)
    return wrapper
