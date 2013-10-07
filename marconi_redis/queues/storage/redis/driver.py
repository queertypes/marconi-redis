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
"""Redis storage driver implementation."""
import redis

from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi_redis.queues.storage.redis import controllers
from marconi_redis.queues.storage.redis import options

LOG = logging.getLogger(__name__)


class Driver(storage.DriverBase):

    def __init__(self):
        self._database = None

    @property
    def db(self):
        """Property for lazy instantiation of redis's database."""
        host = options.CFG.host
        port = options.CFG.port
        if self._database is None:
            self._database = redis.StrictRedis(host=host, port=port)

        return self._database

    def gc(self):
        """gc is meaningless for the Redis implementation. All
        objects that have a TTL associated with them are automatically
        reaped by the Redis runtime.
        """
        pass

    @property
    def gc_interval(self):
        pass

    @property
    def queue_controller(self):
        return controllers.QueueController(self)

    @property
    def message_controller(self):
        return controllers.MessageController(self)

    @property
    def claim_controller(self):
        return controllers.ClaimController(self)
