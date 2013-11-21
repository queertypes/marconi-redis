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

from marconi.common import decorators
from marconi.openstack.common import log as logging
from marconi.queues import storage
from marconi_redis.queues.storage.redis import controllers
from marconi_redis.queues.storage.redis import options

LOG = logging.getLogger(__name__)


def _connection(conf):
    path = conf.unix_socket_path
    password = conf.password
    if path:
        return redis.StrictRedis(unix_socket_path=path,
                                 password=password)
    else:
        host = conf.host
        port = conf.port
        return redis.StrictRedis(host=host, port=port, password=password)


class DataDriver(storage.DataDriverBase):

    def __init__(self, conf):
        super(DataDriver, self).__init__(conf)

        opts = options.REDIS_OPTIONS
        self.conf.register_opts(opts, group=options.REDIS_GROUP)
        self.redis_conf = self.conf[options.REDIS_GROUP]

    @property
    def connection(self):
        return _connection(self.redis_conf)

    @property
    def queues_database(self):
        """Property for lazy instantiation of redis's database."""
        return self.connection

    @property
    def messages_databases(self):
        """Property for lazy instantiation of redis's database."""
        return self.connection

    @property
    def queue_controller(self):
        return controllers.QueueController(self)

    @property
    def message_controller(self):
        return controllers.MessageController(self)

    @property
    def claim_controller(self):
        return controllers.ClaimController(self)


class ControlDriver(storage.ControlDriverBase):

    def __init__(self, conf):
        super(ControlDriver, self).__init__(conf)

        self.conf.register_opts(options.REDIS_OPTIONS,
                                group=options.REDIS_GROUP)
        self.redis_conf = self.conf[options.REDIS_GROUP]

    @decorators.lazy_property(write=False)
    def connection(self):
        """MongoDB client connection instance."""
        return _connection(self.redis_conf)

    @decorators.lazy_property(write=False)
    def shards_database(self):
        return self.connection

    @property
    def shards_controller(self):
        return controllers.ShardsController(self)

    @decorators.lazy_property(write=False)
    def catalogue_database(self):
        return self.connection

    @property
    def catalogue_controller(self):
        return controllers.CatalogueController(self)
