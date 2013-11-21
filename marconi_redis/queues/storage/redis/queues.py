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
"""Implements the Redis storage controller for queues."""
import msgpack

import marconi.openstack.common.log as logging
from marconi.queues import storage
from marconi.queues.storage import errors
from marconi_redis.queues.storage.redis import utils

LOG = logging.getLogger(__name__)


class QueueController(storage.QueueBase):
    """Implements queue resource operations using Redis.

    Schema:
      qs.{project} -> redis.zset[name, name, name, ...]
      q.{project}.{name} -> redis.hset{'m': MsgpackBlob}

    Key:
      'm': metadata
    """

    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self._db = self.driver.database

    def _list(self, project):
        return 'qs.%s' % (project or '_')

    def _queue(self, project, name):
        return 'q.%s.%s' % (project or '_', name)

    #-----------------------------------------------------------------------
    # Interface
    #-----------------------------------------------------------------------

    @utils.raises_conn_error
    def exists(self, name, project=None):
        key = self._queue(project, name)
        return self._db.exists(key)

    @utils.raises_conn_error
    def list(self, project=None, marker=None,
             limit=10, detailed=False):
        qskey = self._list(project)
        start = self._db.zrank(qskey, marker) or 0
        stop = start + limit
        cursor = (q for q in self._db.zrange(qskey, start, stop))
        marker = {}

        def it():
            for queue in cursor:
                marker['next'] = queue
                m = self._db.hget(self._queue(project, queue), 'm')
                yield ({'name': queue} if not detailed
                       else
                       {'name': queue,
                        'metadata': msgpack.loads(m)})

        yield it()
        yield marker['next']

    @utils.raises_conn_error
    def get_metadata(self, name, project=None):
        """Fetch queue metadata."""
        m = self._db.hget(self._queue(project, name), 'm')
        return msgpack.loads(m)

    @utils.raises_conn_error
    def create(self, name, project=None):
        """Creates a redis queue.
        1. Create a ref in the project listing set.

        2. If this is a new ref, creates two keys: one for the queue,
        one for the metadata
        """

        # sorted sets: entries with same score are sorted lexicographically
        added = self._db.zadd(self._list(project),
                              1, name)

        if added:
            self._db.hset(self._queue(project, name), 'm', msgpack.dumps({}))
        return added != 0

    @utils.raises_conn_error
    def set_metadata(self, name, metadata, project=None):
        key = self._queue(project, name)
        if not self._db.exists(key):
            raise errors.QueueDoesNotExist(name, project)
        self._db.hset(key, 'm', msgpack.dumps(metadata))

    @utils.raises_conn_error
    def delete(self, name, project=None):
        """Deletes a queue, then deletes all the messages."""
        qlist = self._list(project)
        key = self._queue(project, name)
        self._db.delete(key)
        self._db.zremrangebyscore(qlist, -1, 1)

    @utils.raises_conn_error
    def stats(self, name, project=None, claim=None):
        key = self._queue(project, name)
        controller = self.driver.message_controller
        active = controller.active(key)

        return {
            'actions': 0,
            'messages': {
                'free': active.count(),
                'claimed': 0,
                'total': 0
            }
        }

    @utils.raises_conn_error
    def actions(self, name, project=None, marker=None, limit=10):
        raise NotImplementedError
