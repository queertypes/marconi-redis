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
"""Implements Redis storage controller for messages."""
import itertools
import time

import msgpack

import marconi.openstack.common.log as logging
from marconi.queues import storage
from marconi.queues.storage import errors
from marconi_redis.queues.storage.redis import utils

LOG = logging.getLogger(__name__)


class MessageController(storage.MessageBase):
    """Implements message resource operations using Redis.

    Schema:
      q -> queue
      m -> message
      q.{project}.{queue}.m.{mid} -> redis.hset{'b': ..., 'c'}
        b -> body, Msgpack Blob
        c -> claim ID
        t -> creation timestamp for age calculations
        k -> client ID
      q.{project}.{queue}.ms -> redis.zset[id, id, id, ...]
    """

    def __init__(self, *args, **kwargs):
        super(MessageController, self).__init__(*args, **kwargs)

        # Cache for convenience and performance (avoids extra lookups and
        # recreating the range for every request.)
        self._queue_ctrl = self.driver.queue_controller
        self._db = self.driver.database
        self._db.set('m.cnt', 0)  # number of messages posted for msg ID

    def _message(self, project, queue, mid):
        return 'q.%s.%s.m.%s' % (project, queue, mid)

    def _mlist(self, project, queue):
        return 'q.%s.%s.ms' % (project, queue)

    def _is_claimed(self, mkey):
        return self._db.hget(mkey, 'c') is not None

    def _active_messages(self, project, queue, marker=None, limit=None):
        return self._all_messages(project, queue, marker, limit,
                                  lambda m: not self._is_claimed(m))

    def _claimed_messages(self, project, queue, marker=None, limit=None):
        return self._all_messages(project, queue, marker, limit,
                                  lambda m: self._is_claimed(m))

    def _all_messages(self, project, queue, marker=None,
                      limit=None, predicate=None):
        key = 'q.%s.%s.ms' % (project, queue)
        start = self._db.zrank(key, marker) or 0
        predicate = predicate or (lambda m: True)
        gen = (m for m in self._db.zrange(key, start, -1)
               if predicate(self._message(project, queue, m)))
        return itertools.islice(gen, limit)

    def _make_consistent(self, project, queue):
        """Use this before any read operation to always yield a consistent
        view.
        """
        expired = (m for m in
                   self._all_messages(project, queue)
                   if self._db.ttl(m) == -1)
        if expired:
            self._remove(project, queue, expired)

    def _remove(self, project, queue, messages):
        """Bulk delete operation - network friendly."""
        keys = (self._message(project, queue, m) for m in messages)
        print(keys, messages)
        self._db.zrem(self._mlist(project, queue), messages)
        self._db.delete(*keys)

    @utils.raises_conn_error
    def list(self, queue, project=None, marker=None,
             limit=10, echo=False, client_uuid=None):
        self._make_consistent(queue, project)
        mids = self._active_messages(project, queue, marker, limit)
        marker = {}

        def it(ids):
            for key in ids:
                marker['next'] = key

                ttl = self._db.ttl(key)
                if ttl == -1:
                    self._remove(project, queue, [key])
                    continue

                b, t = self._db.hmget(key, ['b', 't'])
                if not all([b, t]):
                    continue

                yield {
                    'id': key,
                    'age': int(time.time()) - int(t),
                    'ttl': ttl,
                    'body': msgpack.loads(b)
                }

        yield it(mids)
        yield str(marker['next'])

    @utils.raises_conn_error
    def get(self, queue, message_id, project=None):
        key = self._message(project, queue, message_id)
        b, t = self._db.hmget(key, ['b', 't'])

        ttl = self._db.ttl(key)
        if ttl == -1:
            self._remove(project, queue, [key])
            raise errors.MessageDoesNotExist(message_id, queue, project)

        if not all([b, t]):
            raise errors.MessageDoesNotExist(message_id, queue, project)

        return {
            'body': msgpack.loads(b),
            'age': int(time.time()) - int(t),
            'ttl': ttl,
            'id': message_id
        }

    @utils.raises_conn_error
    def bulk_get(self, queue, message_ids, project=None):
        for mid in message_ids:
            key = self._message(project, queue, mid)

            ttl = self._db.ttl(key)
            if ttl == -1:
                self._remove(project, queue, [key])

            b, t = self._db.hmget(key, ['b', 't'])
            if not all([b, t]):
                continue

            yield {
                'body': msgpack.loads(b),
                'age': int(time.time()) - int(t),
                'ttl': ttl,
                'id': mid
            }

    @utils.raises_conn_error
    def post(self, queue, messages, client_uuid, project=None):
        ids = []
        list_key = self._mlist(project, queue)

        # NOTE(cpp-cabrera): message IDs are maintained as an atomic
        # counter handled by Redis. This is also used to take
        # advantage of sorted sets, where the score and the value are
        # the same.
        for message in messages:
            msg_id = self._db.incr('m.cnt')
            msg_key = self._message(project, queue, msg_id)
            added = self._db.zadd(list_key, 1.0, msg_id)

            if added:
                self._db.hmset(msg_key, {
                    'b': msgpack.dumps(message['body']),
                    't': time.time(),
                    'k': client_uuid
                })
                self._db.expire(msg_key, message['ttl'])
                ids.append(str(msg_id))

        return ids

    def first(self, queue, project=None, sort=1):
        if sort != 1:
            yield reversed(self._active_messages(project, queue))
        else:
            yield self._active_messages(project, queue)

    @utils.raises_conn_error
    def delete(self, queue, message_id, project=None, claim=None):
        self._remove(project, queue, [message_id])

    @utils.raises_conn_error
    def bulk_delete(self, queue, message_ids, project=None):
        self._remove(project, queue, message_ids)
