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
import marconi.openstack.common.log as logging
from marconi import storage
from marconi.storage import exceptions
from marconi.storage.redis import utils

LOG = logging.getLogger(__name__)


class MessageController(storage.MessageBase):
    """Implements message resource operations using Redis.

    Messages:
      q => queue
      a => active
      c => claimed
      m => message
      {project}.q.{name}.a.ms => [{project}.q.{name}.a.m.{id1}, ...]
      {project}.q.{name}.c.ms => [{project}.q.{name}.c.m.{id1}, ...]
      {project}.q.{name}.a.m.{id} => ...
      {project}.q.{name}.c.m.{id} => ...
    """

    def __init__(self, *args, **kwargs):
        super(MessageController, self).__init__(*args, **kwargs)

        # Cache for convenience and performance (avoids extra lookups and
        # recreating the range for every request.)
        self._queue_ctrl = self.driver.queue_controller
        self._db = self.driver.db
        self._db.set('m.cnt', 0)  # number of messages posted for msg ID

    def _make_consistent(self, queue_id):
        active_list = '%s.a.ms' % (queue_id)
        stop = self._db.zcard(active_list)
        expired = [m for m in
                   self._db.zrange(active_list, 0, stop)
                   if self._db.ttl(m) == -1]
        if expired:
            self._db.zrem(active_list, *expired)

    def _list(self, queue_id, claim_id=None):
        self._make_consistent(queue_id)
        msgs = '%s.%s.ms' % (queue_id, 'c' if claim_id else 'a')

        # efficient facade for counting messages in Redis
        class Counter(object):
            def __init__(self, db, msgs_):
                self._count = db.zcard(msgs_)

            def count(self):
                return self._count

        return Counter(self._db, msgs)


    def active(self, queue_id, marker=None, echo=False,
               client_uuid=None, fields=None):
        return self._list(queue_id)

    def claimed(self, queue_id, claim_id=None, expires=None, limit=None):
        return self._list(queue_id, claim_id)

    @utils.raises_conn_error
    def list(self, queue, project=None, marker=None,
             limit=10, echo=False, client_uuid=None):
        self._make_consistent('%s.q.%s' % (project, queue))
        active_list = '%s.q.%s.a.ms' % (project, queue)
        start = self._db.zrank(active_list, marker) or 0
        stop = start + limit
        msg_keys = self._db.zrange(active_list, start, stop)
        marker_id = {}

        def it(keys):
            for key in keys:
                key_id = key.split('.')[-1]
                marker_id['next'] = key_id
                ttl = self._db.ttl(key)

                # remove expired keys on read
                if ttl == -1:
                    self._db.zrem(active_list, key)
                    continue

                yield {
                    'id': key_id,
                    'age': None,
                    'ttl': ttl,
                    'body': self._db.get(key)
                }

        yield it(msg_keys)
        yield str(marker_id['next'])

    @utils.raises_conn_error
    def get(self, queue, message_ids, project=None):
        active_list = '%s.q.%s.a.ms' % (project, queue)
        for message in message_ids:
            msg_key = '%s.q.%s.a.m.%s' % (project, queue, message)
            ttl = self._db.ttl(msg_key)

            if ttl == -1:
                self._db.zrem(active_list, msg_key)
                continue

            yield {
                'body': self._db.get(msg_key),
                'ttl': ttl,
                'id': message
            }

    @utils.raises_conn_error
    def bulk_get(self, queue, message_ids, project=None):
        pass


    @utils.raises_conn_error
    def post(self, queue, messages, client_uuid, project=None):
        ids = []
        msg_list = '%s.q.%s.a.ms' % (project, queue)
        msg = '%s.q.%s.a.m' % (project, queue)

        # message IDs are maintained as an atomic counter handled by
        # Redis. This is also used to take advantage of sorted sets,
        # where the score and the value are the same.
        for message in messages:
            msg_id = self._db.incr('m.cnt')
            msg_key = msg + '.' + str(msg_id)
            print(msg_key)
            added = self._db.zadd(msg_list, msg_id, msg_key)

            if added:
                self._db.set(msg_key, message['body'])
                self._db.expire(msg_key, message['ttl'])
                ids.append(str(msg_id))

        return ids

    @utils.raises_conn_error
    def delete(self, queue, message_id, project=None, claim=None):
        status = 'c' if claim else 'a'
        msg_list = '%s.q.%s.%s.ms' % (project, queue, status)
        msg_key = '%s.q.%s.%s.m.%s' % (project, queue, status, message_id)
        if self._db.zrem(msg_list, msg_key):
            self._db.delete(msg_key)

    @utils.raises_conn_error
    def bulk_delete(self, queue, message_ids, project=None):
        pass
