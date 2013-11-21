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
"""Implements the Redis storage controller for claims."""
import uuid

import marconi.openstack.common.log as logging
from marconi.queues import storage
from marconi_redis.queues.storage.redis import utils

LOG = logging.getLogger(__name__)


class ClaimController(storage.ClaimBase):
    """Implements claim resource operations using Redis.

    Schema:
      c.{project}.{queue}.{cid} -> redis.ttl_key
    """
    def __init__(self, *args, **kwargs):
        super(ClaimController, self).__init__(*args, **kwargs)
        self._msg_ctrl = self.driver.message_controller
        self._db = self.driver.database

    def _key(self, project, queue, cid):
        return 'c.%s.%s.%s' % (project, queue, cid)

    @utils.raises_conn_error
    def get(self, queue, claim_id, project=None):
        """Returns age, TTL, list of claimed messages."""
        raise NotImplementedError()

    @utils.raises_conn_error
    def create(self, queue, metadata, project=None, limit=10):
        """FIFO: sets the claim ID field for a set of messages.

        :returns: A list of claimed messages
        """
        cid = str(uuid.uuid4())
        ttl = metadata['ttl']
        grace = metadata['grace']
        for mid in self._msg_ctrl._active_messages(project, queue,
                                                   limit=limit):
            key = self._msg_ctrl._message(project, queue, mid)
            old_ttl = 100

            self._db.hset(key, 'c', cid)
            self._db.expire(key, old_ttl + ttl + grace)

        claim_key = self._key(project, queue, cid)
        self._db.set(claim_key, 0)
        self._db.expire(claim_key, ttl + grace)

    @utils.raises_conn_error
    def update(self, queue, claim_id, metadata, project=None):
        """Updates the ttl."""
        key = self._key(project, queue, claim_id)
        # raise if not exists
        self._db.expire(key, metadata['ttl'])

    @utils.raises_conn_error
    def delete(self, queue, claim_id, project=None):
        """Renames message keys from *cm* to *m*."""
        key = self._key(project, queue, claim_id)
        self._db.delete(key)
        # release messages
