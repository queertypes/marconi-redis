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


import marconi.openstack.common.log as logging
from marconi import storage
from marconi.storage import exceptions
from marconi.storage.redis import utils

LOG = logging.getLogger(__name__)


class QueueController(storage.QueueBase):
    """Implements queue resource operations using Redis.

    Queues:
        project_id.qs -> [project_id.q.name1, project_id.q.name2, ...]
        project_id.q.name1 -> {'metadata': {}, 'counter': {}}
    """

    def __init__(self, *args, **kwargs):
        super(QueueController, self).__init__(*args, **kwargs)
        self._db = self.driver.db

    #-----------------------------------------------------------------------
    # Interface
    #-----------------------------------------------------------------------

    def exists(self, name, project=None):
        key = '%s.q.%s' % (name, project)
        return self._db.exists(key)

    @utils.raises_conn_error
    def list(self, project=None, marker=None,
             limit=10, detailed=False):
        proj_idx = '%s.qs' % (project, )
        start = self._db.zrank(proj_idx, marker) or 0
        stop = start + limit
        cursor = (q.split('.')[-1] for q in
                  self._db.zrange(proj_idx, start, stop))
        marker_name = {}

        def it():
            for queue in cursor:
                marker_name['next'] = queue
                yield ({'name': queue} if not detailed
                       else
                       {'name': queue,
                        'metadata': self._db.get('%s.q.%s.m' % (project,
                                                                queue))})

        yield it()
        yield marker_name['next']

    @utils.raises_conn_error
    def get(self, name, project=None):
        """Fetch queue metadata."""
        return self._db.get('%s.q.%s.m' % (project, name)) or {}

    @utils.raises_conn_error
    def create(self, name, project=None):
        """Creates a redis queue.
        1. Create a ref in the project listing set.

        2. If this is a new ref, creates two keys: one for the queue,
        one for the metadata
        """
        queue_name = '%s.q.%s' % (project, name)
        added = self._db.zadd('%s.qs' % (project,),
                              1, queue_name)  # sorted sets: entries with same
                                              # score are sorted
                                              # lexicographically
        if added:
            self._db.set(queue_name, None)
            self._db.set(queue_name + '.m', {})  # metadata
        return added != 0

    @utils.raises_conn_error
    def update(self, name, metadata, project=None):
        if not self._db.exists('%s.q.%s' % (project, name)):
            raise exceptions.QueueDoesNotExist(name, project)
        self._db.set('%s.q.%s.m' % (project, name), metadata)

    @utils.raises_conn_error
    def delete(self, name, project=None):
        queue_name = '%s.q.%s' % (project, name)

    @utils.raises_conn_error
    def stats(self, name, project=None, claim=None):
        queue_id = '%s.q.%s' % (project, name)
        controller = self.driver.message_controller
        active = controller.active(queue_id)

        return {
            'actions': 0,
            'messages': {
                'free': active.count()
            }
        }

    @utils.raises_conn_error
    def actions(self, name, project=None, marker=None, limit=10):
        raise NotImplementedError
