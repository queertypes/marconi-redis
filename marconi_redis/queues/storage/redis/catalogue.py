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
from marconi.queues.storage import base
from marconi.queues.storage import errors
import six

from marconi_redis.queues.storage.redis import utils


def _lkey(project):
    return 'cs.%s' % project


def _key(project, queue):
    return 'c.%s.%s' % (project, queue)


class CatalogueController(base.CatalogueBase):

    def __init__(self, *args, **kwargs):
        super(CatalogueController, self).__init__(*args, **kwargs)

        self._db = self.driver.database

    @utils.raises_conn_error
    def list(self, project):
        cursor = self._db.lrange(_lkey(project), 0, -1)

        def it(proj):
            for queue in cursor:
                key = _key(proj, queue)
                shard = self._db.get(key)
                if not shard:
                    continue
                yield _normalize(project, queue, shard)

        return it(project)

    @utils.raises_conn_error
    def get(self, project, queue):
        shard = self._db.get(_key(project, queue))
        if not shard:
            raise errors.QueueNotMapped(project, queue)
        return _normalize(project, queue, shard)

    @utils.raises_conn_error
    def exists(self, project, queue):
        return self._db.exists(_key(project, queue))

    @utils.raises_conn_error
    def insert(self, project, queue, shard):
        added = self._db.rpush(_lkey(project), queue)
        self._db.set(_key(project, queue), shard)

        return added != 0

    @utils.raises_conn_error
    def delete(self, project, queue):
        self._db.lrem(_lkey(project), 0, queue)
        self._db.delete(_key(project, queue))

    @utils.raises_conn_error
    def update(self, project, queue, shard=None):
        if not self.exists(project, queue):
            raise errors.QueueNotMapped(project, queue)
        self._db.set(_key(project, queue), shard)

    @utils.raises_conn_error
    def drop_all(self):
        self._db.flushall()


def _normalize(project, queue, shard):
    return {
        'queue': six.text_type(queue),
        'project': six.text_type(project),
        'shard': six.text_type(shard)
    }
