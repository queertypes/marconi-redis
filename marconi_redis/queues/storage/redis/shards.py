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
from marconi.common import utils as common_utils
from marconi.queues.storage import base
from marconi.queues.storage import errors
import msgpack

from marconi_redis.queues.storage.redis import utils


def _lkey():
    return 'ss'


def _key(name):
    return 's.%s' % name


class ShardsController(base.ShardsBase):

    def __init__(self, *args, **kwargs):
        super(ShardsController, self).__init__(*args, **kwargs)

        self._db = self.driver.database

    @utils.raises_conn_error
    def list(self, marker=None, limit=None, detailed=False):
        if limit is None:
            limit = 10

        lkey = _lkey()
        start = utils.lfind(self._db, _lkey(), marker)
        cursor = (q for q in
                  self._db.sort(lkey, start=start, num=limit))

        def it():
            for shard in cursor:
                w, u, o = self._db.hmget(_key(shard), ['w', 'u', 'o'])
                if not all([w, u, o]):
                    continue
                yield _normalize(shard, w, u, o, detailed)

        return it()

    @utils.raises_conn_error
    def get(self, name, detailed=False):
        fields = ['w', 'u', 'o']
        values = self._db.hmget(_key(name), fields)

        if not all(values):
            raise errors.ShardDoesNotExist(name)

        return _normalize(name, values[0], values[1], values[2], detailed)

    @utils.raises_conn_error
    def create(self, name, weight, uri, options=None):
        added = self._db.rpush(_lkey(), name)
        self._db.hmset(_key(name), {
            'w': weight,
            'u': uri,
            'o': msgpack.dumps(options or {})
        })

        return added != 0

    @utils.raises_conn_error
    def exists(self, name):
        return self._db.exists(_key(name))

    @utils.raises_conn_error
    def update(self, name, **kwargs):
        names = ('uri', 'weight', 'options')
        fields = common_utils.fields(kwargs, names,
                                     pred=lambda x: x is not None,
                                     key_transform=lambda x: x[0])
        assert fields, '`weight`, `uri`, or `options` not found in kwargs'
        fields['o'] = msgpack.dumps(fields['o'])
        self._db.hmset(_key(name), fields)

    @utils.raises_conn_error
    def delete(self, name):
        self._db.delete(_key(name))
        self._db.lrem(_lkey(), 0, name)

    @utils.raises_conn_error
    def drop_all(self):
        self._db.flushall()


def _normalize(name, weight, uri, options, detailed):
    ret = {
        'name': name,
        'weight': int(weight),
        'uri': uri
    }

    if detailed:
        ret['options'] = msgpack.loads(options)

    return ret
