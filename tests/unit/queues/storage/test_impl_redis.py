# Copyright (c) 2013 Rackspace, Inc.
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

import os

import testtools

from marconi import tests as testing
from marconi.tests.queues.storage import base
from marconi_redis.queues.storage import redis
from marconi_redis.queues.storage.redis import controllers


SKIP_REDIS_TESTS = os.environ.get('MARCONI_TEST_REDIS') is None


def requires_redis(test_case):
    """Decorator to indicate that a test requires Redis.

    If the environment variable is set, the tests will assume that
    redis-server is running.
    """

    reason = ('Skipping tests that require Redis. Ensure '
              'redis-server is running on localhost and then set '
              'MARCONI_TEST_REDIS in order to enable tests '
              'that are specific to this storage backend. ')

    return testtools.skipIf(SKIP_REDIS_TESTS, reason)(test_case)


@requires_redis
class RedisDriverTest(testing.TestBase):

    def setUp(self):
        super(RedisDriverTest, self).setUp()
        self.load_conf('wsgi_redis.conf')

    def test_db_instance(self):
        driver = redis.Driver()
        # check we can obtain a DB instance


@requires_redis
class RedisQueueTests(base.QueueControllerTest):

    driver_class = redis.Driver
    controller_class = controllers.QueueController

    def setUp(self):
        super(RedisQueueTests, self).setUp()
        self.load_conf('wsgi_redis.conf')

    def tearDown(self):
        super(RedisQueueTests, self).tearDown()


@requires_redis
class RedisMessageTests(base.MessageControllerTest):

    driver_class = redis.Driver
    controller_class = controllers.MessageController

    def setUp(self):
        super(RedisMessageTests, self).setUp()
        self.load_conf('wsgi_redis.conf')

    def tearDown(self):
        super(RedisMessageTests, self).tearDown()


@requires_redis
class RedisClaimTests(base.ClaimControllerTest):

    driver_class = redis.Driver
    controller_class = controllers.ClaimController

    def setUp(self):
        super(RedisClaimTests, self).setUp()
        self.load_conf('wsgi_redis.conf')

    def tearDown(self):
        super(RedisClaimTests, self).tearDown()
