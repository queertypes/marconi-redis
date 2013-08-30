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

import marconi.openstack.common.log as logging
from marconi import storage
from marconi.storage.redis import utils

LOG = logging.getLogger(__name__)


class ClaimController(storage.ClaimBase):
    """Implements claim resource operations using Redis."""

    @utils.raises_conn_error
    def get(self, queue, claim_id, project=None):
        raise NotImplementedError()

    @utils.raises_conn_error
    def create(self, queue, metadata, project=None, limit=10):
        """Creates a claim.
        """
        raise NotImplementedError()

    @utils.raises_conn_error
    def update(self, queue, claim_id, metadata, project=None):
        raise NotImplementedError()

    @utils.raises_conn_error
    def delete(self, queue, claim_id, project=None):
        raise NotImplementedError()