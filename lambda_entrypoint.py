#
# Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import logging
from lithops.version import __version__
from lithops.utils import setup_lithops_logger
from worker.handler import worker_handler
from lithops.worker.utils import get_runtime_preinstalls

logger = logging.getLogger('lithops.worker')


def lambda_handler(event, context):
    os.environ['__LITHOPS_ACTIVATION_ID'] = context.aws_request_id
    os.environ['__LITHOPS_BACKEND'] = 'AWS Lambda'

    setup_lithops_logger(event.get('log_level', logging.INFO))

    if 'get_preinstalls' in event:
        logger.info("Lithops v{} - Generating metadata".format(__version__))
        return get_runtime_preinstalls()
    else:
        logger.info("Lithops v{} - Starting AWS Lambda stateful worker".format(__version__))
        worker_handler(event)

    return {"Execution": "Finished"}
