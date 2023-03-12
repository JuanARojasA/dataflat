'''
commons.py - a script file for common used functions

Copyright (C) 2023 Juan ROJAS
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Authors:
    Juan ROJAS <jarojasa97@gmail.com>
'''

import logging
from typeguard import typechecked

@typechecked
def init_logger(file_name:str, log_level=None):
    if not log_level:
        log_level = 'INFO'
    logging.basicConfig()
    logger = logging.getLogger(file_name)
    logger.setLevel(getattr(logging, log_level))
    return logger