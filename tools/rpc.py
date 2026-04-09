#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Tool """

import functools
import traceback

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


def wrap_exceptions(target_exception):
    """ Wrap exceptions into generic exception (for RPC transport) """
    #
    def _decorator(func):
        _target_exception = target_exception
        #
        @functools.wraps(func)
        def _decorated(*_args, **_kvargs):
            try:
                return func(*_args, **_kvargs)
            except BaseException as exception_data:
                if isinstance(exception_data, _target_exception):
                    raise exception_data
                raise _target_exception(traceback.format_exc())  # pylint: disable=W0707
        #
        return _decorated
    #
    return _decorator
