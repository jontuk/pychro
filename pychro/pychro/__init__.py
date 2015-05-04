#
#  Copyright 2015 Jon Turner 
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

__all__ = ['vanilla_reader', 'vanilla_writer', '_pychro']

import platform

# Index is 24bits cycle (days since 1970), 16bits index file number, 24bits sequence of the cycle (day)

FILENUM_FROM_POS_SHIFT = 26
POS_MASK = eval('0b'+'1'*FILENUM_FROM_POS_SHIFT)
PLATFORM_WINDOWS = (platform.uname().system == 'Windows')
DEFAULT_MAX_MAPPED_MEMORY_PER_READER = 1024*1024*1024 if PLATFORM_WINDOWS else None
DATA_FILE_SIZE = 64*1024*1024 # 64MB
FILENUM_FROM_INDEX_SHIFT = 24 # 16MB
CYCLE_INDEX_POS = 40
INDEX_FILE_SIZE = 16*1024*1024
INDEX_OFFSET_MASK = eval('0b'+'1'*(FILENUM_FROM_INDEX_SHIFT))


class PychroException(Exception):
    pass


class NoData(PychroException):
    pass


class ConfigError(PychroException):
    pass


class NoSpace(PychroException):
    pass


class PartialWriteLostOnRollover(PychroException):
    pass


class InvalidArgumentError(PychroException):
    pass


# No data file or incorrect thread_id_bits. Copied from different platform or change in settings?
class CorruptData(PychroException):
    pass


# Differs from NoData, as may happen even when following
# as no Chronicle has been created for that date.
class NoChronicleForDate(PychroException):
    pass


from pychro.vanilla_reader import *
from pychro.vanilla_writer import *
from pychro._pychro import *