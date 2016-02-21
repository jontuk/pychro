
# Index is 24bits cycle (days since 1970), 16bits index file number, 24bits sequence of the cycle (day)

import sys

FILENUM_FROM_POS_SHIFT = 26
POS_MASK = eval('0b'+'1'*FILENUM_FROM_POS_SHIFT)
DEFAULT_MAX_MAPPED_MEMORY_PER_READER = 1024*1024*1024
DATA_FILE_SIZE = 64*1024*1024 # 64MB
FILENUM_FROM_INDEX_SHIFT = 24 # 16MB
CYCLE_INDEX_POS = 40
INDEX_FILE_SIZE = 16*1024*1024
ENTRIES_PER_INDEX_FILE = INDEX_FILE_SIZE//8
INDEX_OFFSET_MASK = eval('0b'+'1'*FILENUM_FROM_INDEX_SHIFT)


class PychroException(Exception):
    pass


class UnsupportedPlatformException(Exception):
    pass


if sys.version_info.major != 3:
    raise UnsupportedPlatformException('Only python3 is supported')


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
class EndOfIndexfile(PychroException):
    pass


from .vanilla_reader import *
from .vanilla_writer import *