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

from .common import *
from . import _pychro

import time
import datetime
import collections
import mmap
import struct
import re
import os
import socket


class VanillaChronicleReader:
    # polling_interval of None means non-blocking and an exception of NoData will be raised
    # polling_interval of 0 means blocking spin (cpu intensive)
    #
    # provide date (for start of day) or index (which includes date)
    #
    # max mapped memory only relevant on windows due to the way memory mapped files are handled
    #
    # close() resets to chronicle, releasing all resources. Reading will begin again from the start.
    #

    def __init__(self, base_dir, polling_interval=None, date=None, full_index=None,
                 max_mapped_memory=DEFAULT_MAX_MAPPED_MEMORY_PER_READER,
                 thread_id_bits=None, utcnow=datetime.datetime.utcnow):
        self._index_file_size = INDEX_FILE_SIZE
        self._utcnow = utcnow
        self._thread_id_bits = thread_id_bits
        if self._thread_id_bits is None:
            with open('/proc/sys/kernel/pid_max') as fh:
                self._thread_id_bits = VanillaChronicleReader.get_thread_id_bits(int(fh.read().strip()))

        self._base_dir = base_dir
        self._index_data_offset_bits = 64 - self._thread_id_bits
        self._thread_id_idx_mask = eval('0b'+'1'*self._thread_id_bits+'0'*self._index_data_offset_bits)
        self._thread_id_mask = eval('0b'+'1'*self._thread_id_bits)
        self._index_data_offset_mask = eval('0b'+'0'*self._thread_id_bits+'1'*self._index_data_offset_bits)
        self._max_maps = (max_mapped_memory//DATA_FILE_SIZE) if max_mapped_memory else None
        if self._max_maps is not None and self._max_maps < 1:
            raise ConfigError('max_mapped_memory must be >= 64MB')
        self._polling_interval = polling_interval
        self._base_dir = base_dir

        self._max_index = 0
        self._index = 0
        self._date = None
        self._cycle_dir = None
        self._full_index_base = None
        self._index_fh = []
        self._index_mm = []
        self._data_fhs = dict()
        self._data_mms = collections.OrderedDict()
        index = None

        if full_index:
            if date:
                raise InvalidArgumentError('Providing index and date are mutually exclusive')
            date, index = VanillaChronicleReader.from_full_index(full_index)

        if date is None:
            try:
                self._try_set_cycle_dir()
            except NoData:
                return
        else:
            self._update_cycle_dir(os.path.join(base_dir, '%4d%02d%02d' % (date.year, date.month, date.day)))
        if index:
            self._index = index

    def __str__(self):
        return '<VanillaChronicleReader dir:%s idx:%s>' % (self._cycle_dir, self._index)

    def __del__(self):
        self.close()

    @staticmethod
    def get_thread_id_bits(pid_max):
        i = 0
        pid_max -= 1
        while pid_max:
            pid_max >>= 1
            i += 1
        return i

    @staticmethod
    def to_full_index(date, index):
        return index + ((int(datetime.datetime(date.year, date.month, date.day,
                        tzinfo=datetime.timezone.utc).timestamp())//86400) << CYCLE_INDEX_POS)

    @staticmethod
    def from_full_index(full_index):
        index = full_index & INDEX_OFFSET_MASK
        date = datetime.datetime.fromtimestamp((full_index >> CYCLE_INDEX_POS)*86400,
                                               tz=datetime.timezone.utc).date()
        return date, index

    def _update_cycle_dir(self, fp):
        self.close()
        self._cycle_dir = fp
        dstr = os.path.split(self._cycle_dir)[1]
        self._update_date_and_index_base(datetime.date(int(dstr[:4]), int(dstr[4:6]), int(dstr[6:8])))

    def _update_date_and_index_base(self, date):
        self._date = date
        self._full_index_base = VanillaChronicleReader.to_full_index(date, 0)

    def _open_next_index(self):
        file_num = len(self._index_fh)
        if not self._cycle_dir:
            self._try_set_cycle_dir()
        try:
            self._index_fh += [open(os.path.join(self._cycle_dir, 'index-%s' % file_num), 'rb')]
        except FileNotFoundError:
            raise EndOfIndexfile
        self._index_mm += [_pychro.open_read_mmap(self._index_fh[-1], INDEX_FILE_SIZE)]

    def _open_data_file(self, filenum, thread):
        if self._cycle_dir is None:
            if not self._try_next_date():
                raise NoData
        try:
            return open(os.path.join(self._cycle_dir, 'data-%s-%s' % (thread, filenum)), 'rb')
        except FileNotFoundError:
            raise CorruptData

    def _open_data_memory_map(self, filenum, thread):
        fh = self._data_fhs.get((filenum, thread))
        if not fh:
            fh = self._open_data_file(filenum, thread)
            self._data_fhs[(filenum, thread)] = fh
        return mmap.mmap(fh.fileno(), 0, prot=mmap.PROT_READ)

    def _try_set_cycle_dir(self, date=None):
        date_str = '%4d%02d%02d' % (date.year, date.month, date.day) if date else None
        for f in sorted(os.listdir(self._base_dir)):
            if date_str and date_str > f:
                continue
            fp = os.path.join(self._base_dir, f)
            if not re.match('^[0-9]{8}$', f):
                continue
            if not os.path.isdir(fp):
                continue
            self._update_cycle_dir(fp)
            return
        raise NoData

    def _try_next_date(self):
        _next = False
        if not self._cycle_dir:
            self._try_set_cycle_dir()
        cur_date = os.path.split(self._cycle_dir)[1]
        for f in sorted(os.listdir(self._base_dir)):
            if f == cur_date:
                _next = True
            elif _next:
                self._update_cycle_dir(os.path.join(self._base_dir, f))
                return True
        return False

    def _get_index_value(self, index_offset):
        index_offset *= 8
        index_filenum = index_offset >> FILENUM_FROM_INDEX_SHIFT
        index_offset &= INDEX_OFFSET_MASK
        if index_filenum >= len(self._index_mm):
            try:
                self._open_next_index()
            except EndOfIndexfile:
                return 0
        return _pychro.read_mmap(self._index_mm[index_filenum], index_offset)

    def _get_data_memory_map(self, filenum, thread):
        if (filenum, thread) in self._data_mms:
            return self._data_mms[(filenum, thread)]

        fm = self._open_data_memory_map(filenum, thread)
        self._data_mms[(filenum, thread)] = fm

        if self._max_maps and len(self._data_mms) > self._max_maps:
            try:
                filenum_thread, mm = self._data_mms.popitem(last=False)
                mm.close()
                self._data_fhs[filenum_thread].close()
                del self._data_fhs[filenum_thread]
            except ReferenceError:
                pass
        return fm

    def _prev_position_today(self):
        while self._index > 0:
            self._index -= 1

            val = self._get_index_value(self._index)
            pos = val & self._index_data_offset_mask

            if pos:
                filenum = (pos >> FILENUM_FROM_POS_SHIFT)
                pos = pos & POS_MASK
                thread = (val & self._thread_id_idx_mask) >> self._index_data_offset_bits
                return filenum, pos, thread
        raise NoData

    def _next_position(self):
        while True:
            val = self._get_index_value(self._index)
            pos = val & self._index_data_offset_mask

            if not pos:
                if self._date != self._utcnow().date() and self._try_next_date():
                    continue
                if self._polling_interval is None:
                    raise NoData
                if self._polling_interval != 0:
                    time.sleep(self._polling_interval)
                continue
            break

        filenum = (pos >> FILENUM_FROM_POS_SHIFT)
        pos = pos & POS_MASK

        self._index += 1
        thread = (val & self._thread_id_idx_mask) >> self._index_data_offset_bits

        return filenum, pos, thread

    def close(self):
        while True:
            try:
                self._data_mms.popitem()[1].close()
            except ReferenceError:
                break
            except KeyError:
                break

        while True:
            try:
                self._data_fhs.popitem()[1].close()
            except KeyError:
                break

        [_pychro.close_mmap(mm, self._index_file_size) for mm in self._index_mm if mm]
        self._index_mm = []

        [fh.close() for fh in self._index_fh if fh]
        self._index_fh = []

        self._max_index = 0
        self._index = 0
        self._date = None
        self._cycle_dir = None
        self._full_index_base = None

    def get_index(self):
        if self._full_index_base is None:
            raise NoData
        return self._index + self._full_index_base

    def next_index(self):
        self._next_position()
        return self._index + self._full_index_base

    def get_date(self):
        return self._date

    def get_raw_bytes(self, filenum, pos, thread):
        mm = self._get_data_memory_map(filenum, thread)
        return pos, mm

    def next_raw_bytes(self):
        return self.get_raw_bytes(*self._next_position())

    def set_index(self, full_index):
        date, index = VanillaChronicleReader.from_full_index(full_index)
        if self._date != date:
            self._try_set_cycle_dir(date)
        self._index = index

    def set_date(self, date):
        self._try_set_cycle_dir(date)

    def set_end(self):
        while self._try_next_date():
            pass
        self.set_end_index_today()

    def set_start_index_today(self):
        self._index = 0

    def set_end_index_today(self):
        self.set_index(self.get_end_index_today())

    def get_end_index_today(self):

        # minimum currently known
        low_idx = max(self._max_index, self._index)
        if not self._get_index_value(low_idx):
            return low_idx + self._full_index_base

        # find the maximum possible
        high_idx = ENTRIES_PER_INDEX_FILE-1
        while self._get_index_value(high_idx):
            high_idx += ENTRIES_PER_INDEX_FILE

        while True:
            current_idx = (low_idx + high_idx) // 2
            if self._get_index_value(current_idx):
                low_idx = current_idx
                if high_idx == low_idx + 1:
                    break
            else:
                high_idx = current_idx
                if low_idx == high_idx - 1:
                    break
        return high_idx + self._full_index_base

    def next_reader(self):
        return RawByteReader(*self.next_raw_bytes())


class RemoteChronicleReader:
    HEADER_LENGTH = 12
    IN_SYNC = -128
    PAD = -127
    SYNCED_OK = -126

    FROM_START = -1
    FROM_END = -2

    SUBSCRIBE = 1

    # where in 'start', 'end'/'now', index or date (YYYY-MM-DD)
    def __init__(self, host, port, where):
        self._host = host
        self._port = port
        self._idx = None
        self._soc = None

        if where == 'start':
            self._startidx = RemoteChronicleReader.FROM_START
        elif where == 'now' or where == 'end':
            self._startidx = RemoteChronicleReader.FROM_END
        else:
            try:
                self._startidx = int(where)
            except ValueError:
                try:
                    if where == 'today':
                        date = datetime.datetime.utcnow().date()
                    else:
                        date = datetime.date(*map(int, where.split('-')))
                    dt = datetime.datetime(date.year, date.month, date.day, tzinfo=datetime.timezone.utc)
                    self._startidx = (int(dt.timestamp()) << CYCLE_INDEX_POS)*86400
                except Exception as e:
                    raise InvalidArgumentError('Unable to determine start position for remote tailer from %s'
                                                      % where)

        self._soc = socket.create_connection((self._host, self._port))
        # subscribe to -1 start -2 end
        self._soc.send(struct.pack('qq', RemoteChronicleReader.SUBSCRIBE, self._startidx))
        while True:
            msg = self._soc.recv(RemoteChronicleReader.HEADER_LENGTH)
            length, index = struct.unpack('=iq', msg)
            if length in (RemoteChronicleReader.IN_SYNC, RemoteChronicleReader.PAD): # in-sync, pad
                continue
            elif length == RemoteChronicleReader.SYNCED_OK: # synced OK
                self._idx = index
                if where == 'now': # consume last message which we get with end..
                    self.next_reader()
                return
            else:
                raise Exception('In-Sync not received as expected (length:%s, index:%s)' % (length, index))

    def __str__(self):
        return '<RemoteChronicleReader host:%s port:%s idx:%s>' % (self._host, self._port, self._idx)

    def get_index(self):
        return self._idx

    def close(self):
        if self._soc:
            self._soc.close()
            self._soc = None

    def __del__(self):
        self.close()

    def next_reader(self):
        while True:

            hdr_chunks = []
            hdr_length = RemoteChronicleReader.HEADER_LENGTH
            while hdr_length > 0:
                hdr_chunks += [self._soc.recv(hdr_length)]
                hdr_length -= len(hdr_chunks[-1])

            body_length, index = struct.unpack('=iq', b''.join(hdr_chunks))
            if body_length in (RemoteChronicleReader.IN_SYNC, RemoteChronicleReader.PAD): # in-sync, pad
                continue

            self._idx = index
            body_chunks = [struct.pack('i', ~body_length)]
            while body_length > 0:
                body_chunks += [self._soc.recv(body_length)]
                body_length -= len(body_chunks[-1])
            return RawByteReader(4, b''.join(body_chunks))


class RawByteReader:
    __slots__ = ['_offset', '_bytes']

    def __init__(self, offset, _bytes):
        self._offset = offset
        self._bytes = _bytes

    def get_length(self):
        return ~struct.unpack('i', self._bytes[self._offset-4:self._offset])[0]

    def get_offset(self):
        return self._offset

    def get_bytes(self):
        return self._bytes

    def set_offset(self, offset):
        self._offset = offset

    def advance(self, num_bytes):
        self._offset += num_bytes

    def read_int(self):
        ret = struct.unpack('i', self._bytes[self._offset:self._offset+4])[0]
        self._offset += 4
        return ret

    def read_short(self):
        ret = struct.unpack('h', self._bytes[self._offset:self._offset+2])[0]
        self._offset += 2
        return ret

    def read_long(self):
        ret = struct.unpack('q', self._bytes[self._offset:self._offset+8])[0]
        self._offset += 8
        return ret

    def read_double(self):
        ret = struct.unpack('d', self._bytes[self._offset:self._offset+8])[0]
        self._offset += 8
        return ret

    def read_float(self):
        ret = struct.unpack('f', self._bytes[self._offset:self._offset+4])[0]
        self._offset += 4
        return ret

    # todo: remove. works for test data but not correct and no corresponding write
    def read_char(self): # utf16
        ret = self._bytes[self._offset:self._offset+2].decode('utf16')
        self._offset += 2
        return ret

    def read_byte(self):
        ret = self._bytes[self._offset]
        self._offset += 1
        return ret

    def read_boolean(self):
        ret = self._bytes[self._offset]
        self._offset += 1
        return ret != 0

    def read_stopbit(self):
        shift = 0
        value = 0
        while True:
            b = self.read_byte()
            value += (b & 0x7f) << shift
            shift += 7
            if (b & 0x80) == 0:
                return value

    def read_string(self):
        l = self.read_stopbit()
        ret = self._bytes[self._offset: self._offset + l].decode()
        self._offset += l
        return ret

    def read_fixed_string(self, size):
        start_pos = self._offset
        l = self.read_stopbit()
        ret = self._bytes[self._offset: self._offset + l].decode()
        self._offset = start_pos + size
        return ret

    def peek_int(self):
        return struct.unpack('i', self._bytes[self._offset:self._offset+4])[0]

    def peek_short(self):
        return struct.unpack('h', self._bytes[self._offset:self._offset+2])[0]

    def peek_long(self):
        return struct.unpack('q', self._bytes[self._offset:self._offset+8])[0]

    def peek_double(self):
        return struct.unpack('d', self._bytes[self._offset:self._offset+8])[0]

    def peek_char(self): # utf16
        return self._bytes[self._offset:self._offset+2].decode('utf16')

    def peek_byte(self):
        return self._bytes[self._offset]

    def peek_boolean(self):
        return self._bytes[self._offset] != 0

    def peek_string(self):
        o = self.get_offset()
        l = self.read_stopbit()
        ret = self._bytes[self._offset: self._offset + l].decode()
        self.set_offset(o)
        return ret

    # returns the string at the current offset,
    # and makes no guarantees about where the offset is left. The
    # user must set it before performing a future read.
    # This is the most efficient way to read a string.
    def peek_string_undef_offset(self):
        l = self.read_stopbit()
        return self._bytes[self._offset: self._offset + l].decode()






