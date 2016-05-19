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

import struct
import os
import mmap


class Appender:
    def __init__(self, chronicle, tid, filenum, pos, utcnow, max_msg_size=64*1024):
        self._tid = tid
        self._utcnow = utcnow
        self._chronicle = chronicle
        self._filenum = filenum
        self._pos = pos
        self._start_pos = self._pos
        self._max_msg_size = max_msg_size
        self._start_date = None
        self._mm = None

    def get_bytes(self):
        self._start()
        return self._mm

    def get_offset(self):
        return self._pos

    def fill(self, size, ch):
        mm = self._start()
        if self._pos + size >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+size] = ch*size
        self._pos += size

    def advance(self, size):
        self._start()
        if self._pos + size >= DATA_FILE_SIZE:
            raise NoSpace
        self._pos += size

    def bytes_written(self):
        return self._pos - self._start_pos

    def write_byte(self, val):
        assert val < 256
        mm = self._start()
        if self._pos + 1 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos] = val
        self._pos += 1

    def write_double(self, val):
        mm = self._start()
        if self._pos + 8 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+8] = struct.pack('d', val)
        self._pos += 8

    def write_float(self, val):
        mm = self._start()
        if self._pos + 4 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+4] = struct.pack('f', val)
        self._pos += 4

    def write_boolean(self, val):
        mm = self._start()
        if self._pos + 1 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos] = 1 if val else 0
        self._pos += 1

    def write_short(self, val):
        mm = self._start()
        if self._pos + 2 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+2] = struct.pack('h', val)
        self._pos += 2

    def write_long(self, val):
        mm = self._start()
        if self._pos + 8 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+8] = struct.pack('q', val)
        self._pos += 8

    def write_int(self, val):
        mm = self._start()
        if self._pos + 4 >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+4] = struct.pack('i', val)
        self._pos += 4

    # will add filler to fixed_size if set
    # else if serialises to larger, is an error
    def write_fixed_string(self, val, size):
        mm = self._start()
        start_pos = self._pos
        encoded = val.encode()
        l = len(encoded)
        self.write_stopbit(l)
        if self._pos - start_pos + l > size:
            raise InvalidArgumentError
        if self._pos + l >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+l] = encoded
        self._pos = start_pos + size

    def write_string(self, val):
        mm = self._start()
        encoded = val.encode()
        l = len(encoded)
        self.write_stopbit(l)
        if self._pos + l >= DATA_FILE_SIZE:
            raise NoSpace
        mm[self._pos:self._pos+l] = encoded
        self._pos += l

    def write_stopbit(self, val):
        if val < 0:
            raise ValueError('Stop-bit encoding does not support negative values')
        mm = self._start()
        while val > 127:
            mm[self._pos] = 0x80 | (val & 0x7f)
            self._pos += 1
            val >>= 7
        mm[self._pos] = val
        self._pos += 1

    def _start(self):
        if self._start_date is None:
            self._start_date = self._utcnow().date()
            if self._start_date != self._chronicle._date:
                self._chronicle._day_rollover(self._start_date)
                self._pos = 4
                self._start_pos = self._pos
                self._filenum = 0
                self._mm = None
        if self._mm is None:
            self._mm = self._chronicle._get_data_memory_map(self._filenum, self._tid)
        return self._mm

    def finish(self):
        length = self._pos - self._start_pos
        now_date = self._utcnow().date()
        if now_date != self._chronicle._date:
            # need to rewrite pos-start_pos bytes
            bytes = self._mm(self._filenum, self._tid)[self._start_pos:self._pos]
            if not self._chronicle._day_rollover(now_date):
                raise PartialWriteLostOnRollover()
            self._pos = length + 4
            self._start_pos = 4
            self._filenum = 0
            self._mm = self._chronicle._get_data_memory_map(self._filenum, self._tid)
            self._mm[self._start_pos:self._pos] = bytes

        self._mm[self._start_pos-4:self._start_pos] = struct.pack('i', ~length)

        self._chronicle._set_index(self._tid, self._filenum, self._start_pos)

        if self._pos + self._max_msg_size > DATA_FILE_SIZE:
            self._pos = 0
            self._filenum += 1
            self._mm = None
        self._pos += 4
        self._chronicle._set_appender_pos(self._tid, self._filenum, self._pos)
        self._start_pos = self._pos
        self._start_date = None


class VanillaChronicleWriter(VanillaChronicleReader):
    def __init__(self, base_dir, polling_interval=None,
                 max_mapped_memory=DEFAULT_MAX_MAPPED_MEMORY_PER_READER,
                 thread_id_bits=None, utcnow=datetime.datetime.utcnow):
        try:
            os.makedirs(base_dir)
        except FileExistsError:
            pass
        super().__init__(base_dir=base_dir, polling_interval=polling_interval,
                         max_mapped_memory=max_mapped_memory, thread_id_bits=thread_id_bits,
                         utcnow=utcnow)
        self._positions = dict()
        self._update_date_and_index_base(self._utcnow().date())
        todays_dir = os.path.join(self._base_dir, '%4d%02d%02d' % (self._date.year, self._date.month, self._date.day))
        if self._cycle_dir != todays_dir:
            self._cycle_dir = todays_dir
            try:
                os.makedirs(todays_dir)
            except FileExistsError:
                pass
        self.set_end_index_today()

    def _set_appender_pos(self, tid, filenum, pos):
        self._positions[tid] = (filenum, pos)

    # Returns whether rollover succeeded or not
    def _day_rollover(self, new_date):
        todays_dir = os.path.join(self._base_dir, '%4d%02d%02d'
                                  % (new_date.year, new_date.month, new_date.day))
        try:
            os.makedirs(todays_dir)
            ret = True
        except FileExistsError:
            # todo: wait here for rollover initiated by another to complete
            ret = False
        self.close()
        self._positions = dict()
        self._cycle_dir = todays_dir
        self._open_next_index()
        self._update_date_and_index_base(new_date)
        return ret

    def _set_index(self, tid, data_filenum, offset):
        assert self._date == self._utcnow().date()

        index_val = (tid << (64-self._thread_id_bits)) | (data_filenum << FILENUM_FROM_POS_SHIFT) | offset

        if self._index == 0:
            self.set_end_index_today()

        while True:
            index_filenum, index_offset = divmod(self._index*8, INDEX_FILE_SIZE)
            if len(self._index_mm) <= index_filenum:
                self._open_next_index()
            prev_index_val = _pychro.read_mmap(self._index_mm[index_filenum], index_offset)
            if prev_index_val != 0:
                self._index += 1
                continue
            if prev_index_val != _pychro.try_atomic_write_mmap(self._index_mm[index_filenum],
                                                               prev_index_val, index_val, index_offset):
                continue
            break

    def _get_tid(self):
        return _pychro.get_thread_id() & self._thread_id_mask
        # thread_id_bits not large enough? have to live with this..
        # assert get_thread_id() == tid

    def _open_next_index(self):
        file_num = len(self._index_fh)
        fn = os.path.join(self._cycle_dir, 'index-%s' % file_num)
        fh = open(fn, 'a+b')
        fh.truncate(INDEX_FILE_SIZE)
        fh.flush()
        self._index_fh += [fh]
        self._index_mm += [_pychro.open_write_mmap(fh, INDEX_FILE_SIZE)]

    def _open_data_file(self, filenum, thread):
        fn = os.path.join(self._cycle_dir, 'data-%s-%s' % (thread, filenum))
        fh = open(fn, 'a+b')
        fh.truncate(DATA_FILE_SIZE)
        return fh

    def _open_data_memory_map(self, filenum, thread):
        fh = self._data_fhs.get((filenum, thread))
        if not fh:
            fh = self._open_data_file(filenum, thread)
            self._data_fhs[(filenum, thread)] = fh

        while True:
            try:
                return mmap.mmap(fh.fileno(), 0, prot=mmap.PROT_READ | mmap.PROT_WRITE)
            except ValueError:
                pass
                # Alternative to this ugliness appears to be os.fsync(), but is much slower.
                # A thread lazily creating new data and index files would be a good optimisation.

    def __str__(self):
        return '<VanillaChronicleWriter dir:%s idx:%s tid:%s>' % (self._cycle_dir, self._index, self._get_tid())

    def get_appender(self):
        tid = self._get_tid()

        filenum_pos = self._positions.get(tid)
        if filenum_pos:
            filenum, pos = filenum_pos
        else:
            # search back looking for our thread id.
            # although the writer can write with different threads, those same threads should not be
            # writing to the same chronicle with a different writer.
            filenum = 0
            pos = 4
            while self._index > 0:
                # positions here are from the read perspective, need to advance to create
                # appender with write pos
                _filenum, _pos, _tid = self._prev_position_today()
                if _tid == tid:
                    _pos, mm = self.get_raw_bytes(_filenum, _pos, _tid)
                    pos = _pos + ~struct.unpack('i', mm[_pos-4:_pos])[0] + 4
                    filenum = _filenum
                    break
        return Appender(self, tid, filenum, pos, self._utcnow)



