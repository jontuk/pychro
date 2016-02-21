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

from . import pychroc


class PychroCError(Exception):
    pass


def get_thread_id():
    return pychroc.get_thread_id()


def open_read_mmap(fh, size):
    fh.flush()
    fileno = fh.fileno()
    res = pychroc.open_read_mmap(fileno, size)
    if res == 0xffffffffffffffff:
        raise PychroCError
    return res


def open_write_mmap(fh, size):
    fh.flush()
    fileno = fh.fileno()
    res = pychroc.open_write_mmap(fileno, size)
    if res == 0xffffffffffffffff:
        raise PychroCError
    return res


def close_mmap(mh, size):
    if pychroc.close_mmap(mh, size) == -1:
        raise PychroCError


def read_mmap(mh, offset):
    return pychroc.read_mmap(mh, offset)


def try_atomic_write_mmap(mh, prev, val, offset):
    return pychroc.try_atomic_write_mmap(mh, prev, val, offset)


def unsafe_write_mmap(mh, val, offset):
    pychroc.try_atomic_write_mmap(mh, pychroc.read_mmap(mh, offset), val, offset)
