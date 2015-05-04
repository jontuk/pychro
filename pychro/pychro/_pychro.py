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
   
import ctypes
import os
import pychro


class PychroCError(Exception):
    pass


if pychro.PLATFORM_WINDOWS:
    import msvcrt
    cdll = ctypes.cdll.LoadLibrary(os.path.join(os.path.dirname(__file__), 'PychroCLib.dll'))
    cdll.open_read_mmap.argtypes = [ctypes.c_int, ctypes.c_int32]
    cdll.open_write_mmap.argtypes = [ctypes.c_int, ctypes.c_int32]
    cdll.read_mmap.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ]
    cdll.close_mmap.argtypes = [ctypes.c_void_p, ctypes.c_int32]
    cdll.try_atomic_write_mmap.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_longlong]
else:
    cdll = ctypes.cdll.LoadLibrary(os.path.join(os.path.dirname(__file__), 'libpychroc.so'))
    cdll.open_read_mmap.argtypes = [ctypes.c_int, ctypes.c_size_t]
    cdll.open_write_mmap.argtypes = [ctypes.c_int, ctypes.c_size_t]
    cdll.read_mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    cdll.close_mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    cdll.try_atomic_write_mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong, ctypes.c_longlong]

cdll.get_thread_id.restype = ctypes.c_int
cdll.open_read_mmap.restype = ctypes.c_void_p
cdll.open_write_mmap.restype = ctypes.c_void_p
cdll.close_mmap.restype = ctypes.c_int
cdll.read_mmap.restype = ctypes.c_longlong
cdll.try_atomic_write_mmap.restype = ctypes.c_longlong


def get_thread_id():
    return cdll.get_thread_id()


def open_read_mmap(fh, size):
    fh.flush()
    if pychro.PLATFORM_WINDOWS:
        fileno = msvcrt.get_osfhandle(fh.fileno())
    else:
        fileno = fh.fileno()
    res = cdll.open_read_mmap(fileno, size)
    if res == 0xffffffffffffffff:
        raise PychroCError
    return res

def open_write_mmap(fh, size):
    fh.flush()
    if pychro.PLATFORM_WINDOWS:
        fileno = msvcrt.get_osfhandle(fh.fileno())
    else:
        fileno = fh.fileno()
    res = cdll.open_write_mmap(fileno, size)
    if res == 0xffffffffffffffff:
        raise PychroCError
    return res


def close_mmap(mh, size):
    if cdll.close_mmap(mh, size) == -1:
        raise PychroCError


def read_mmap(mh, offset):
    return cdll.read_mmap(mh, offset)


def try_atomic_write_mmap(mh, offset, prev, val):
    return cdll.try_atomic_write_mmap(mh, offset, prev, val)


def unsafe_write_mmap(mh, offset, val):
    cdll.try_atomic_write_mmap(mh, offset, cdll.read_mmap(mh, offset), val)