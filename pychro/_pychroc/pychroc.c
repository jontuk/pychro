/*
 *
 *  Copyright 2015 Jon Turner
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <Python.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>

static PyObject *
get_thread_id(PyObject *self, PyObject *args) {
    return PyLong_FromLong(syscall(SYS_gettid));
}

static PyObject *
open_write_mmap(PyObject *self, PyObject *args) {
    int fh;
    unsigned int size;
    PyArg_ParseTuple(args, "iI", &fh, &size);
    return PyLong_FromVoidPtr(mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fh, 0));
}

static PyObject *
open_read_mmap(PyObject *self, PyObject *args) {
    int fh;
    unsigned int size;
    PyArg_ParseTuple(args, "iI", &fh, &size);
    return PyLong_FromVoidPtr(mmap(NULL, size, PROT_READ, MAP_SHARED, fh, 0));
}

static PyObject *
close_mmap(PyObject *self, PyObject *args) {
    void *data;
    unsigned int size;
    PyArg_ParseTuple(args, "KI", &data, &size);
    return PyLong_FromLong(munmap(data, size));
}

static PyObject *
read_mmap(PyObject *self, PyObject *args) {
    void* data;
    unsigned int offset;
    PyArg_ParseTuple(args, "KI", &data, &offset);
    return PyLong_FromUnsignedLongLong(*(unsigned long long*)((unsigned char*)data+offset));
}

static PyObject *
try_atomic_write_mmap(PyObject *self, PyObject *args) {
    void *data;
    unsigned long long prev;
    unsigned long long val;
    unsigned int offset;
    PyArg_ParseTuple(args, "KKKI", &data,  &prev, &val, &offset);
    unsigned long long *valp = (unsigned long long*)((unsigned char*)data+offset);
    return PyLong_FromUnsignedLongLong(__sync_val_compare_and_swap(valp, prev, val));
}

static PyMethodDef Methods[] = {
    {"get_thread_id", get_thread_id, METH_NOARGS, NULL },
    {"open_write_mmap", open_write_mmap, METH_VARARGS, NULL },
    {"open_read_mmap", open_read_mmap, METH_VARARGS, NULL },
    {"close_mmap", close_mmap, METH_VARARGS, NULL },
    {"read_mmap", read_mmap, METH_VARARGS, NULL },
    {"try_atomic_write_mmap", try_atomic_write_mmap, METH_VARARGS, NULL },
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
   PyModuleDef_HEAD_INIT,
   "pychroc",
   NULL,
   -1,
   Methods
};

PyMODINIT_FUNC
PyInit_pychroc(void)
{
    return PyModule_Create(&module);
}