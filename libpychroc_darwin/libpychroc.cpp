/*
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
 */

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>

extern "C"
{

int get_thread_id() {
   //return (int)(size_t)pthread_self();   
   return syscall(SYS_thread_selfid);
}

int close_mmap(void *data, size_t size) {
  int ret = munmap(data, size);
  return ret;
}


void *open_read_mmap(int fh, size_t size) {
  return mmap(NULL, size, PROT_READ, MAP_SHARED, fh, 0);
}


void *open_write_mmap(int fh, size_t size) {
  return mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fh, 0);
}


long long read_mmap(void *data, size_t offset) {
  return *(long long*)((unsigned char*)data+offset);
}

long long try_atomic_write_mmap(void *data, size_t offset, long long prev, long long val) {

  long long *valp = (long long*)((unsigned char*)data+offset);
  return __sync_val_compare_and_swap(valp, prev, val);
}

}

