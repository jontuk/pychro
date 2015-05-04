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

#include <windows.h>

extern "C" 
{

__declspec(dllexport) int get_thread_id() 
{
	return GetCurrentThreadId();
} 

__declspec(dllexport) int close_mmap(void *data, DWORD size) {
	return UnmapViewOfFile(data);
}

__declspec(dllexport) void *open_read_mmap(HANDLE fh, DWORD size) {
	HANDLE mh = CreateFileMapping(fh, NULL, PAGE_READONLY, 0, size, 0);
	if (mh == INVALID_HANDLE_VALUE || mh == 0)
		return (void*)-1;
	void *ret = MapViewOfFile(mh, FILE_READ_ACCESS, 0, 0, 0);
	if (ret == NULL)
		return (void*)-1;
	return ret;
}

__declspec(dllexport) void *open_write_mmap(HANDLE fh, DWORD size) {
	HANDLE mh = CreateFileMapping(fh, NULL, PAGE_READWRITE, 0, size, 0);
	if (mh == INVALID_HANDLE_VALUE || mh == 0)
		return (void*)-1;
	void *ret = MapViewOfFile(mh, FILE_MAP_ALL_ACCESS, 0, 0, 0);
	if (ret == NULL)
		return (void*)-1;
	return ret;
}

__declspec(dllexport) long long read_mmap(void *data, long long  offset) {
	return *(long long*)((unsigned char*)data+offset);
}

__declspec(dllexport) long long try_atomic_write_mmap(void *data, long long offset, long long prev, long long val) {

  long long *valp = (long long*)((unsigned char*)data+offset);
  return InterlockedCompareExchange64(valp, val, prev);
}

}