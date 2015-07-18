
OPTS = -O3

all : libpychroc.so
	
clean : 
	rm libpychroc.so; rm libpychroc.o; echo Cleaned

libpychroc.so : libpychroc.o
	clang $(OPTS) -Wall -shared -fPIC libpychroc.o -o libpychroc.so
	cp libpychroc.so ../pychro/pychro/libpychroc_darwin.so
	
libpychroc.o :
	clang $(OPTS) -Wall -fPIC -c libpychroc.cpp -o libpychroc.o


	
	
		
