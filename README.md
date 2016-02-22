# Pychro

### Memory-mapped message journal in Python

This project is hosted at <https://github.com/jontuk/pychro>

### Introduction

Pychro is a Python library for accessing 'Chronicle' message journals created by the OpenHFT Chronicle-Queue Java project 
(https://github.com/OpenHFT/Chronicle-Queue). Chronicle queues are persistent, support concurrent access by multiple
 reading/writing processes and high performance as use memory mapped files for IPC.

### Current Status

- Vanilla Chronicle reading/writing
- Tested with CPython 3.4/5 on 64bit Linux
- Primitive and unicode string fields
- OpenHFT Chronicle-Queue default settings
- Subscribe to remote Chronicles (e.g. served by a Java app)

### Usage

Each message has an index, which encodes the date, and message number on that date.
Messages are simply the in-order sequence of fields binary serialised, so to understand a particular message
the reader must know how it is constructed. 

#### Writing

Write 3 messages, with 3 fields each - an int, string and double. The message is atomically commited to the Chronicle
index by calling appender.finish().

    import tempdir
    import pychro

    tempdir = tempfile.mkdtemp()
    print('Chronicle location: %s' % tempdir)

    write_chron = pychro.VanillaChronicleWriter(tempdir)
    appender = write_chron.get_appender()
    for i in range(1, 4):
        appender.write_int(i)
        appender.write_string('1/%s=%s' % (i, 1/i))
        appender.write_double(1/i)
        appender.finish()
    write_chron.close()
    
#### Reading

This example will begin reading at the earliest message it can (no date or index is provided to VanillaChronicleReader()
and since no polling_interval has been provided either will throw pychro.NoData once it reaches the end.

It expects messages formed of an int, string and double.

    read_chron = pychro.VanillaChronicleReader(chron_dir)
    while True:
        print('Next read index:%s' % read_chron.get_index())
        try:
            reader = read_chron.next_reader()
        except pychro.NoData:
            break
        print(reader.read_int())
        print(reader.read_string())
        print(reader.read_double())
    read_chron.close()



### Deficiencies

This level of functionality and performance serves me well in a number of projects. However there are a number of
 things which should be improved, including:
 
- More extensive coverage of the OpenHFT Chronicle functionality
- Support non-default settings (e.g. assumes daily cycles)
- Support other platforms, e.g. Windows or non-CPython

Enjoy!

Jon
