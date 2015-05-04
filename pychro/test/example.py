
import sys
import os
sys.path.append(os.path.split(os.path.dirname(__file__))[0])

import pychro
import tempfile

if '__main__' == __name__:
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

    read_chron = pychro.VanillaChronicleReader(tempdir)
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