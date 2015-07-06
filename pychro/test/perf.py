#!/usr/bin/python3

import test_pychro

def main():
    N = 100000

    case = test_pychro.TestChronPerf()
    case.setUp()
    case.n = N
    case.test_perf_str()
    case.tearDown()

    case.setUp()
    case.n = N
    case.test_perf_int()
    case.tearDown()

    case.setUp()
    case.n = N
    case.test_perf_mixed()
    case.tearDown()

if __name__ == '__main__':
    main()