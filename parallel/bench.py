from math import sqrt
from multiprocessing import Pool
from timeit import Timer

from .parallel_collections import parallel


def get_divisors_sum(n):
    res = 1
    for i in range(2, int(sqrt(n)) + 1):
        if not n % i:
            res += i
            j = n // i
            if j > i:
                res += j

    return res


def is_prime(m):
    if m == 1:
        return False
    for i in range(2, int(sqrt(m)) + 1):
        if not m % i:
            return False
    return True


if __name__ == '__main__':

    def test_multiprocessing_pool(n):
        with Pool(processes=4) as pool:
            return pool.map(get_divisors_sum, [1000_000 for _ in range(n)])

    def test_parallel_map(n):
        *l, = parallel([1000_000 for _ in range(n)], pool_size=4).map(get_divisors_sum)
        return l

    def test_map(n):
        *l, = map(get_divisors_sum, [1000_000 for _ in range(n)])
        return l

    def test_parallel_filter(n):
        *l, = parallel([1000_000 for _ in range(n)], pool_size=4).filter(is_prime)
        return l

    def test_filter(n):
        *l, = filter(is_prime, [1000_000 for _ in range(n)])
        return l

    # equality check
    a = 1000
    assert test_multiprocessing_pool(a) == test_parallel_map(a) == test_map(a)
    assert test_parallel_filter(a) == test_filter(a)

    # performance check
    loops = 10

    for b in (1, 100, 10_000, 1000_000):
        print(f'================================= start n={b} =================================')

        test_multiprocessing_pool_timer = Timer(lambda: test_multiprocessing_pool(b))
        test_parallel_map_timer = Timer(lambda: test_parallel_map(b))
        test_map_timer = Timer(lambda: test_map(b))

        print('test_parallel_map: ', test_parallel_map_timer.timeit(loops) / loops)
        print('test_multiprocessing_pool: ', test_multiprocessing_pool_timer.timeit(loops) / loops)
        print('test_map: ', test_map_timer.timeit(loops) / loops)

        print()

        test_parallel_filter_timer = Timer(lambda: test_parallel_filter(b))
        test_filter_timer = Timer(lambda: test_filter(b))

        print('test_filter: ', test_filter_timer.timeit(loops) / loops)
        print('test_parallel_filter: ', test_parallel_filter_timer.timeit(loops) / loops)

        print(f'================================= end n={b} =================================')
        print('\n\n')