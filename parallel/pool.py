from itertools import chain
from multiprocessing import pool


def mapstar(args):
    return tuple(map(*args))


def filterstar(args):
    return tuple(filter(*args))


class FPPool(pool.Pool):
    __slots__ = (
        '_pool',
        '_state',
        '_ctx',
        '_taskqueue',
        '_change_notifier',
        '_cache',
        '_maxtasksperchild',
        '_initializer',
        '_initargs',
        '_processes',
        '_worker_handler',
        '_task_handler',
        '_result_handler',
        '_terminate',
    )

    def map(self, func, iterable, chunksize=None):
        return self._map_filter_async(func, iterable, mapstar, False, chunksize).get()

    def filter(self, func, iterable, chunksize=None):
        return self._map_filter_async(func, iterable, filterstar, True, chunksize).get()

    def _map_filter_async(self, func, iterable, mapper, is_filter, chunksize=None, callback=None, error_callback=None):
        '''
        Helper function to implement map and filter.
        '''
        self._check_running()
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = FPPool._get_tasks(func, iterable, chunksize)
        result = MapFilterResult(self, chunksize, len(iterable), is_filter, callback, error_callback=error_callback)
        self._taskqueue.put(
            (
                self._guarded_task_generation(result._job,
                                              mapper,
                                              task_batches),
                None
            )
        )
        return result


class MapFilterResult(pool.MapResult):

    __slots__ = (
        '_pool',
        '_event',
        '_job',
        '_cache',
        '_callback',
        '_error_callback',
        '_success',
        '_value',
        '_chunksize',
        '_is_filter',
    )

    def __init__(self, pool_, chunksize, length, is_filter, callback, error_callback):
        pool.ApplyResult.__init__(self, pool_, callback, error_callback=error_callback)
        self._success = True
        self._chunksize = chunksize
        self._is_filter = is_filter
        if chunksize <= 0:
            self._number_left = 0
            self._event.set()
            del self._cache[self._job]
        else:
            self._number_left = length // chunksize + bool(length % chunksize)

        self._value = [None] * self._number_left

    def get(self, timeout=None):
        self.wait(timeout)
        if not self.ready():
            raise TimeoutError
        if self._success:
            if self._is_filter:
                return chain(*filter(None, self._value))
            else:
                return chain(*self._value)
        else:
            raise self._value

    def _set(self, i, success_result):
        self._number_left -= 1
        success, result = success_result
        if success and self._success:
            self._value[i] = result
            if self._number_left == 0:
                if self._callback:
                    self._callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None
        else:
            if not success and self._success:
                # only store first exception
                self._success = False
                self._value = result
            if self._number_left == 0:
                # only consider the result ready once all jobs are done
                if self._error_callback:
                    self._error_callback(self._value)
                del self._cache[self._job]
                self._event.set()
                self._pool = None
