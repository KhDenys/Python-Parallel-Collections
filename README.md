###Python Parallel Collections
####Implementations of dict and list which support parallel map/reduce style operations

####Who said Python was not setup for multicore computing? 
In this package you'll find very simple parallel implementations of list and dict. The parallelism uses the [Python 2.7 backport](http://pythonhosted.org/futures/#processpoolexecutor-example) of the [concurrent.futures](http://docs.python.org/dev/library/concurrent.futures.html) package. If you can define your problem in terms of map/reduce/filter/flatten operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 
Otherwise these datastructures are equivalent to the non-parallel ones found in the standard library.


####Examples

```python
>>> def double(i):
...     return i*2
... 
>>> list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> flat_list = list_of_list.flatten()
[1, 2, 3, 4, 5, 6]
>>> list_of_list
[[1, 2, 3], [4, 5, 6]]
>>> flat_list.map(double)
[2, 4, 6, 8, 10, 12]
>>> list_of_list.flatmap(double)
[2, 4, 6, 8, 10, 12]
```

As you see every method call returns a new collection, instead of changing the current one.
The exception is the foreach method, which is equivalent to map but instead of returning a new collection it operates directly on the 
current one and returns `None`.  
```python
>>> flat_list
[1, 2, 3, 4, 5, 6]
>>> flat_list.foreach(double)
None
>>> flat_list
[2, 4, 6, 8, 10, 12]
```

Since every operation (except foreach) returns a collection, these can be chained.
```python
>>> list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(double).map(str)
['2', '4', '6', '8', '10', '12']
```

####Regarding lambdas and closures
Sadly lambdas, closures and partial functions cannot be passed around multiple processes, so every function that you pass to the collection methods needs to be defined using the def statement. If you want the operation to carry extra state, use a class with a `__call__` method defined.
```python
>>> class multiply(object):
...     def __init__(self, factor):
...         self.factor = factor
...     def __call__(self, item):
...         return item * self.factor
... 
>>> multiply(2)(3)
6
>>>list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(multiply(2))
[2, 4, 6, 8, 10, 12]
```

###Quick example of flatmap and filter for both collections

####FlatMap

Functions passed to the flatmap method of a list will be passed every element in the list and should return a single element. For a dict, the function will receive a tuple (key, values) for every key in the dict, and should equally return a two element sequence.
 
```python
>>>def double(item):
...    return item * 2
...
>>> list_of_list =  ParallelList([[1,2,3],[4,5,6]])
>>> list_of_list.flatmap(double).map(str)
['2', '4', '6', '8', '10', '12']
>>> def double_dict(item):
...     k,v = item
...     try:
...         return [k, [i *2 for i in v]]
...     except TypeError:
...         return [k, v * 2]
... 
>>> d = ParallelDict(zip(range(2), [[[1,2],[3,4]],[3,4]]))
>>> d
{0: [[1, 2], [3, 4]], 1: [3, 4]}
>>> flat_mapped = d.flatmap(double_dict)
>>> flat_mapped
{0: [2, 4, 6, 8], 1: [6, 8]}
```

####Filter
The Filter method should be passed a predicate, which means a function that will return True or False and will be called once for every element in the list and for every (key, values) in a dict.
```python
>>> def is_digit(item):
...     return item.isdigit()
...
>>> p = ParallelList(['a','2','3'])
>>> pred = is_digit
>>> filtered = p.filter(pred)
>>> filtered
['2', '3']

>>>def is_digit_dict(item):
...    return item[1].isdigit()
...
>>>p = ParallelDict(zip(range(3), ['a','2', '3',]))
>>>p
{0: 'a', 1: '2', 2: '3'}
>>>pred = is_digit_dict
>>>filtered = p.filter(pred)
>>>filtered
{1: '2', 2: '3'}
```