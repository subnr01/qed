**A concurrent queue implementation for buffering data between stages in pipeline-parallel applications.**



# Introduction #

QED is a non-blocking circular-array based concurrent queue implementation which adjusts its capacity at run-time. The intended usage case of QED is buffering data between stages in pipeline-parallel applications. There is a trade-off between degree of parallelism and inter-stage buffer space: buffers too small cannot accommodate the run-time variance of applications, while buffers too large incur cache or TLB misses. QED automatically adjusts its capacity at run-time so that parallelism can be maximized while keeping memory foot-print minimal.

There are other ways to find a sweet spot of the trade-offs. First, we could search the smallest static queue capacities that maximizes the execution time for given training input data. However, this requires potentially long search time and need for finding a representative training data set. Second, we could use linked-list based concurrent queues such as Michael and Scott's queue. However, they have overhead associated with dynamic memory allocation.

QED maintains the efficiency of circular-array based queues but also automatically adjusts its capacity at run-time so that programmer doesn't need to worry about queue sizing.

# Two Phase Interface #

As opposed to conventional interface where an enqueue or a dequeue is done in a single function call, QED requires two phase (reserve and commit) interface for both enqueue and dequeue. The following code shows an example

```
// In producer thread
Element *out;
while (!q->reserveEnqueue(&out));
... // writes to out
q->commitEnqueue(out);

// In consumer thread
Element *in;
while (!q->reserveDequeue(&in));
... // reads from in
q->commitDequeue(in);
```

When reserveEnqueue returns true, it is safe to modify reserved element. After finishing the modification, we call commitEnqueue so that the reserved location can be released. The motivation for having this two phase interface is to do in-place computation instead of copying a large chunk of data or passing pointer after dynamic memory allocation. Suppose that we don't have such two phase interface, then one option would be copying elements to or from the queue as follows:

```
// In producer thread
Element out; // assume Element is large
... // writes to out
while (q->isFull());
q->enqueue(out); // WARNING: memcpy of large data!!!
```

The other option would be passing pointer after dynamic memory allocation as follows:

```
// In producer thread
Element *out = new Element(); // WARNING: dynamic memory allocation!!!
... // writes to out
while (q->isFull());
q->enqueue(out);

// In consumer thread
while (q->isEmpty());
Element *in = q->dequeue();
... // reads from in
delete in; // WARNING: dynamic memory deallocation!!!
```

As you have seen, the both approach may involve an excessive overhead, which motivates our two phase interface.

# Capacity Adjustment #

The capacity is adjusted by the time variance of the _occupancy_, where the _occupancy_ is defined as the number of tokens present in the queue. Since measuring the accurate time variance can incur significant overhead, we approximate the variance by the difference between the maximum and minimum occupancy during the last _epoch_, where _epochs_ are defined as times between consecutive tail index wrap-arounds.

# Non-blocking #

A common strategy to implement a non-blocking data structure is that 1) copy the current state to local store, 2) modify the current state within the local store, and 3) try to modify the glocal state using compare and swap. Succeeding the third step (compare-and-swap, or in short cas) means that no body has interfered during the modification (ignoring ABA problem. If you are not familiar with ABA problem, please refer to "The Art of Multiprocessor Programming").

The challenge is that how to compact all the stages into 64-bit, which is the maximum size that the current x86 architecture supports compare-and-swap instructions. We restrict ourselves that the capacity must be a power of 2. This allows us to not only efficiently wrap-around indices by using bit-wise and operations but also compress the capacity in its binary logarithm form. We pack logical tail index (a 32-bit sequence number), physical tail index (an 24-bit index to the physical circular array), and 8-bit capacity in its binary logarithm form into a 64-bit word. The packed 64-bit word is represented by a union, PackedIndexAndC, which provides a 64-bit union field for atomic 64-bit compare-and-swap.

# Assumptions #

The current QED implementation has the following assumptions, but later QED definitely can be implemented more platform independent way.

  * Architecture: 64-bit x86
  * Compiler: g++