/*
 * qed_base.h
 *
 * Queue Enchaned With Dynamic sizing base classes.
 *
 *  Created on: Nov 29, 2010
 *      Author: jongsoo
 */

#ifndef _QED_BASE_H_
#define _QED_BASE_H_

#include <cassert>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace qed {

/**
 * malloc at cache line boundaries.
 */
template<class T>
T * const alignedMalloc(size_t n) {
  long addr = (long)malloc((n + 64)*sizeof(T));
  char *ptr = (char *)(((addr + 63) >> 5) << 5);
  return (T *)ptr;
}

/**
 * calloc at cache line boundaries.
 */
template<class T>
T * const alignedCalloc(size_t n) {
  long addr = (long)calloc((n + 64), sizeof(T));
  char *ptr = (char *)(((addr + 63) >> 5) << 5);
  return (T *)ptr;
}

/**
 * The size of buffer that holds traces.
 * When we have more traces than this number, old traces will be overwritten.
 */
static const int TRACE_LENGTH = 65536*16;

/**
 * Read TSC (Time Stamp Counter) hardware performance counter
 */
static inline unsigned long long int readTsc(void)
{
  unsigned a, d;

  __asm__ volatile("rdtsc" : "=a" (a), "=d" (d));

  return ((unsigned long long)a) | (((unsigned long long)d) << 32);
}

/**
 * EventId used in our trace functions
 */
enum EventId {
  SET_CAPACITY = 0,
  RESERVE_ENQUEUE,
  RESERVE_DEQUEUE,
  COMMIT_ENQUEUE,
  COMMIT_DEQUEUE,
  FULL,
  EMPTY,
  SET_CAPACITY2,
};

/**
 * A trace record
 */
struct Trace {
  unsigned long long tsc;
  EventId id;
  int value;
};

/**
 * @return true if i is a power of two
 */
static inline bool is2ToN(int i) {
  return ((i - 1)&i) == 0;
}

template<class T>
class TraceableQ {
public :
  TraceableQ() {
#if QED_TRACE_LEVEL > 0
    memset(traces, 0, sizeof(traces));
    traceIndex = 0;
    lastTsc = 0;
#endif
#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
    isSpinningEmpty = false;
#endif
  }

  typedef T tokenType;

  size_t getSizeOfToken() {
    return sizeof(T);
  }

  /**
   * Dump traces to stdout.
   */
  void dumpToCout() __attribute__((noinline)) {
    dump(std::cout);
  }

  /**
   * Dump traces to stdout in a human friendly format.
   */
  void dumpToCoutHumanFriendy() __attribute__((noinline)) {
    dumpHumanFriendly(std::cout);
  }

#if QED_TRACE_LEVEL > 0
  void trace(EventId id, int value) {
    int i = __sync_fetch_and_add(&traceIndex, 1);
    Trace *t = traces + (i&(TRACE_LENGTH - 1));
    t->tsc = readTsc();
    t->id = id;
    t->value = value;
  }

  void traceResizing(size_t newCapacity) {
    trace(SET_CAPACITY, newCapacity);
  }

  void traceResizing2(size_t newCapacity) {
    trace(SET_CAPACITY2, newCapacity);
  }

  void dump(std::ostream& out) __attribute__((noinline)) {
    if (traceIndex >= TRACE_LENGTH) {
      for (int i = traceIndex - TRACE_LENGTH; i < traceIndex; i++) {
        Trace *t = traces + (i&(TRACE_LENGTH - 1));
        out << t->tsc << " " << t->id << " " << t->value << std::endl;
      }
    }
    else {
      for (int i = 0; i < traceIndex; i++) {
        Trace *t = traces + i;
        out << t->tsc << " " << t->id << " " << t->value << std::endl;
      }
    }
  }

  void dumpHumanFriendly(std::ostream& out) __attribute__((noinline)) {
    int begin = 0, end = traceIndex;
    if (traceIndex >= TRACE_LENGTH) {
      begin = traceIndex - TRACE_LENGTH;
    }
    for (int i = begin; i < end; i++) {
      Trace *t = traces + (i&(TRACE_LENGTH - 1));
      out << (t->tsc - traces[begin].tsc)/2.27/1000000000 << " ";
      switch (t->id) {
      case SET_CAPACITY: 
        out << "setCapacity";
        break;
      case RESERVE_ENQUEUE :
        out << "reserveEnqueue";
        break;
      case RESERVE_DEQUEUE :
        out << "reserveDequeue";
        break;
      case COMMIT_ENQUEUE :
        out << "commitEnqueue";
        break;
      case COMMIT_DEQUEUE :
        out << "commitDequeue";
        break;
      case FULL :
        out << "full";
        break;
      case EMPTY :
        out << "empty";
        break;
      case SET_CAPACITY2 :
        out << "setCapacity2";
        break;
      }
      
      out << " " << t->value << std::endl;
    }
  }

  double getCapacityAverage() {
    double sum = 0;
    unsigned long long firstTsc = 0;
    unsigned long long lastTsc = 0;
    int lastC = -1;
    for (int i = 0; i < traceIndex; i++) {
      if (traces[i].id == SET_CAPACITY && traces[i].tsc > lastTsc) {
        if (lastC >= 0) {
          sum += (traces[i].tsc - lastTsc)*lastC;
          //printf("traces[i].tsc = %lld, lastTsc = %lld, lastC = %d, sum = %f\n", traces[i].tsc, lastTsc, lastC, sum);
        }
        else {
          firstTsc = traces[i].tsc;
        }
        lastTsc = traces[i].tsc;
        lastC = traces[i].value;
      }
    }
    if (TraceableQ<T>::lastTsc > lastTsc) {
      sum += (TraceableQ<T>::lastTsc - lastTsc)*lastC;
      //printf("TraceableQ<T>::lastTsc = %lld, lastTsc = %lld, lastC = %d, sum = %f\n", TraceableQ<T>::lastTsc, lastTsc, lastC, sum);
      lastTsc = TraceableQ<T>::lastTsc;
    }
    //printf("lastTsc = %lld, firstTsc = %lld\n", lastTsc, firstTsc);
    return sum/(lastTsc - firstTsc);
  }
#else
  void traceResizing(size_t newCapacity) { }

  void traceResizing2(size_t newCapacity) { }

  void trace(const char *fmt, ...) { }

  void dump(std::ostream& out) { }

  void dumpHumanFriendly(std::ostream& out) { }
#endif

#if QED_TRACE_LEVEL >= 2
  void traceReserveEnqueue(int tail) {
    trace(RESERVE_ENQUEUE, tail);
  }

  void traceReserveDequeue(int head) {
    trace(RESERVE_DEQUEUE, head);
  }

  void traceCommitEnqueue(int tail) {
    trace(COMMIT_ENQUEUE, tail);
  }

  void traceCommitDequeue(int head) {
    trace(COMMIT_DEQUEUE, head);
  }

  void traceFull() {
    if (!isSpinningFull) {
      isSpinningFull = true;
      trace(FULL, 0);
    }
  }

  void traceEmpty() {
    if (!isSpinningEmpty) {
      isSpinningEmpty = true;
      trace(EMPTY, 0);
    }
  }

  void traceFull(bool isFull) {
    if (isFull) {
      traceFull();
    }
    else {
      isSpinningFull = false;
    }
  }

  void traceEmpty(bool isEmpty) {
    if (isEmpty) {
      traceEmpty();
    }
    else {
      isSpinningEmpty = false;
    }
  }
#else
  void traceReserveEnqueue(int tail) { }
  void traceReserveDequeue(int head) { }
  void traceCommitEnqueue(int tail) { }
  void traceCommitDequeue(int head) { }
  void traceFull() { };
  void traceEmpty() { };
  void traceFull(bool isFull) { };
  void traceEmpty(bool isEmpty) { };
#endif

protected:
#if QED_TRACE_LEVEL > 0
  Trace traces[TRACE_LENGTH];
  volatile int traceIndex;
  volatile unsigned long long lastTsc;
#endif
#if QED_TRACE_LEVEL >= 2
  volatile bool isSpinningFull, isSpinningEmpty;
#endif
};

/**
 * The class at the very top of our queue class hierarchy.
 */
template<class T>
class BaseQ : public TraceableQ<T> {
public :

  /**
   * @param N capacity of the queue. Must be a power of two
   */
  BaseQ(size_t N) : N(N), buf(alignedMalloc<T>(N)) {
    assert(is2ToN(N)); // N should be a power of two.
  }

  /**
   * @return the pointer to the circular array
   */
  T * const getBuf() {
    return buf;
  }

  size_t getCapacity() const {
    return N;
  }

protected :
  const size_t N;
  T * const buf __attribute__((aligned (64)));
};

#define QED_USING_BASEQ_MEMBERS_ \
  using BaseQ<T>::N; \
  using TraceableQ<T>::traceReserveEnqueue; \
  using TraceableQ<T>::traceReserveDequeue; \
  using TraceableQ<T>::traceCommitEnqueue; \
  using TraceableQ<T>::traceCommitDequeue; \
  using TraceableQ<T>::traceFull; \
  using TraceableQ<T>::traceEmpty; \
  int getHeadIndex() const { \
    return headIndex; \
  } \
  int getTailIndex() const { \
    return tailIndex; \
  } \
  int size() const { \
    return getTailIndex() - getHeadIndex(); \
  }

#if QED_TRACE_LEVEL >= 2
#define QED_USING_BASEQ_MEMBERS \
  QED_USING_BASEQ_MEMBERS_ \
  using TraceableQ<T>::isSpinningFull; \
  using TraceableQ<T>::isSpinningEmpty;
#else  
#define QED_USING_BASEQ_MEMBERS QED_USING_BASEQ_MEMBERS_
#endif

} // namespace qed

#endif // _QED_BASE_H_
