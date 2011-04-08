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
#include <algorithm>
#include <pthread.h>

//#define QED_USE_CV

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
static const int TRACE_LENGTH = 65536*64;

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
#ifdef QED_USE_CV
    pthread_mutex_init(&empty_lock_, NULL);
    pthread_mutex_init(&full_lock_, NULL);
    pthread_cond_init(&not_empty_cv_, NULL);
    pthread_cond_init(&not_full_cv_, NULL);
#endif
  }

#ifdef QED_USE_CV
  virtual ~TraceableQ() {
    pthread_mutex_destroy(&empty_lock_);
    pthread_mutex_destroy(&full_lock_);
    pthread_cond_destroy(&not_empty_cv_);
    pthread_cond_destroy(&not_full_cv_);
  }
#endif

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

  void writeOccupancies(std::ostream& out) __attribute__((noinline)) {
    int occ = 0;
    int lastOcc = -1;
    for (int i = 0; i < traceIndex; i++) {
      if (traces[i].id == RESERVE_ENQUEUE) {
        occ++;
        if (occ != lastOcc) {
          out <<
            (traces[i].tsc - traces[0].tsc)/2.27/1000000000 << ' ' << occ <<
            std::endl;
          lastOcc = occ;
        }
      }
      else if (traces[i].id == COMMIT_DEQUEUE) {
        occ--;
      }
    }
  }

  int writeOptimalCapacities(std::ostream& out) __attribute__((noinline)) {
    unsigned long long *resEnqTimes = new unsigned long long[TRACE_LENGTH];
      // timeSeqId -> resEnqTime
    unsigned long long *comEnqTimes = new unsigned long long[TRACE_LENGTH];
      // logSeqId -> comEnqTime
    unsigned long long *resDeqTimes = new unsigned long long[TRACE_LENGTH];
      // timeSeqId -> resDeqTime
    unsigned long long *comDeqTimes = new unsigned long long[TRACE_LENGTH];
      // timeSeqId -> comDeqTime

    // timeSeqId -> logicalSeqId
    int *enqIds = new int[TRACE_LENGTH];
    int *deqIds = new int[TRACE_LENGTH];

    // 1. Initialize arrays.
    int enqCnt = 0, deqCnt = 0, comDeqCnt = 0;
    for (int i = 0; i < traceIndex; i++) {
      if (traces[i].id == RESERVE_ENQUEUE) {
        resEnqTimes[enqCnt] = traces[i].tsc;
        enqIds[enqCnt] = traces[i].value;
        enqCnt++;
      }
      else if (traces[i].id == COMMIT_ENQUEUE) {
        comEnqTimes[traces[i].value] = traces[i].tsc;
      }
      else if (traces[i].id == RESERVE_DEQUEUE) {
        resDeqTimes[deqCnt] = traces[i].tsc;
        deqIds[deqCnt] = traces[i].value;
        deqCnt++;
      }
      else if (traces[i].id == COMMIT_DEQUEUE) {
        comDeqTimes[comDeqCnt] = traces[i].tsc;
        comDeqCnt++;
      }
    }

    // 2. Compute optimal reserveEnqueue times.
    int cnt = std::min(enqCnt, deqCnt);
    unsigned long long *optTimes = new unsigned long long[cnt + 1];
      // optimal reserveEnqueue times
    resEnqTimes[cnt] = resEnqTimes[cnt - 1]; // boundary value
    resDeqTimes[cnt] = resDeqTimes[cnt - 1]; // boundary value
    optTimes[cnt] = ULLONG_MAX; // boundary value
    for (int i = cnt - 1; i >= 0; i--) { // Moving backward.
      int nextEnq = cnt, nextDeq = cnt;
      for (int j = i; j < cnt && (nextEnq == cnt || nextDeq == cnt); j++) {
        if (enqIds[j] > enqIds[i] && nextEnq == cnt) {
          nextEnq = j;
        }
        if (deqIds[j] == enqIds[i] && nextDeq == cnt) {
          nextDeq = j;
        }
      }

      optTimes[i] = std::min(
        optTimes[nextEnq] - (resEnqTimes[nextEnq] - resEnqTimes[i]),
        resDeqTimes[nextDeq] - (comEnqTimes[enqIds[i]] - resEnqTimes[i]));
    }

    // 3. Sort optimal reserveEnqueue times.
    std::sort(optTimes, optTimes + cnt);

    // 4. Compute optimal capacities.
    int lastOpt = -1;
    int *hist = new int[1048576];
    for (int i = 0; i < 1048576; i++) {
      hist[i] = 0;
    }
    for (int i = 0; i < cnt; i++) {
      int opt = 1;
      for (int j = i - 1; j >= 0; j--) {
        if (comDeqTimes[j] < optTimes[i]) {
          opt = i - j;
          break;
        }
      }
      hist[opt]++;
      if (optTimes[i] >= traces[0].tsc && (opt != lastOpt || i == cnt - 1)) {
        out <<
          (optTimes[i] - traces[0].tsc)/2.27/1000000000 << ' ' <<
          opt << std::endl;
        lastOpt = opt;
      }
    }

    delete[] resEnqTimes;
    delete[] comEnqTimes;
    delete[] resDeqTimes;
    delete[] comDeqTimes;
    delete[] enqIds;
    delete[] deqIds;
    delete[] optTimes;

    int sum = hist[0];
    int i;
    for (i = 1; i < 1048576; i *= 2) {
      for (int j = i; j < i*2; j++) {
        sum += hist[j];
      }
      if (sum*10 >= cnt*9) {
        break;
      }
    }

    delete[] hist;
    return i*2;
  }

  void printTraceIndex() {
    std::cout << traceIndex << std::endl;
    std::cout << traces[traceIndex].id << " " << traces[traceIndex].value << std::endl;
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
  void writeOccupancies(std::ostream& out) { };
  void writeOptimalCapacities(std::ostream& out) { };
#endif

#ifdef QED_USE_CV
  void waitNotEmpty() {
    pthread_mutex_lock(&empty_lock_);
    pthread_cond_wait(&not_empty_cv_, &empty_lock_);
    pthread_mutex_unlock(&empty_lock_);
  }

  void waitNotFull() {
    pthread_mutex_lock(&full_lock_);
    pthread_cond_wait(&not_full_cv_, &full_lock_);
    pthread_mutex_unlock(&full_lock_);
  }

  void signalNotEmpty() {
    pthread_mutex_lock(&empty_lock_);
    pthread_cond_signal(&not_empty_cv_);
    pthread_mutex_unlock(&empty_lock_);
  }

  void signalNotFull() {
    pthread_mutex_lock(&full_lock_);
    pthread_cond_signal(&not_full_cv_);
    pthread_mutex_unlock(&full_lock_);
  }

  void broadCastNotEmpty() {
    pthread_mutex_lock(&empty_lock_);
    pthread_cond_broadcast(&not_empty_cv_);
    pthread_mutex_unlock(&empty_lock_);
  }

  void broadCastNotFull() {
    pthread_mutex_lock(&full_lock_);
    pthread_cond_broadcast(&not_full_cv_);
    pthread_mutex_unlock(&full_lock_);
  }
#else
  void waitNotEmpty() { }
  void waitNotFull() { }
  void signalNotEmpty() { }
  void signalNotFull() { }
  void broadCastNotEmpty() { }
  void broadCastNotFull() { }
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
#ifdef QED_USE_CV
  pthread_mutex_t empty_lock_, full_lock_;
  pthread_cond_t not_empty_cv_, not_full_cv_;
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
  using TraceableQ<T>::waitNotEmpty; \
  using TraceableQ<T>::waitNotFull; \
  using TraceableQ<T>::signalNotEmpty; \
  using TraceableQ<T>::signalNotFull; \
  using TraceableQ<T>::broadCastNotEmpty; \
  using TraceableQ<T>::broadCastNotFull; \
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
