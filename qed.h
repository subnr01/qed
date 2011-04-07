/*
 * qed.h
 *
 * Queue Enhanced with Dynamic sizing: circular array based queue whose
 *   capacity is dynamically adjusted.
 *
 *  Created on: Jul 5, 2010
 *      Author: jongsoo
 *
 * Usage example:
 *
 * Qed<Element> *q = new Qed<Element>();
 *   // Can use SpscQed, SpQed, or ScQed, which are optimized
 *   // implementations for single producer and/or single consumer cases.
 *
 * // In producer thread
 * Element *out;
 * while (!q->reserveEnqueue(&out));
 * ... // writes to out
 * q->commitEnqueue(out);
 *
 * // In consumer thread
 * Element *in;
 * while (!q->reserveDequeue(&in));
 * ... // reads from in
 * q->commitDequeue(in);
 *
 * We sometimes want to preserve the ordering when we gather results from
 * multiple producers.
 * In this case, we need to alternative interfaces that allow us to reference
 * reserved logical index numbers.
 * Note how the sequence number from the input queue is used to maintain
 * the ordering of the output queue.
 *
 * OrderedScQed<Element> *outQ = new OrderedScQed<Element>();
 *
 * // In producer thread
 * PackedIndex h;
 * while (!q->reserveDequeue(&h));
 * Element *in = q->getBuf() + h.physical;
 * PackedIndex t;
 * t = h;
 * while (!outQ->reserveEnqueue(&t));
 * Element *out = outQ->getBuf() + t.physical;
 * ... // reads from in and writes to out
 * q->commitDequeue(h);
 * q->commitEnqueue(t); 
 */

#ifndef _QED_H_
#define _QED_H_

#include <climits>

#include "qed_base.h"

namespace qed {

static const int MAX_QLEN = 1 << 16;
static const int DEFAULT_CAPACITY = 256;
//static const size_t CACHE_CAPACITY = 512*1024; // 8MB L3 cache / 4 hyper-threaded cores / 2
//static const size_t CACHE_CAPACITY = 64*1024; // 256KB / 1 hyper-threaded core / 2
static const size_t CACHE_CAPACITY = 16*1024; // 32KB / 1 hyper-threaded core

/*
 * A union to atomically update logical and physical indices.
 */
typedef union {
  struct {
    int logical;
    int physical;
  };
  long l; /** for atomic updates */
} PackedIndex;

typedef union {
  struct {
    int logical;
    int physical:24;
    char c;
  };
  long l; /** for atomic updates */
} PackedIndexAndC;

typedef union {
  struct {
    int index;
    int c;
  };
  long l; /** for atomic updates */
} IndexAndC;

static inline int modPowerOf2(int n, int power) {
  return n&((1 << power) - 1); 
}

static inline int log2(int x) {
  int y;
  asm("bsr %1, %0\n" : "=r" (y) : "r" (x));
  return y;
}

/**
 * The base class of Qed.
 *
 * Expand/shrink decision functions are implemented here.
 */
template<class T>
class BaseQed : public BaseQ<T> {
public :
  BaseQed(size_t minC, size_t maxC) : BaseQ<T>(maxC), minC(minC) {
#if QED_TRACE_LEVEL > 0
    firstTrace = true;
#endif
  }

protected :
  /**
   * @param occ occupancy
   * @param c capacity
   * @param minOcc min occupancy during the last epoch
   */
  bool shouldExpand(int occ, int c, int minOcc, int maxOcc) {
    return
      (//(maxOcc >= c - 1 && c <= (int)(CACHE_CAPACITY/8/sizeof(T))) || // assume there are two queues (input/output) per thread
        (maxOcc - minOcc > c >> 1)) &&
      occ > 1 && c < (int)BaseQ<T>::N;
  }

  /**
   * @param mod physical tail index
   * @param d occupancy
   * @param maxOcc max occpancy during the last epoch
   */
  bool shouldShrink(int mod, int occ, int minOcc, int maxOcc) {
    return
      is2ToN(mod) && occ < mod && maxOcc - minOcc < mod >> 1 && mod >= minC;
      //&& !(maxOcc >= mod - 1 && mod < (int)(CACHE_CAPACITY/4/sizeof(T)));
  }

  bool shouldShrink2(int occ, int c, int minOcc, int maxOcc) {
    return maxOcc - minOcc < c >> 4 && occ < c && c > minC;
  }

  const int minC;
#if QED_TRACE_LEVEL > 0
  volatile bool firstTrace;
#endif
};

/*
 * A macro for things that repeatedly used in child classes of BaseStaticQ.
 *
 * Why not implement reserveEnqueue and reserveDequeue as members of
 * BaseStaticQ?
 * They call functions whose implementation differs in BaseStaticQ child
 * classes and if we implement that functions as virtual functions we
 * introduce overhead.
 * Theoretically, a smart C++ compiler can resolve which virtual function will
 * be called at the compile time, but, unfortunately, the current compiler
 * (at least g++) doesn't do that.
 */
#define QED_USING_BASE_QED_MEMBERS_ \
  QED_USING_BASEQ_MEMBERS \
  using TraceableQ<T>::traceResizing; \
  using TraceableQ<T>::traceResizing2; \
  using BaseQed<T>::shouldExpand; \
  using BaseQed<T>::shouldShrink; \
  using BaseQed<T>::shouldShrink2; \
  using BaseQed<T>::minC; \
  bool reserveEnqueue(int *t) { \
    PackedIndex temp; \
    if (reserveEnqueue(&temp)) { \
      *t = temp.physical; \
      return true; \
    } \
    else { \
      return false; \
    } \
  } \
 \
  bool reserveEnqueue(T **out) { \
    int t; \
    if (reserveEnqueue(&t)) { \
      *out = BaseQ<T>::getBuf() + t; \
      return true; \
    } \
    else { \
      return false; \
    } \
  } \
 \
  void commitEnqueue(const PackedIndex& t) { \
    commitEnqueue(t.physical);  \
  } \
 \
  bool reserveDequeue(int *h) { \
    PackedIndex temp; \
    if (reserveDequeue(&temp)) { \
      *h = temp.physical; \
      return true; \
    } \
    else { \
      return false; \
    } \
  } \
 \
  bool reserveDequeue(T **in) { \
    int h; \
    if (reserveDequeue(&h)) { \
      *in = BaseQ<T>::getBuf() + h; \
      return true; \
    } \
    else { \
      return false; \
    } \
  } \
  void commitDequeue(const PackedIndex& h) { \
    commitDequeue(h.physical); \
  }

#if QED_TRACE_LEVEL > 0
#define QED_USING_BASE_QED_MEMBERS \
  QED_USING_BASE_QED_MEMBERS_ \
  void traceReserveEnqueue(int tail) { \
    if (BaseQed<T>::firstTrace) { \
      traceResizing(getCapacity()); \
      BaseQed<T>::firstTrace = false; \
    } \
    BaseQ<T>::traceReserveEnqueue(tail); \
  } \
  void traceCommitDequeue(int head) { \
    TraceableQ<T>::lastTsc = readTsc(); \
    BaseQ<T>::traceCommitDequeue(head); \
  }
#else
#define QED_USING_BASE_QED_MEMBERS QED_USING_BASE_QED_MEMBERS_
#endif

/**
 * Multi-producer multi-consumer (MPMC) Qed
 */
template<class T>
class Qed : public BaseQed<T> {
public :
  Qed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    next((volatile int * const)alignedCalloc<int>(N)),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(-1),
    reservedDequeueCounter(0),
    tailIndex(-1),
    c(log2(std::min(std::max(DEFAULT_CAPACITY, minC), maxC))),
    minOcc(SHRT_MAX), maxOcc(0), 
    reservedEnqueueCounter(0) {
    tailIndexMod = (1 << c) - 1;
    headIndexMod = maxC - 1;
  }

  QED_USING_BASE_QED_MEMBERS

  /**
   * @param ret points to reserved index
   *
   * @return true if reservation is sucessful.
   */
  bool reserveEnqueue(PackedIndex *ret) {
    PackedIndexAndC oldPacked, newPacked; 
    int h, t, mod;

    do {
      h = headIndex;
      oldPacked.l = packedTailIndexAndC;

      t = oldPacked.logical;
      mod = oldPacked.physical + 1;
      int localC = 1 << oldPacked.c;

      newPacked.l = oldPacked.l;

      if (t - h >= localC || presence[mod]) {
        traceFull();
        waitNotFull();
        return false;
      }

      if (mod >= localC) {
        assert(mod < 2*localC);

        if (shouldExpand(t - h, localC, minOcc, maxOcc)) {
          newPacked.c = oldPacked.c + 1;
        }
        else if (presence[0]) {
          traceFull();
          waitNotFull();
          return false;
        }
        else {
          mod = 0;
        }
      }
      else if (shouldShrink(mod, t - h, minOcc, maxOcc) && !presence[0]) {
        newPacked.c = log2(mod);
        mod = 0;
      }

      newPacked.logical = t + 1;
      newPacked.physical = mod;
    } while (!__sync_bool_compare_and_swap(
      &packedTailIndexAndC, oldPacked.l, newPacked.l));

    int occ1 = t - h - reservedEnqueueCounter;
    int occ2 = t - h + reservedDequeueCounter;
    if (mod == 0) {
      minOcc = occ1;
      maxOcc = occ2;
    }
    else if (newPacked.c > oldPacked.c) {
      minOcc = occ1;
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }
    else {
      minOcc = std::min<volatile int>(minOcc, occ1);
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }

    ret->logical = t + 1;
    ret->physical = mod;
    next[oldPacked.physical] = mod;
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

#if QED_TRACE_LEVEL > 0
    if (newPacked.c != oldPacked.c) {
      traceResizing(1 << newPacked.c);
    }
#endif
#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(mod);

    return true;
  }

  /**
   * @param t the reserved physical index
   */
  void commitEnqueue(int t) {
    assert(t < (1 << c));
    assert(!presence[t]);
    presence[t] = 1;
    broadCastNotEmpty();
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  /**
   * @param ret points to reserved index
   */
  bool reserveDequeue(PackedIndex *ret) {
    PackedIndex oldPacked;
    do {
      oldPacked.l = packedHeadIndex;
      ret->logical = oldPacked.logical + 1;
      ret->physical = next[oldPacked.physical];
      if (oldPacked.logical >= tailIndex || !presence[ret->physical]) {
        traceEmpty();
        waitNotEmpty();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&packedHeadIndex, oldPacked.l, ret->l));

    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(ret->physical);

    return true;
  }

  /**
   * @param h the reserved physical index
   */
  void commitDequeue(int h) {
    assert(presence[h]);
    presence[h] = 0;
    broadCastNotFull();
    __sync_fetch_and_add(&reservedDequeueCounter, -1);
    traceCommitDequeue(h);
  }

  bool isEmpty(const PackedIndex &h) {
    bool ret = !presence[modPowerOf2(h.physical, c)] || h.logical == tailIndex;
    traceEmpty(ret);
    return ret;
  }

  bool isEmpty() const {
    PackedIndex head;
    head.l = packedHeadIndex;
    head.physical = next[head.physical];
    return isEmpty(head);
  }

  bool isFull(int t) const {
    return presence[t] || tailIndex - headIndex >= (1 << c);
  }

  size_t getCapacity() const {
    return 1 << c;
  }

private :
  volatile int * const next __attribute__((aligned (64)));
  volatile int * const presence __attribute__((aligned (64)));
  struct {
    union {
      struct {
        volatile int headIndex;
        volatile int headIndexMod;
      };
      volatile long packedHeadIndex;
    };
    volatile int reservedDequeueCounter;
  } __attribute__((aligned (64)));
  struct {
    union {
      struct {
        volatile int tailIndex;
        volatile int tailIndexMod:24;
        volatile char c;
      };
      volatile long packedTailIndexAndC;
    };
    volatile int minOcc, maxOcc;
    volatile int reservedEnqueueCounter;
  } __attribute__((aligned (64)));
};

/**
 * SPMC queue.
 */
template<class T>
class SpQed : public BaseQed<T> {
public :
  SpQed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    next((volatile int * const)alignedCalloc<int>(N)),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(-1),
    reservedDequeueCounter(0),
    tailIndex(-1),
    c(std::min(std::max(DEFAULT_CAPACITY, minC), maxC)),
    minOcc(SHRT_MAX), maxOcc(0) {
    tailIndexMod = c - 1;
    headIndexMod = maxC - 1;
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(PackedIndex *ret) {
    int localTailIndex = tailIndex;
    int occ1 = localTailIndex - headIndex;

    int mod = tailIndexMod + 1;

    if (presence[mod]) {
      traceFull();
      waitNotFull();
      return false;
    }

    if (mod >= c) {
      if (shouldExpand(occ1, c, minOcc, maxOcc)) {
        c <<= 1;
        traceResizing(c);
      }
      else if (presence[0]) {
        traceFull();
        waitNotFull();
        return false;
      }
      else {
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    else if (shouldShrink(mod, occ1, minOcc, maxOcc) && !presence[0]) {
      c = mod;
      minOcc = SHRT_MAX;
      maxOcc = 0;
      traceResizing(c);
    }

    ret->logical = localTailIndex + 1;
    ret->physical = mod&(c - 1);
    next[tailIndexMod] = ret->physical;
    tailIndexMod = ret->physical;

    int occ2 = occ1 + reservedDequeueCounter;
    minOcc = std::min(minOcc, occ1);
    maxOcc = std::max(maxOcc, occ2);

    traceReserveEnqueue(ret->physical);

    return true;
  }

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    presence[tailIndexMod] = 1;
    tailIndex++;
    traceCommitDequeue(tailIndexMod);
    broadCastNotEmpty();
  }

  bool reserveDequeue(PackedIndex *ret) {
    PackedIndex oldPacked;
    do {
      oldPacked.l = packedHeadIndex;
      ret->logical = oldPacked.logical + 1;
      ret->physical = next[oldPacked.physical];
      if (oldPacked.logical >= tailIndex || !presence[ret->physical]) {
        traceEmpty();
        waitNotEmpty();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&packedHeadIndex, oldPacked.l, ret->l));

    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(ret->physical);

    return true;
  }

  /**
   * @param h the reserved physical index
   */
  void commitDequeue(int h) {
    assert(presence[h]);
    presence[h] = 0;
    signalNotFull();
    __sync_fetch_and_add(&reservedDequeueCounter, -1);
    traceCommitDequeue(h);
  }

  bool isEmpty(const PackedIndex& h) {
    bool ret = h.logical == tailIndex || !presence[next[headIndexMod]];
    traceEmpty(ret);
    return ret;
  }

  bool isEmpty() {
    PackedIndex head;
    head.l = packedHeadIndex;
    head.physical = next[head.physical];
    return isEmpty(head);
  }

  bool isFull() {
    bool ret = presence[(tailIndexMod + 1)&(c - 1)];
    traceFull(ret);
    return ret;
  }

  size_t getCapacity() const {
    return c;
  }

private :
  volatile int * const next __attribute__((aligned (64)));
  volatile int * const presence __attribute__((aligned (64)));
  struct {
    union {
      struct {
        volatile int headIndex;
        volatile int headIndexMod;
      };
      volatile long packedHeadIndex;
    };
    volatile int reservedDequeueCounter;
  } __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
  int tailIndexMod, c;
  int minOcc, maxOcc;
};

/**
 * MPSC queue.
 */
template<class T>
class ScQed : public BaseQed<T> {
public :
  ScQed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    next((volatile int * const)alignedCalloc<int>(N)),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(-1), tailIndex(-1),
    c(log2(std::min(std::max(DEFAULT_CAPACITY, minC), maxC))),
    minOcc(SHRT_MAX), maxOcc(0), reservedEnqueueCounter(0) {
    tailIndexMod = (1 << c) - 1;
    headIndexMod = maxC - 1;
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(PackedIndex *ret) {
    PackedIndexAndC oldPacked, newPacked; 
    int h, t, mod, occ2;

    do {
      h = headIndex;
      oldPacked.l = packedTailIndexAndC;

      t = oldPacked.logical;
      mod = oldPacked.physical + 1;
      int localC = 1 << oldPacked.c;

      newPacked.l = oldPacked.l;

      occ2 = t - h;
      if (occ2 >= localC || presence[mod]) {
        traceFull();
        waitNotFull();
        return false;
      }

      if (mod >= localC) {
        if (shouldExpand(occ2, localC, minOcc, maxOcc)) {
          newPacked.c = oldPacked.c + 1;
        }
        else if (presence[0]) {
          traceFull();
          waitNotFull();
          return false;
        }
        else {
          mod = 0;
        }
      }
      else if (shouldShrink(mod, occ2, minOcc, maxOcc) && !presence[0]) {
        newPacked.c = log2(mod);
        mod = 0;
      }

      newPacked.logical= t + 1;
      newPacked.physical= mod;
    } while (!__sync_bool_compare_and_swap(
      &packedTailIndexAndC, oldPacked.l, newPacked.l));

    int occ1 = occ2 - reservedEnqueueCounter;
    if (mod == 0) {
      minOcc = occ1;
      maxOcc = occ2;
    }
    else if (newPacked.c > oldPacked.c) {
      minOcc = occ1;
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }
    else {
      minOcc = std::min<volatile int>(minOcc, occ1);
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }

    ret->logical = t + 1;
    ret->physical = mod;
    next[oldPacked.physical] = mod;
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

#if QED_TRACE_LEVEL > 0
    if (newPacked.c != oldPacked.c) {
      traceResizing(1 << newPacked.c);
    }
#endif
#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(mod);

    return true;
  }

  /**
   * @param t the reserved physical index
   */
  void commitEnqueue(int t) {
    assert(t < (1 << c));
    assert(!presence[t]);
    presence[t] = 1;
    signalNotEmpty();
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  bool reserveDequeue(PackedIndex *ret) {
    ret->physical = next[headIndexMod];
    if (!presence[ret->physical]) {
      traceEmpty();
      waitNotEmpty();
      return false;
    }

    ret->logical = headIndex + 1;
    headIndexMod = ret->physical;

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(ret->physical);
    return true;
  }

  /**
   * @param h a dummy argument to make the interface consistent
   */
  void commitDequeue(int h = 0) {
    assert(headIndexMod < (1 << c));
    assert(presence[headIndexMod]);
    presence[headIndexMod] = 0;
    traceCommitDequeue(headIndexMod);
    headIndex++;
    broadCastNotFull();
  }

  bool isEmpty() {
    bool ret = !presence[next[headIndexMod]];
    traceEmpty(ret);
    return ret;
  }

  bool isFull(int seqId) const {
    return presence[seqId] || seqId - headIndex >= (1 << c);
  }

  size_t getCapacity() const {
    return 1 << c;
  }

private :
  volatile int * const next __attribute__((aligned (64)));
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  int headIndexMod;
  struct {
    union {
      struct {
        volatile int tailIndex;
        volatile int tailIndexMod:24;
        volatile char c;
      };
      volatile long packedTailIndexAndC;
    };
    volatile int minOcc, maxOcc;
    volatile int reservedEnqueueCounter;
  } __attribute__((aligned (64)));
};

#define QED_USE_SPIN_LOCK

/**
 * Ordered MPSC queue.
 */
template<class T>
class OrderedScQed : public BaseQed<T> {
public :
  OrderedScQed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(BaseQ<T>::N)),
    headIndex(0), headIndexMod(0), tailIndex(0),
    tailIndexBase(0), c(std::min(std::max(DEFAULT_CAPACITY, minC), maxC)),
    minOcc(SHRT_MAX), maxOcc(0), reservedEnqueueCounter(0) {
#ifdef QED_USE_SPIN_LOCK
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
#else
    pthread_mutex_init(&lock_, NULL);
#endif
  }

  OrderedScQed() {
#ifdef QED_USE_SPIN_LOCK
    pthread_spin_destroy(&lock_);
#else
    pthread_mutex_destroy(&lock_);
#endif
  }

  QED_USING_BASE_QED_MEMBERS

  /**
   * @param ret caller sets the logical index of the item to enqueue.
   *            callee sets the reserved physical index.
   */
  bool reserveEnqueue(PackedIndex *ret) {
    int localC = c;
    int seqId = ret->logical;
    int h = headIndex;
    if (seqId - h >= localC ||
      presence[(seqId - tailIndexBase)&(localC - 1)]) {
      traceFull();
      waitNotFull();
      return false;
    }

    lock();

    int base = tailIndexBase;
    localC = c;

    if (seqId - h >= localC || presence[(seqId - base)&(localC - 1)]) {
      unlock();
      traceFull();
      waitNotFull();
      return false;
    }

    tailIndex = std::max<unsigned int>(seqId, tailIndex);
    int mod = tailIndex - base;
    int occ2 = tailIndex - h;

    if (mod >= localC) {
      if (shouldExpand(occ2, localC, minOcc, maxOcc)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else if (presence[0]) {
        unlock();
        traceFull();
        waitNotFull();
        return false;
      }
      else {
        base += localC;
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    // If it's almost empty.
    else if (shouldShrink(mod, occ2, minOcc, maxOcc) && !presence[0]) {
      localC = mod;
      base += mod;
      minOcc = SHRT_MAX;
      maxOcc = 0;
      traceResizing(localC);
    }

    c = localC;
    tailIndexBase = base;

    int occ1 = occ2 - reservedEnqueueCounter;
    minOcc = std::min<volatile int>(minOcc, occ1);
    maxOcc = std::max<volatile int>(maxOcc, occ2);

    unlock();

    ret->physical = (seqId - base)&(localC - 1);
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

    traceReserveEnqueue(ret->physical);

    return true;
  }

  /**
   * @param t the reserved physical index
   */
  void commitEnqueue(int t) {
    assert(!presence[t&(c - 1)]);
    presence[t&(c - 1)] = 1;
    signalNotEmpty();
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t&(c - 1));
  }

  bool reserveDequeue(PackedIndex *ret) {
    if (isEmpty()) {
      waitNotEmpty();
      return false;
    }
    else {
      ret->logical = headIndex;
      ret->physical = headIndexMod;
      traceReserveDequeue(ret->physical);
      return true;
    }
  }

  /**
   * @param h a dummy argument to make the interface consistent
   */
  void commitDequeue(int h = 0) {
    assert(headIndexMod < c);
    assert(presence[headIndexMod]);
    presence[headIndexMod] = 0;
    traceCommitDequeue(headIndexMod);
    headIndexMod = (headIndexMod + 1)&(c - 1);
    headIndex++;
    broadCastNotFull();
  }

  bool isEmpty() {
    headIndexMod &= c - 1;
    bool ret = !presence[headIndexMod];
    traceEmpty(ret);
    return ret;
  }

  bool isFull(int seqId) {
    int localC = c;
    bool ret =
      presence[(seqId - tailIndexBase)&(localC - 1)] ||
      seqId - headIndex >= localC;
    traceFull(ret);
    return ret;
  }

  size_t getCapacity() const {
    return c;
  }

private :
  void lock() {
#ifdef QED_USE_SPIN_LOCK
    pthread_spin_lock(&lock_);
#else
    pthread_mutex_lock(&lock_);
#endif
  }

  void unlock() {
#ifdef QED_USE_SPIN_LOCK
    pthread_spin_unlock(&lock_);
#else
    pthread_mutex_unlock(&lock_);
#endif
  }

#ifdef QED_USE_SPIN_LOCK
  pthread_spinlock_t lock_ __attribute__((aligned (64)));
#else
  pthread_mutex_t lock_ __attribute__((aligned (64)));
#endif
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  int headIndexMod;
  struct {
    volatile int tailIndex;
    volatile int tailIndexBase;
    volatile int c;
    volatile int minOcc, maxOcc;
    volatile int reservedEnqueueCounter;
  } __attribute__((aligned (64)));
};

/**
 * Single-producer single-consumer (SPSC) Qed
 */
template<class T>
class SpscQed : public BaseQed<T> {
public :
  SpscQed(int minC = 4, int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    next((volatile int * const)alignedCalloc<int>(N)),
    headIndex(-1), tailIndex(-1),
    c(std::min(std::max(DEFAULT_CAPACITY, minC), maxC)),
    minOcc(SHRT_MAX), maxOcc(0) {
    tailIndexMod = c - 1;
    headIndexMod = maxC - 1;
  }

  QED_USING_BASE_QED_MEMBERS

  /**
   * @param ret points to reserved index
   *
   * @return true if reservation is successful.
   */
  bool reserveEnqueue(PackedIndex *ret) {
    if (isFull()) {
      waitNotFull();
      return false;
    }

    int occ = tailIndex - headIndex;
    int mod = tailIndexMod + 1;

    if (mod >= c) {
      if (shouldExpand(occ, c, minOcc, maxOcc)) {
        c <<= 1;
        traceResizing(c);
      }
      else {
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    else if (shouldShrink(mod, occ, minOcc, maxOcc)) {
      c = mod;
      minOcc = SHRT_MAX;
      maxOcc = 0;
      traceResizing(c);
    } 

    ret->logical = tailIndex + 1;
    ret->physical = mod&(c - 1);
    next[tailIndexMod] = ret->physical;
    tailIndexMod = ret->physical;

    minOcc = std::min(minOcc, occ);
    maxOcc = std::max(maxOcc, occ);

    traceReserveEnqueue(ret->physical);

    return true;
  }

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    tailIndex++;
    traceCommitEnqueue(tailIndexMod);
    signalNotEmpty();
  }

  /**
   * @param ret points to reserved index
   *
   * @return true if reservation is successful.
   */
  bool reserveDequeue(PackedIndex *ret) {
    ret->logical = headIndex + 1;
    if (ret->logical > tailIndex) {
      traceEmpty();
      waitNotEmpty();
      return false;
    }

    ret->physical = next[headIndexMod];
    headIndexMod = ret->physical;
#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(ret->physical);
    return true;
  }

  /**
   * @param h a dummy argument to make the interface consistent
   */
  void commitDequeue(int h = 0) {
    assert(headIndexMod < c);
    traceCommitDequeue(headIndexMod);
    headIndex++;
    signalNotFull();
  }

  bool isEmpty() {
    bool ret = headIndex == tailIndex;
    traceEmpty(ret);
    return ret;
  }

  bool isFull() {
    bool ret = tailIndex - headIndex >= c;
    traceFull(ret);
    return ret;
  }

  size_t getCapacity() const {
    return c;
  }

private :
  volatile int * const next __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  int headIndexMod;
  volatile int tailIndex __attribute__((aligned (64)));
  int tailIndexMod, c;
  int minOcc, maxOcc;
};

// TODO: ordered MPMC queue

} // namespace qed

#endif // _QED_H_
