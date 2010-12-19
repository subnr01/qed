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
static const int DEFAULT_SIZE = 128;

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
 * The base class of QED.
 *
 * Expand/shrink decision functions are implemented here.
 */
template<class T>
class BaseQed : public BaseQ<T> {
public :
  BaseQed(size_t minC, size_t maxC) : BaseQ<T>(maxC), minC(minC) {
  }

protected :
  /**
   * @param occ occupancy
   * @param c capacity
   * @param minOcc min occupancy during the last epoch
   */
  bool shouldExpand(int occ, int c, int minOcc, int maxOcc) {
    return occ > 1 && maxOcc - minOcc >= (c >> 1) + (c >> 2) && c < (int)BaseQ<T>::N;
  }

  /**
   * @param mod physical tail index
   * @param d occupancy
   * @param maxOcc max occpancy during the last epoch
   */
  bool shouldShrink(int mod, int occ, int minOcc, int maxOcc) {
    return is2ToN(mod) && occ < mod && maxOcc - minOcc < mod >> 1 && mod >= minC; 
  }

  bool shouldShrink2(int occ, int c, int minOcc, int maxOcc) {
    return maxOcc - minOcc < c >> 4 && occ < c && c > minC;
  }

  const int minC;
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
#define QED_USING_BASE_QED_MEMBERS \
  QED_USING_BASEQ_MEMBERS \
  using BaseQ<T>::traceResizing; \
  using BaseQ<T>::traceResizing2; \
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
  } \
  size_t getCapacity() const { \
    return c; \
  }

/**
 * A single-producer single-consumer (SPSC) QED
 */
template<class T>
class SpscQed : public BaseQed<T> {
public :
  SpscQed(int minC = 4, int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC), headIndex(0), tailIndex(0),
    c(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    tailIndexMod(0), headIndexMod(0),
    localC(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minOcc(SHRT_MAX), maxOcc(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  /**
   * @param ret points to reserved index
   *
   * @return true if reservation is successful.
   */
  bool reserveEnqueue(PackedIndex *ret) {
    if (isFull()) {
      return false;
    }
    else {
      ret->logical = tailIndex;
      ret->physical = tailIndexMod;
      traceReserveEnqueue(ret->physical);
      return true;
    }
  }

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    int occ = tailIndex - headIndex + 1;
    int mod = tailIndexMod + 1;

    if (__builtin_expect(mod >= localC, 0)) {
      if (__builtin_expect(shouldExpand(occ, localC, minOcc, maxOcc), false)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else {
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    else if (__builtin_expect(shouldShrink(mod, occ, minOcc, maxOcc), false)) {
      localC = tailIndexMod + 1;
      minOcc = SHRT_MAX;
      maxOcc = 0;
      traceResizing(localC);
    }

    traceCommitEnqueue(tailIndexMod);
    IndexAndC localTailIndexAndC;
    localTailIndexAndC.index = tailIndex + 1;
    localTailIndexAndC.c = localC;
    tailIndexAndC = localTailIndexAndC.l;
    tailIndexMod = mod&(localC - 1);

    minOcc = std::min(minOcc, occ);
    maxOcc = std::max(maxOcc, occ);
  }

  /**
   * @param ret points to reserved index
   *
   * @return true if reservation is successful.
   */
  bool reserveDequeue(PackedIndex *ret) {
    IndexAndC localTailIndexAndC;
    localTailIndexAndC.l = tailIndexAndC;
    ret->logical = headIndex;
    if (ret->logical >= localTailIndexAndC.index) {
      traceEmpty();
      return false;
    }

    headIndexMod &= localTailIndexAndC.c - 1;
    ret->physical = headIndexMod;
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
    headIndexMod = (headIndexMod + 1)&(c - 1);
    headIndex++;
  }

  bool isEmpty() {
    bool ret = headIndex == tailIndex;
    traceEmpty(ret);
    return ret;
  }

  bool isFull() {
    bool ret = tailIndex - headIndex >= localC;
    traceFull(ret);
    return ret;
  }

private :
  volatile int headIndex __attribute__((aligned (64)));
  union {
    struct {
      volatile int tailIndex;
      volatile int c;
    };
    volatile long tailIndexAndC;
  } __attribute__((aligned (64)));
  int tailIndexMod __attribute__((aligned (64))), headIndexMod, localC;
  int minOcc, maxOcc;
};

/**
 * SPMC queue.
 */
template<class T>
class SpQed : public BaseQed<T> {
public :
  SpQed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), headIndexMod(0),
    reservedDequeueCounter(0),
    tailIndex(0), c(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    tailIndexMod(0),
    minOcc(SHRT_MAX), maxOcc(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(PackedIndex *ret) {
    if (isFull()) {
      return false;
    }
    else {
      ret->logical = tailIndex;
      ret->physical = tailIndexMod;
      traceReserveEnqueue(ret->physical);
      return true;
    }
  }

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    int localTailIndex = tailIndex;
    int occ1 = localTailIndex - headIndex + 1;

    int mod = tailIndexMod + 1;
    int localC = c;
    if (__builtin_expect(mod >= localC, 0)) {
      if (__builtin_expect(shouldExpand(occ1, localC, minOcc, maxOcc), false)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else {
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    else if (__builtin_expect(shouldShrink(mod, occ1, minOcc, maxOcc), false)) {
      localC = mod;
      minOcc = SHRT_MAX;
      maxOcc = 0;
      traceResizing(localC);
    }

    assert(!presence[tailIndexMod]);
    presence[tailIndexMod] = 1;
    traceCommitEnqueue(tailIndexMod);
    IndexAndC localTailIndexAndC;
    localTailIndexAndC.index = localTailIndex + 1;
    localTailIndexAndC.c = localC;
    tailIndexAndC = localTailIndexAndC.l;
    tailIndexMod = mod&(localC - 1);

    int occ2 = occ1 + reservedDequeueCounter;
    minOcc = std::min(minOcc, occ1);
    maxOcc = std::max(maxOcc, occ2); 
  }

  bool reserveDequeue(PackedIndex *ret) {
    PackedIndex next;
    int mod;
    do {
      IndexAndC localTailIndexAndC;
      localTailIndexAndC.l = tailIndexAndC;
      int localC = localTailIndexAndC.c;
      ret->l = packedHeadIndex;
      mod = ret->physical&(localC - 1);
      if (ret->logical >= localTailIndexAndC.index || !presence[mod]) {
        traceEmpty();
        return false;
      }
      
      next.logical = ret->logical + 1;
      next.physical = (mod + 1)&(localC - 1);

    } while (!__sync_bool_compare_and_swap(&packedHeadIndex, ret->l, next.l));

    ret->physical = mod;
    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(mod);

    return true;
  }

  /**
   * @param h the reserved physical index
   */
  void commitDequeue(int h) {
    __sync_fetch_and_add(&reservedDequeueCounter, -1);
    assert(presence[h]);
    presence[h] = 0;
    traceCommitDequeue(h);
  }

  bool isEmpty(const PackedIndex& h) {
    bool ret = !presence[h.physical&(c - 1)] || h.logical == tailIndex;
    traceEmpty(ret);
    return ret;
  }

  bool isEmpty() {
    PackedIndex head;
    head.l = packedHeadIndex;
    return isEmpty(head);
  }

  bool isFull() {
    bool ret = presence[tailIndexMod];
    traceFull(ret);
    return ret;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  union {
    struct {
      volatile int headIndex;
      volatile int headIndexMod;
    };
    volatile long packedHeadIndex;
  } __attribute__((aligned (64)));
  volatile int reservedDequeueCounter;
  union {
    struct {
      volatile int tailIndex;
      volatile int c;
    };
    volatile long tailIndexAndC;
  } __attribute__((aligned (64)));
  int tailIndexMod __attribute__((aligned (64)));
  int minOcc, maxOcc;
};

/**
 * Unordered MPSC queue.
 */
template<class T>
class ScQed : public BaseQed<T> {
public :
  ScQed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), tailIndex(0),
    tailIndexMod(0), c(log2(std::min(std::max(DEFAULT_SIZE, minC), maxC))),
    minOcc(SHRT_MAX), maxOcc(0), reservedEnqueueCounter(0), headIndexMod(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(PackedIndex *ret) {
    PackedIndexAndC oldPacked, newPacked; 
    int h, t, mod, occ2;

    //bool shrink2;
    do {
      //shrink2 = false;
      h = headIndex;
      oldPacked.l = packedTailIndexAndC;

      t = oldPacked.logical;
      mod = oldPacked.physical;
      int localC = 1 << oldPacked.c;

      newPacked.l = oldPacked.l;

      occ2 = t - h;
      if (occ2 >= localC || presence[mod]) {
        traceFull();
        return false;
      }

      if (__builtin_expect(mod >= localC, 0)) {
        if (__builtin_expect(
            shouldExpand(occ2, localC, minOcc, maxOcc), false)) {
          newPacked.c = oldPacked.c + 1;
        }
        else if (presence[0]) {
          traceFull();
          return false;
        }
        //else if (shouldShrink2(occ2, localC, minOcc, maxOcc)) {
          //newPacked.c = oldPacked.c - 1;
          //mod = 0;
          //shrink2 = true;
        //}
        else {
          mod = 0;
        }
      }
      else if (__builtin_expect(
          shouldShrink(mod, occ2, minOcc, maxOcc) && !presence[0], false)) {
        newPacked.c = log2(mod);
        mod = 0;
      }

      newPacked.logical= t + 1;
      newPacked.physical= mod + 1;
    } while (!__sync_bool_compare_and_swap(
      &packedTailIndexAndC, oldPacked.l, newPacked.l));

    int occ1 = occ2 - reservedEnqueueCounter;
    if (__builtin_expect(mod == 0, 0)) {
      minOcc = occ1;
      maxOcc = occ2;
    }
    else if (__builtin_expect(newPacked.c > oldPacked.c, 0)) {
      minOcc = occ1;
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }
    else {
      minOcc = std::min<volatile int>(minOcc, occ1);
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }

    ret->logical = t;
    ret->physical = mod;
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

#if QED_TRACE_LEVEL > 0
    if (newPacked.c != oldPacked.c) {
      //if (shrink2) {
        //traceResizing2(1 << newPacked.c);
      //}
      //else {
        traceResizing(1 << newPacked.c);
      //}
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
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  bool reserveDequeue(PackedIndex *ret) {
    PackedIndexAndC packed;
    packed.l = packedTailIndexAndC;
    int localC = 1 << packed.c;
    int mod = headIndexMod&(localC - 1);
    if (!presence[mod]) {
      traceEmpty();
      return false;
    }

    ret->logical = headIndex;
    ret->physical = mod;
    headIndexMod = mod;

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(mod);
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
    headIndexMod = modPowerOf2(headIndexMod + 1, c);
    headIndex++;
  }

  bool isEmpty() {
    headIndexMod &= (1 << c) - 1;
    bool ret = !presence[headIndexMod];
    traceEmpty(ret);
    return ret;
  }

  bool isFull(int seqId) const {
    return presence[seqId] || seqId - headIndex >= (1 << c);
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  union {
    struct {
      volatile int tailIndex;
      volatile int tailIndexMod:24;
      volatile char c;
    };
    volatile long packedTailIndexAndC;
  } __attribute__((aligned (64)));
  volatile int minOcc, maxOcc;
  volatile int reservedEnqueueCounter;
  int headIndexMod __attribute__((aligned (64)));
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
    headIndex(0), tailIndex(0),
    tailIndexBase(0), c(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minOcc(SHRT_MAX), maxOcc(0), reservedEnqueueCounter(0),
    headIndexMod(0) {
#ifdef QED_USE_SPIN_LOCK
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
#else
    pthread_mutex_init(&lock_, NULL);
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
      return false;
    }

    lock();

    int base = tailIndexBase;
    localC = c;

    if (seqId - h >= localC || presence[(seqId - base)&(localC - 1)]) {
      unlock();
      traceFull();
      return false;
    }

    tailIndex = std::max<unsigned int>(seqId, tailIndex);
    int mod = tailIndex - base;
    int occ2 = tailIndex - h;

    if (__builtin_expect(mod >= localC, 0)) {
      if (__builtin_expect(shouldExpand(occ2, localC, minOcc, maxOcc), 0)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else if (__builtin_expect(presence[0], 0)) {
        unlock();
        traceFull();
        return false;
      }
      else {
        base += localC;
        maxOcc = 0;
      }
      minOcc = SHRT_MAX;
    }
    // If it's almost empty.
    else if (__builtin_expect(
        shouldShrink(mod, occ2, minOcc, maxOcc) && !presence[0], 0)) {
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
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t&(c - 1));
  }

  bool reserveDequeue(PackedIndex *ret) {
    if (isEmpty()) {
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
  volatile int tailIndex __attribute__((aligned (64)));
  volatile int tailIndexBase;
  volatile int c;
  volatile int minOcc, maxOcc;
  volatile int reservedEnqueueCounter;
  int headIndexMod __attribute__((aligned (64)));
};

/**
 * Unordered MPMC queue
 */
template<class T>
class Qed : public BaseQed<T> {
public :
  Qed(int minC = 4, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), headIndexMod(0),
    reservedDequeueCounter(0),
    tailIndex(0), tailIndexMod(0),
    c(log2(std::min(std::max(DEFAULT_SIZE, minC), maxC))),
    minOcc(SHRT_MAX), maxOcc(0), 
    reservedEnqueueCounter(0) {
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
      mod = oldPacked.physical;
      int localC = 1 << oldPacked.c;

      newPacked.l = oldPacked.l;

      if (t - h >= localC || presence[mod]) {
        traceFull();
        return false;
      }

      if (__builtin_expect(mod >= localC, 0)) {
        assert(mod < 2*localC);

        if (__builtin_expect(shouldExpand(t - h, localC, minOcc, maxOcc), 0)) {
          newPacked.c = oldPacked.c + 1;
        }
        else if (__builtin_expect(presence[0], 0)) {
          traceFull();
          return false;
        }
        else {
          mod = 0;
        }
      }
      else if (__builtin_expect(
          shouldShrink(mod, t - h, minOcc, maxOcc) && !presence[0], 0)) {
        newPacked.c = log2(mod);
        mod = 0;
      }

      newPacked.logical = t + 1;
      newPacked.physical = mod + 1;
    } while (!__sync_bool_compare_and_swap(
      &packedTailIndexAndC, oldPacked.l, newPacked.l));

    int occ1 = t - h - reservedEnqueueCounter;
    int occ2 = t - h + reservedDequeueCounter;
    if (__builtin_expect(mod == 0, 0)) {
      minOcc = occ1;
      maxOcc = occ2;
    }
    else if (__builtin_expect(newPacked.c > oldPacked.c, 0)) {
      minOcc = occ1;
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }
    else {
      minOcc = std::min<volatile int>(minOcc, occ1);
      maxOcc = std::max<volatile int>(maxOcc, occ2);
    }

    ret->logical = t;
    ret->physical = mod;
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
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  /**
   * @param ret points to reserved index
   */
  bool reserveDequeue(PackedIndex *ret) {
    PackedIndex next;
    int mod;
    do {
      PackedIndexAndC packed;
      packed.l = packedTailIndexAndC;
      int localC = packed.c;
      ret->l = packedHeadIndex;
      mod = modPowerOf2(ret->physical, localC);
      if (ret->logical >= packed.logical || !presence[mod]) {
        traceEmpty();
        return false;
      }

      next.logical = ret->logical + 1;
      next.physical = modPowerOf2(mod + 1, localC);

    } while (!__sync_bool_compare_and_swap(&packedHeadIndex, ret->l, next.l));

    ret->physical = mod;
    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(mod);

    return true;
  }

  /**
   * @param h the reserved physical index
   */
  void commitDequeue(int h) {
    assert(presence[h]);
    presence[h] = 0;
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
    return isEmpty(head);
  }

  bool isFull(int t) const {
    return presence[t] || tailIndex - headIndex >= (1 << c);
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  union {
    struct {
      volatile int headIndex;
      volatile int headIndexMod;
    };
    volatile long packedHeadIndex;
  } __attribute__((aligned (64)));
  volatile int reservedDequeueCounter;
  union {
    struct {
      volatile int tailIndex;
      volatile int tailIndexMod:24;
      volatile char c;
    };
    volatile long packedTailIndexAndC;
  } __attribute__((aligned (64)));
  volatile int minOcc, maxOcc;
  volatile int reservedEnqueueCounter;
};

// TODO: ordered MPMC queue

} // namespace qed

#endif // _QED_H_
