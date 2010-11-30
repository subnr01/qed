/*
 * qed.h
 *
 * Queue Enhanced with Dynamic sizing: circular array based queue whose
 *   capacity is dynamically adjusted.
 *
 *  Created on: Jul 5, 2010
 *      Author: jongsoo
 */

#ifndef _QED_H_
#define _QED_H_

#include <cassert>
#include <climits>

#include "qed_base.h"

namespace qed {

static const int DEFAULT_SIZE = 64;
static const int MAX_QLEN = 1 << 16;

/*
 * A union to atomically update two integer values using compare and swap.
 */
typedef union {
  long l;
  int i[2];
} U;

#define USE_SPIN_LOCK

static inline bool is2ToN(int i) {
  return ((i - 1)&i) == 0;
}

template<class T>
class BaseQed : public BaseQ<T> {
public :
  BaseQed(int minC, size_t maxC) : BaseQ<T>(maxC), minC(minC) {
#ifdef USE_SPIN_LOCK
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
#else
    pthread_mutex_init(&lock_, NULL);
#endif
  }

protected :
  void lock() {
#ifdef USE_SPIN_LOCK
    pthread_spin_lock(&lock_);
#else
    pthread_mutex_lock(&lock_);
#endif
  }

  void unlock() {
#ifdef USE_SPIN_LOCK
    pthread_spin_unlock(&lock_);
#else
    pthread_mutex_unlock(&lock_);
#endif
  }

protected :
  bool shouldExpand(int d, int C, int minSize) {
    return d >= (C >> 1) + (C >> 2) && minSize <= C >> 2;
  }

  bool shouldShrink(int mod, int d, int maxSize) {
    return is2ToN(mod) && d < mod >> 2 && maxSize < mod >> 1 && mod >= minC; 
  }

  const int minC;

private :
#ifdef USE_SPIN_LOCK
  pthread_spinlock_t lock_ __attribute__((aligned (64)));
#else
  pthread_mutex_t lock_ __attribute__((aligned (64)));
#endif
};

#define QED_USING_BASE_QED_MEMBERS \
  QED_USING_BASEQ_MEMBERS \
  using BaseQed<T>::lock; \
  using BaseQed<T>::unlock; \
  using BaseQ<T>::traceResizing; \
  using BaseQed<T>::shouldExpand; \
  using BaseQed<T>::shouldShrink; \
  using BaseQed<T>::minC; \
  bool reserveEnqueue(int *t) { \
    U temp; \
    if (reserveEnqueue(&temp)) { \
      *t = temp.i[1]; \
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
  void commitEnqueue(const U& t) { \
    commitEnqueue(t.i[1]);  \
  } \
 \
  bool reserveDequeue(int *h) { \
    U temp; \
    if (reserveDequeue(&temp)) { \
      *h = temp.i[1]; \
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
  void commitDequeue(const U& h) { \
    commitDequeue(h.i[1]); \
  }

template<class T>
class SpscQed : public BaseQed<T> {
public :
  SpscQed(int minC = 64, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC), headIndex(0), tailIndex(0),
    C(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    tailIndexMod(0), headIndexMod(0),
    localC(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    localTailIndex(0), minSize(INT_MAX), maxSize(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(U *ret) {
    if (isFull()) {
      return false;
    }
    else {
      ret->i[0] = tailIndex;
      ret->i[1] = tailIndexMod;
      traceReserveEnqueue(ret->i[1]);
      return true;
    }
  }

  void commitEnqueue(int t = 0) {
    int d = tailIndex - headIndex + 1;
    int mod = tailIndexMod + 1;

    if (mod >= localC) {
      assert(mod < 2*localC);
      if (shouldExpand(d, localC, minSize)) {
        localC <<= 1;
        C = localC;
        traceResizing(localC);
      }
      else {
        minSize = INT_MAX;
        maxSize = 0;
      }
    }
    else if (shouldShrink(mod, d, maxSize)) {
      localC = tailIndexMod + 1;
      C = localC;
      minSize = INT_MAX;
      maxSize = 0;
      traceResizing(localC);
    }

    tailIndex++;
    traceCommitEnqueue(tailIndexMod);
    tailIndexMod = mod&(localC - 1);

    minSize = std::min(minSize, d);
    maxSize = std::max(maxSize, d);
  }

  bool reserveDequeue(U *ret) {
    if (isEmpty()) {
      return false;
    }
    else {
      ret->i[0] = headIndex;
      ret->i[1] = headIndexMod;
      traceReserveDequeue(ret->i[1]);
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    assert(headIndexMod < C);
    traceCommitDequeue(headIndexMod);
    headIndexMod = (headIndexMod + 1)&(C - 1);
    headIndex++;
  }

  bool isEmpty() {
    if (localTailIndex == headIndex) {
      localTailIndex = tailIndex;
      if (localTailIndex == headIndex) {
        traceEmpty();
        return true;
      }
    }
#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    return false;
  }

  bool isFull() {
    bool ret = tailIndex >= headIndex + localC;
    traceFull(ret);
    return ret;
  }

  int size() const {
    return tailIndex - headIndex;
  }

  int getHeadIndex() const {
    return headIndex;
  }

  int getTailIndex() const {
    return tailIndex;
  }

  int getCapacity() const {
    return C;
  }

private :
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
  volatile int C;
  int tailIndexMod __attribute__((aligned (64))), headIndexMod, localC, localTailIndex;
  int minSize, maxSize;
};

template<class T>
class SpQed : public BaseQed<T> {
public :
  SpQed(int minC = 64, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    reservedDequeueCounter(0),
    tailIndex(0), C(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    localTailIndex(0), tailIndexMod(0),
    localC(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minSize(INT_MAX), maxSize(0) {
    head.l = 0;
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(U *ret) {
    if (isFull()) {
      return false;
    }
    else {
      ret->i[0] = localTailIndex;
      ret->i[1] = tailIndexMod;
      traceReserveEnqueue(ret->i[1]);
      return true;
    }
  }

  void commitEnqueue(int t = 0) {
    int d1 = localTailIndex - head.i[0] + 1;
    int d2 = d1 + reservedDequeueCounter;

    int mod = tailIndexMod + 1;
    if (mod >= localC) {
      assert(mod < 2*localC);
      if (shouldExpand(d2, localC, minSize)) {
        localC <<= 1;
        C = localC;
        traceResizing(localC);
      }
      else {
        minSize = INT_MAX;
        maxSize = 0;
      }
    }
    else if (shouldShrink(mod, d2, maxSize)) {
      localC = mod;
      C = localC;
      minSize = INT_MAX;
      maxSize = 0;
      traceResizing(localC);
    }

    assert(!presence[tailIndexMod]);
    presence[tailIndexMod] = 1;
    traceCommitEnqueue(tailIndexMod);
    localTailIndex++;
    tailIndex = localTailIndex;
    tailIndexMod = mod&(localC - 1);

    minSize = std::min(minSize, d1);
    maxSize = std::max(maxSize, d2); 
  }

  bool reserveDequeue(U *ret) {
    U temp;
    int mod;
    do {
      ret->l = head.l;
      mod = ret->i[1]&(C - 1);
      if (!presence[mod] || ret->i[0] == tailIndex) {
        traceEmpty();
        return false;
      }
      
      temp.i[0] = ret->i[0] + 1;
      temp.i[1] = mod + 1;

    } while (!__sync_bool_compare_and_swap(&head.l, ret->l, temp.l));

    ret->i[1] = mod;
    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(mod);

    return true;
  }

  void commitDequeue(int h) {
    assert(presence[h]);
    presence[h] = 0;
    __sync_fetch_and_add(&reservedDequeueCounter, -1);
    traceCommitDequeue(h);
  }

  bool isEmpty() const {
    U h;
    h.l = head.l;
    if (!presence[h.i[1]] || h.i[0] == tailIndex) {
      if (h.i[1] >= C) {
        h.i[1] &= C - 1;
        if (!presence[h.i[1]]) {
          return true;
        }
      }
      else {
        return true;
      }
    }
    return false;
  }

  bool isFull() {
    bool ret = presence[tailIndexMod];
    traceFull(ret);
    return ret;
  }

  int size() const {
    return tailIndex - head.i[0];
  }

  int getHeadIndex() const {
    return head.i[0];
  }

  int getTailIndex() const {
    return tailIndex;
  }

  int getCapacity() const {
    return C;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile U head __attribute__((aligned (64)));
  volatile int reservedDequeueCounter;
  volatile int tailIndex __attribute__((aligned (64)));
  volatile int C;
  int localTailIndex __attribute__((aligned (64))), tailIndexMod, localC;
  int minSize, maxSize;
};

template<class T>
class ScQed : public BaseQed<T> {
public :
  ScQed(int minC = 64, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), tailIndex(0),
    tailIndexMod(0), C(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minSize(INT_MAX), maxSize(0), reservedEnqueueCounter(0), headIndexMod(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(U *ret) {
    int h = headIndex;

    if (tailIndex - h >= C || presence[tailIndexMod]) {
      traceFull();
      return false;
    }

    lock();

    int t = tailIndex;
    int mod = tailIndexMod;
    int localC = C;

    if (t - h >= localC || presence[mod]) {
      unlock();
      traceFull();
      return false;
    }

    int d2 = t - h;
    if (mod >= localC) {
      assert(mod < 2*localC);

      if (shouldExpand(d2, localC, minSize)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else if (presence[0]) {
        unlock();
        traceFull();
        return false;
      }
      else {
        mod = 0;
        minSize = INT_MAX;
        maxSize = 0;
      }
    }
    else if (shouldShrink(mod, d2, maxSize) && !presence[0]) {
      localC = mod;
      mod = 0;
      minSize = INT_MAX;
      maxSize = 0;
      traceResizing(localC);
    }

    C = localC;
    tailIndex = t + 1; // tailIndex must be modified after modification of C
    tailIndexMod = mod + 1;

    int d1 = d2 - reservedEnqueueCounter;
    minSize = std::min<volatile int>(minSize, d1);
    maxSize = std::max<volatile int>(maxSize, d2);

    unlock();

    ret->i[0] = t;
    ret->i[1] = mod;
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(mod);

    return true;
  }

  void commitEnqueue(int t) {
    assert(t < C);
    assert(!presence[t]);
    presence[t] = 1;
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  bool reserveDequeue(U *ret) {
    if (isEmpty()) {
      return false;
    }
    else {
      ret->i[0] = headIndex;
      ret->i[1] = headIndexMod;
      traceReserveDequeue(ret->i[1]);
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    assert(headIndexMod < C);
    assert(presence[headIndexMod]);
    presence[headIndexMod] = 0;
    traceCommitDequeue(headIndexMod);
    headIndexMod = (headIndexMod + 1)&(C - 1);
    headIndex++;
  }

  bool isEmpty() {
    if (headIndexMod >= C) {
      assert(headIndexMod < 2*C);
      headIndexMod &= C - 1;
      assert(headIndexMod == 0);
      if (!presence[0]) {
        traceEmpty();
        return true;
      }
      else {
#if QED_TRACE_LEVEL >= 2
        isSpinningEmpty = false;
#endif
        return false;
      }
    }
    else if (presence[headIndexMod]) {
#if QED_TRACE_LEVEL >= 2
      isSpinningEmpty = false;
#endif
      return false;
    }
    else {
      traceEmpty();
      return true;
    }
  }

  bool isFull(int seqId) const {
    return presence[seqId] || tailIndex >= headIndex + C;
  }

  int size() const {
    return tailIndex - headIndex;
  }

  int getHeadIndex() const {
    return headIndex;
  }

  int getTailIndex() const {
    return tailIndex;
  }

  int getCapacity() const {
    return C;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
  volatile int tailIndexMod;
  volatile int C;
  volatile int minSize, maxSize;
  volatile int reservedEnqueueCounter;
  int headIndexMod __attribute__((aligned (64)));
};

template<class T>
class OrderedScQed : public BaseQed<T> {
public :
  OrderedScQed(int minC = 64, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), maxSeqId(0),
    tailIndexBase(0), C(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minSize(INT_MAX), maxSize(0), reservedEnqueueCounter(0),
    headIndexMod(0) {
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(U *ret) {
    int localC = C;
    int seqId = ret->i[0];
    int h = headIndex;
    if (seqId - h >= localC ||
      presence[(seqId - tailIndexBase)&(localC - 1)]) {
      traceFull();
      return false;
    }

    lock();

    int base = tailIndexBase;
    localC = C;

    if (seqId - h >= localC || presence[(seqId - base)&(localC - 1)]) {
      unlock();
      traceFull();
      return false;
    }

    maxSeqId = std::max<unsigned int>(seqId, maxSeqId);
    int mod = maxSeqId - base;
    int d2 = maxSeqId - h;

    if (mod >= localC) {
      assert(mod < 2*localC);
      
      if (shouldExpand(d2, localC, minSize)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else if (presence[0]) {
        unlock();
        traceFull();
        return false;
      }
      else {
        base += localC;
        minSize = INT_MAX;
        maxSize = 0;
      }
    }
    // If it's almost empty.
    else if (shouldShrink(mod, d2, maxSize) && !presence[0]) {
      localC = mod;
      base += mod;
      minSize = INT_MAX;
      maxSize = 0;
      traceResizing(localC);
    }

    C = localC;
    tailIndexBase = base;

    int d1 = d2 - reservedEnqueueCounter;
    minSize = std::min<volatile int>(minSize, d1);
    maxSize = std::max<volatile int>(maxSize, d2);

    unlock();

    ret->i[1] = (seqId - base)&(localC - 1);
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);

    traceReserveEnqueue(ret->i[1]);

    return true;
  }

  void commitEnqueue(int t) {
    assert(!presence[t&(C - 1)]);
    presence[t&(C - 1)] = 1;
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t&(C - 1));
  }

  bool reserveDequeue(U *ret) {
    if (isEmpty()) {
      return false;
    }
    else {
      ret->i[0] = headIndex;
      ret->i[1] = headIndexMod;
      traceReserveDequeue(ret->i[1]);
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    assert(headIndexMod < C);
    assert(presence[headIndexMod]);
    presence[headIndexMod] = 0;
    traceCommitDequeue(headIndexMod);
    headIndexMod = (headIndexMod + 1)&(C - 1);
    headIndex++;
  }

  bool isEmpty() {
    if (!presence[headIndexMod]) {
      if (headIndexMod >= C) {
        headIndexMod &= C - 1;
        if (!presence[0]) {
          traceEmpty();
          return true;
        }
      }
      else {
        traceEmpty();
        return true;
      }
    }
#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    return false;
  }

  bool isFull(int seqId) {
    int localC = C;
    bool ret =
      presence[(seqId + localC - tailIndexBase)&(C - 1)] ||
      seqId >= headIndex + C;
    traceFull(ret);
    return ret;
  }

  int size() const {
    return maxSeqId - headIndex;
  }

  int getHeadIndex() const {
    return headIndex;
  }

  int getTailIndex() const {
    return maxSeqId;
  }

  int getCapacity() const {
    return C;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  volatile int maxSeqId __attribute__((aligned (64)));
  volatile int tailIndexBase;
  volatile int C;
  volatile int minSize, maxSize;
  volatile int reservedEnqueueCounter;
  int headIndexMod __attribute__((aligned (64)));
};

template<class T>
class Qed : public BaseQed<T> {
public :
  Qed(int minC = 64, const int maxC = MAX_QLEN) :
    BaseQed<T>(minC, maxC),
    presence((volatile int * const)alignedCalloc<int>(N)),
    reservedDequeueCounter(0),
    tailIndex(0), tailIndexMod(0),
    C(std::min(std::max(DEFAULT_SIZE, minC), maxC)),
    minSize(INT_MAX), maxSize(0), 
    reservedEnqueueCounter(0) {
    head.l = 0;
  }

  QED_USING_BASE_QED_MEMBERS

  bool reserveEnqueue(U *ret) {
    int h = head.i[0];
    if (tailIndex - h >= C || presence[tailIndexMod]) {
      traceFull();
      return false;
    }

    lock();

    int t = tailIndex;
    int mod = tailIndexMod;
    int localC = C;

    if (t - h >= localC || presence[mod]) {
      unlock();
      traceFull();
      return false;
    }

    int d2 = t - h + reservedDequeueCounter;
    if (mod >= localC) {
      assert(mod < 2*localC);

      if (shouldExpand(d2, localC, minSize)) {
        localC <<= 1;
        traceResizing(localC);
      }
      else if (presence[0]) {
        unlock();
        traceFull();
        return false;
      }
      else {
        mod = 0;
        minSize = INT_MAX;
        maxSize = 0;
      }
    }
    // If it's almost empty.
    else if (shouldShrink(mod, d2, maxSize) && !presence[0]) {
      localC = mod;
      mod = 0;
      minSize = INT_MAX;
      maxSize = 0;
      traceResizing(localC);
    }

    C = localC;
    tailIndex = t + 1; // tailIndex must be modified after modification of C
    tailIndexMod = mod + 1;

    int d1 = t - h - reservedEnqueueCounter;
    minSize = std::min<volatile int>(minSize, d1);
    maxSize = std::max<volatile int>(maxSize, d2);

    unlock();

    ret->i[0] = t;
    ret->i[1] = mod;
    __sync_fetch_and_add(&reservedEnqueueCounter, 1);
    
#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(mod);

    return true;
  }

  void commitEnqueue(int t) {
    assert(t < C);
    assert(!presence[t]);
    presence[t] = 1;
    __sync_fetch_and_add(&reservedEnqueueCounter, -1);
    traceCommitEnqueue(t);
  }

  bool reserveDequeue(U *ret) {
    U temp;
    int mod;
    do {
      ret->l = head.l;
      mod = ret->i[1]&(C - 1);
      if (!presence[mod] || ret->i[0] == tailIndex) {
        traceEmpty();
        return false;
      }

      temp.i[0] = ret->i[0] + 1;
      temp.i[1] = mod + 1;

    } while (!__sync_bool_compare_and_swap(&head.l, ret->l, temp.l));

    ret->i[1] = mod;
    __sync_fetch_and_add(&reservedDequeueCounter, 1);

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(mod);

    return true;
  }

  void commitDequeue(int h) {
    assert(presence[h]);
    presence[h] = 0;
    __sync_fetch_and_add(&reservedDequeueCounter, -1);
    traceCommitDequeue(h);
  }

  bool isEmpty(const U &u) const {
    if (!presence[u.i[1]] || u.i[0] == tailIndex) {
      if (u.i[1] >= C) {
        u.i[1] &= C - 1;
        if (!presence[u.i[1]]) {
          return true;
        }
      }
      else {
        return true;
      }
    }
    return false;
  }

  bool isEmpty() const {
    U u;
    u.l = head.l;
    return isEmpty(head);
  }

  bool isFull(int i) const {
    return presence[i] || tailIndex >= head.i[0] + C;
  }

  int getHeadIndex() const {
    return head.i[0];
  }

  int getTailIndex() {
    return tailIndex;
  }

  int getCapacity() {
    return C;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile U head __attribute__((aligned (64)));
  volatile int reservedDequeueCounter;
  volatile int tailIndex __attribute__((aligned (64)));
  volatile int tailIndexMod;
  volatile int C;
  volatile int minSize, maxSize;
  volatile int reservedEnqueueCounter;
};

} // namespace qed

#endif // _QED_H_
