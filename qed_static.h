/*
 * qed_static.h
 *
 * A concurrent lock-free circular array based queue.
 * Included in qed as a baseline.
 *
 *  Created on: May 10, 2010
 *      Author: jongsoo
 */

#ifndef _QED_STATIC_H_
#define _QED_STATIC_H_

#include "qed_base.h"

namespace qed {

#define QED_USING_BASE_STATICQ_MEMBERS \
  QED_USING_BASEQ_MEMBERS \
  bool reserveEnqueue(T **out) { \
    int t; \
    if (reserveEnqueue(&t)) { \
      *out = BaseQ<T>::getBuf() + (t&(N - 1)); \
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
      *in = BaseQ<T>::getBuf() + (h&(N - 1)); \
      return true; \
    } \
    else { \
      return false; \
    } \
  }

/*
 * A single-producer single-consumer (SPSC) array-based statically-sized
 * lock-free queue.
 */
template<class T>
class SpscStaticQ : public BaseQ<T> {
public :
  SpscStaticQ(size_t N) :
    BaseQ<T>(N),
    headIndex(0), tailIndex(0), localHeadIndex(0), localTailIndex(0) { }

  QED_USING_BASE_STATICQ_MEMBERS

  /**
   * @return true if reservation is successful.
   */
  bool reserveEnqueue(int *t) {
    if (isFull()) {
      return false;
    }
    else {
      *t = tailIndex;
      traceReserveEnqueue(*t&(N - 1));
      return true;
    }
  }

  void commitEnqueue(int t = 0) {
    traceCommitEnqueue(tailIndex&(N - 1));
    tailIndex++;
  }

  /**
   * @return true if reservation is successful.
   */
  bool reserveDequeue(int *h) {
    if (isEmpty()) {
      return false;
    }
    else {
      *h = headIndex;
      traceReserveDequeue(*h&(N - 1));
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    traceCommitDequeue(headIndex&(N - 1));
    headIndex++;
  }

  bool isEmpty() {
    if (localTailIndex == headIndex) {
      localTailIndex = tailIndex;
      bool ret = localTailIndex == headIndex;
      traceEmpty(ret);
      return ret;
    }
#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    return false;
  }

  bool isFull() {
    if (tailIndex - localHeadIndex >= (int)N) {
      localHeadIndex = headIndex;
      bool ret = tailIndex - localHeadIndex >= (int)N;
      traceFull(ret);
      return ret;
    }
#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    return false;
  }

  int getHeadIndex() const {
    return headIndex;
  }

  int getTailIndex() const {
    return tailIndex;
  }

  int size() const {
    return tailIndex - headIndex;
  }

private :
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
  int localHeadIndex __attribute__((aligned (64))), localTailIndex; // cached head/tail indices
};

/*
 * SPMC queue.
 */
template<class T>
class SpStaticQ : public BaseQ<T> {
public :
  SpStaticQ(size_t N) :
    BaseQ<T>(N),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), tailIndex(0) {
  }

  QED_USING_BASE_STATICQ_MEMBERS

  bool reserveEnqueue(int *t) {
    if (isFull()) {
      return false;
    }
    else {
      *t = tailIndex;
      traceReserveEnqueue(*t&(N - 1));
      return true;
    }
  }

  void commitEnqueue(int t = 0) {
    presence[tailIndex&(N - 1)] = 1;
    traceCommitEnqueue(tailIndex&(N - 1));
    tailIndex++;
  }

  bool reserveDequeue(int *h) {
    do {
      *h = headIndex;
      if (presence[*h&(N - 1)] == 0 || *h == tailIndex) {
        traceEmpty();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&headIndex, *h, *h + 1));

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(*h&(N - 1));
    return true;
  }

  void commitDequeue(int h) {
    presence[h&(N - 1)] = 0;
    traceCommitDequeue(h&(N - 1));
  }

  bool isEmpty(int h) const {
    return presence[h&(N - 1)] == 0;
  }

  bool isEmpty() const {
    int h = headIndex;
    return presence[h&(N - 1)] == 0 || h == tailIndex;
  }

  bool isFull() {
    bool ret = presence[tailIndex&(N - 1)] == 1;
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

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
};

/*
 * Unordered MPSC queue.
 */
template<class T>
class ScStaticQ : public BaseQ<T> {
public :
  ScStaticQ(size_t N) :
    BaseQ<T>(N),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), tailIndex(0) {
  }

  QED_USING_BASE_STATICQ_MEMBERS

  bool reserveEnqueue(int *t) {
    do {
      *t = tailIndex;
      if (presence[*t&(N - 1)] == 1 || *t - headIndex >= (int)N) {
        traceFull();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&tailIndex, *t, *t + 1));

#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(*t&(N - 1));

    return true;
  }

  void commitEnqueue(int t) {
    presence[t&(N - 1)] = 1;
    traceCommitEnqueue(t&(N - 1));
  }

  bool reserveDequeue(int *h) {
    if (isEmpty()) {
      return false;
    }
    else {
      *h = headIndex;
      traceReserveDequeue(*h&(N - 1));
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    presence[headIndex&(N - 1)] = 0;
    traceCommitDequeue(headIndex&(N - 1));
    headIndex++;
  }

  bool isEmpty() {
    bool ret = presence[headIndex&(N - 1)] == 0;
    traceEmpty(ret);
    return ret;
  }

  bool isFull(int index) const {
    return presence[index&(N - 1)] == 1 || index >= headIndex + N;
  }

  int size() const {
    return tailIndex - headIndex;
  }

  int getTailIndex() const {
    return tailIndex;
  }

  int getHeadIndex() const {
    return headIndex;
  }

private :
  volatile int *const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
};

/*
 * Ordered MPSC queue.
 */
template<class T>
class OrderedScStaticQ : public BaseQ<T> {
public :
  OrderedScStaticQ(size_t N) :
    BaseQ<T>(N),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), maxSeqId(0) {
  }

  QED_USING_BASE_STATICQ_MEMBERS

  bool reserveEnqueue(int *t) {
    if (isFull(*t)) {
      return false;
    }
    else {
      maxSeqId = std::max(maxSeqId, *t);
      traceReserveEnqueue(*t&(N - 1));
      return true;
    }
  }

  void commitEnqueue(int t) {
    presence[t&(N - 1)] = 1;
    traceCommitEnqueue(t&(N - 1));
  }

  bool reserveDequeue(int *h) {
    if (isEmpty()) {
      return false;
    }
    else {
      *h = headIndex;
      traceReserveDequeue(*h&(N - 1));
      return true;
    }
  }

  void commitDequeue(int h = 0) {
    presence[headIndex&(N - 1)] = 0;
    traceCommitDequeue(headIndex&(N - 1));
    headIndex++;
  }

  bool isEmpty() {
    bool ret = presence[headIndex&(N - 1)] == 0;
    traceEmpty(ret);
    return ret;
  }

  bool isFull(int seqId) {
    bool ret = presence[seqId&(N - 1)] == 1 || seqId >= (int)(headIndex + N);
    traceFull(ret);
    return ret;
  }

  int size() const {
    return maxSeqId - headIndex;
  }

private :
  volatile int * const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  int maxSeqId __attribute__((aligned (64)));
};

/*
 * MPMC queue
 */
template<class T>
class StaticQ : public BaseQ<T> {
public :
  StaticQ(size_t N) :
    BaseQ<T>(N),
    presence((volatile int * const)alignedCalloc<int>(N)),
    headIndex(0), tailIndex(0) {
  }

  QED_USING_BASE_STATICQ_MEMBERS

  bool reserveEnqueue(int *t) {
    do {
      *t = tailIndex;
      if (presence[*t&(N - 1)] == 1 || *t >= headIndex + N) {
        traceFull();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&tailIndex, *t, *t + 1));

#if QED_TRACE_LEVEL >= 2
    isSpinningFull = false;
#endif
    traceReserveEnqueue(*t&(N - 1));
    
    return true;
  }

  void commitEnqueue(int t) {
    presence[t&(N - 1)] = 1;
    traceCommitEnqueue(t&(N - 1));
  }

  bool reserveDequeue(int *h) {
    do {
      *h = headIndex;
      if (presence[*h&(N - 1)] == 0 || *h == tailIndex) {
        traceEmpty();
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&headIndex, *h, *h + 1));

#if QED_TRACE_LEVEL >= 2
    isSpinningEmpty = false;
#endif
    traceReserveDequeue(*h&(N - 1));

    return true;
  }

  void commitDequeue(int h) {
    presence[h&(N - 1)] = 0;
    traceCommitDequeue(h&(N - 1));
  }

  bool isEmpty(int i) const {
    return presence[i&(N - 1)] == 0 || i == tailIndex;
  }

  bool isFull(int i) const {
    return presence[i&(N - 1)] == 1 || i >= headIndex + N;
  }

  int size() const {
    return tailIndex - headIndex;
  }

private :
  volatile int *const presence __attribute__((aligned (64)));
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
};

} // namespace qed

#endif /* QUEUE_H_ */
