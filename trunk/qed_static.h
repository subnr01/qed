/**
 * qed_static.h
 *
 * A concurrent lock-free circular array based queue.
 * Included in qed as a baseline.
 *
 *  Created on: May 10, 2010
 *      Author: jongsoo
 *
 * Usage example:
 *
 * StaticQ<Element> *q = new StaticQ<Element>(32);
 *   // Can use SpscStaticQ, SpStaticQ, or ScStaticQ, which are optimized
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
 * OrderedScStaticQ<Element> *outQ = new OrderedScStaticQ<Element>(32);
 *
 * // In producer thread
 * int h;
 * while (!q->reserveDequeue(&h));
 * Element *in = q->getBuf() + (h&(q->getCapacity() - 1));
 * while (!outQ->reserveEnqueue(&h));
 * Element *out = outQ->getBuf() + (h&(outQ->getCapacity() - 1));
 * ... // reads from in and writes to out
 * q->commitDequeue(h);
 * q->commitEnqueue(h); 
 */

#ifndef _QED_STATIC_H_
#define _QED_STATIC_H_

#include "qed_base.h"

namespace qed {

/**
 * The base class of lock-free circular array based queue.
 */
template<class T>
class BaseStaticQ : public BaseQ<T> {
public :
  BaseStaticQ(size_t N) : BaseQ<T>(N), headIndex(0), tailIndex(0) { }

  /**
   * @return the logical head index value
   */
  int getHeadIndex() const {
    return headIndex;
  }

  /**
   * @return the logical tail index value
   */
  int getTailIndex() const {
    return tailIndex;
  }

protected :
  // The following variables are aligned at the cache line boundary
  // to avoid false sharing.
  volatile int headIndex __attribute__((aligned (64)));
  volatile int tailIndex __attribute__((aligned (64)));
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
#define QED_USING_BASE_STATICQ_MEMBERS \
  QED_USING_BASEQ_MEMBERS \
  using BaseStaticQ<T>::headIndex; \
  using BaseStaticQ<T>::tailIndex; \
  using BaseStaticQ<T>::getHeadIndex; \
  using BaseStaticQ<T>::getTailIndex; \
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
  void commitEnqueue(T *out) { \
    commitEnqueue(out - BaseQ<T>::getBuf()); \
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
  } \
  void commitDequeue(T *in) { \
    commitDequeue(in - BaseQ<T>::getBuf()); \
  }

/**
 * A single-producer single-consumer (SPSC) array-based statically-sized
 * lock-free queue.
 */
template<class T>
class SpscStaticQ : public BaseStaticQ<T> {
public :
  SpscStaticQ(size_t N) :
    BaseStaticQ<T>(N), localHeadIndex(0), localTailIndex(0) { }

  QED_USING_BASE_STATICQ_MEMBERS

  /**
   * @param t points to reserved logical index,
   *          which is essentially a sequence number.
   *
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

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    traceCommitEnqueue(tailIndex&(N - 1));
    tailIndex++;
  }

  /**
   * @param h points to reserved logical index,
   *          which is essentially a sequence number.
   *
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

  /**
   * @param h a dummy argument to make the interface consistent
   */
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

private :
  int localHeadIndex __attribute__((aligned (64))), localTailIndex;
    // cached head/tail indices
    // localHeadIndex is cached in the producer and refreshed only when
    // the queue is determined as full.
};

/**
 * A convenient mid-level class for non-SPSC queues
 *
 * Non-SPSC queues require presence vector since commitEnqueue and/or
 * commitDequeue happen out of order.
 */
template<class T>
class NonSpscStaticQ : public BaseStaticQ<T> {
public :
  NonSpscStaticQ(size_t N) :
    BaseStaticQ<T>(N), presence((volatile int * const)alignedCalloc<int>(N)) {
  }

  using BaseQ<T>::N;
  using BaseQ<T>::traceFull;
  using BaseQ<T>::traceEmpty;
  using BaseStaticQ<T>::headIndex;
  using BaseStaticQ<T>::tailIndex;

  bool isFull(int t) {
    bool ret = presence[t&(N - 1)] || t - headIndex >= (int)N;
    traceFull(ret);
    return ret;
  }

  bool isFull() {
    return isFull(tailIndex);
  }

  bool isEmpty(int h) {
    bool ret = !presence[h&(N - 1)] || h == tailIndex;
    traceEmpty(ret);
    return ret;
  }

  bool isEmpty() {
    return isEmpty(headIndex);
  }

protected :
  volatile int * const presence __attribute__((aligned (64)));
};

#define QED_USING_NON_SPSC_STATICQ_MEMBERS \
  QED_USING_BASE_STATICQ_MEMBERS \
  using NonSpscStaticQ<T>::isFull; \
  using NonSpscStaticQ<T>::isEmpty; \
  using NonSpscStaticQ<T>::presence;

/**
 * SPMC queue.
 */
template<class T>
class SpStaticQ : public NonSpscStaticQ<T> {
public :
  SpStaticQ(size_t N) : NonSpscStaticQ<T>(N) {
  }

  QED_USING_NON_SPSC_STATICQ_MEMBERS

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

  /**
   * @param t a dummy argument to make the interface consistent
   */
  void commitEnqueue(int t = 0) {
    presence[tailIndex&(N - 1)] = 1;
    traceCommitEnqueue(tailIndex&(N - 1));
    tailIndex++;
  }

  bool reserveDequeue(int *h) {
    do {
      *h = headIndex;
      if (isEmpty(*h)) {
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&headIndex, *h, *h + 1));

    traceReserveDequeue(*h&(N - 1));
    return true;
  }

  /**
   * @param h the reserved logical index
   */
  void commitDequeue(int h) {
    presence[h&(N - 1)] = 0;
    traceCommitDequeue(h&(N - 1));
  }
};

/**
 * Unordered MPSC queue.
 */
template<class T>
class ScStaticQ : public NonSpscStaticQ<T> {
public :
  ScStaticQ(size_t N) : NonSpscStaticQ<T>(N) {
  }

  QED_USING_NON_SPSC_STATICQ_MEMBERS

  bool reserveEnqueue(int *t) {
    do {
      *t = tailIndex;
      if (isFull(*t)) {
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&tailIndex, *t, *t + 1));

    traceReserveEnqueue(*t&(N - 1));
    return true;
  }

  /**
   * @param t the reserved logical index
   */
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

  /**
   * @param h a dummy argument to make the interface consistent
   */
  void commitDequeue(int h = 0) {
    presence[headIndex&(N - 1)] = 0;
    traceCommitDequeue(headIndex&(N - 1));
    headIndex++;
  }
};

/**
 * Ordered MPSC queue.
 */
template<class T>
class OrderedScStaticQ : public BaseStaticQ<T> {
public :
  OrderedScStaticQ(size_t N) :
    BaseStaticQ<T>(N), presence((volatile int * const)alignedCalloc<int>(N)) {
  }

  QED_USING_BASE_STATICQ_MEMBERS

  /**
   * @param t caller sets the sequence id to enqueue, the value pointed by
   *          t won't be changed inside reserveEnqueue.
   *          We could use call by value but we use pointer here to make
   *          the interface consistent.
   *
   * @return true if reservation is successful
   */
  bool reserveEnqueue(const int *t) {
    if (isFull(*t)) {
      return false;
    }
    else {
      tailIndex = std::max<volatile int>(tailIndex, *t);
        // Note that tailIndex is an approximate value since we are not
        // atomically update it here.
      traceReserveEnqueue(*t&(N - 1));
      return true;
    }
  }

  /**
   * @param t the reserved logical index
   */
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

  /**
   * @param h a dummy argument to make the interface consistent
   */
  void commitDequeue(int h = 0) {
    presence[headIndex&(N - 1)] = 0;
    traceCommitDequeue(headIndex&(N - 1));
    headIndex++;
  }

  bool isFull(int t) const {
    return presence[t&(N - 1)] || t - headIndex >= (int)N;
  }

  bool isFull() {
    bool ret = isFull(tailIndex);
    traceFull(ret);
    return ret;
  }

  bool isEmpty() {
    bool ret = !presence[headIndex&(N - 1)];
      // We shoudn't compare headIndex and tailIndex since
      // tailIndex is just an approximate value.
    traceEmpty(ret);
    return ret;
  }

protected :
  volatile int * const presence __attribute__((aligned (64)));
};

/**
 * Unordered MPMC queue
 */
template<class T>
class StaticQ : public NonSpscStaticQ<T> {
public :
  StaticQ(size_t N) : NonSpscStaticQ<T>(N) {
  }

  QED_USING_NON_SPSC_STATICQ_MEMBERS

  /**
   * @param t points to reserved logical index,
   *          which is essentially a sequence number.
   *
   * @return true if reservation is successful.
   */
  bool reserveEnqueue(int *t) {
    do {
      *t = tailIndex;
      if (isFull(*t)) {
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&tailIndex, *t, *t + 1));

    traceReserveEnqueue(*t&(N - 1));
    return true;
  }

  /**
   * @param t the reserved logical index
   */
  void commitEnqueue(int t) {
    presence[t&(N - 1)] = 1;
    traceCommitEnqueue(t&(N - 1));
  }

  /**
   * @param h points to reserved logical index,
   *          which is essentially a sequence number.
   */
  bool reserveDequeue(int *h) {
    do {
      *h = headIndex;
      if (isEmpty(*h)) {
        return false;
      }
    } while (!__sync_bool_compare_and_swap(&headIndex, *h, *h + 1));

    traceReserveDequeue(*h&(N - 1));
    return true;
  }

  /**
   * @param h the reserved logical index
   */
  void commitDequeue(int h) {
    presence[h&(N - 1)] = 0;
    traceCommitDequeue(h&(N - 1));
  }
};

// TODO: ordered MPMC queue

} // namespace qed

#endif /* QUEUE_H_ */
