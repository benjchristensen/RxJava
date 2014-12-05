/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rx.internal.util;

import java.util.Queue;

import rx.*;
import rx.exceptions.MissingBackpressureException;
import rx.internal.operators.NotificationLite;
import rx.internal.util.unsafe.*;

/**
 * This assumes Spsc or Spmc usage. This means only a single producer calling the on* methods. This is the Rx contract of an Observer.
 * Concurrent invocations of on* methods will not be thread-safe.
 */
public class RxRingBuffer implements Subscription {

    public static RxRingBuffer getSpscInstance() {
        Queue<Object> q;
        boolean bounded;
//        if (UnsafeAccess.isUnsafeAvailable()) {
//            q = new AtomicArrayQueue(4, SIZE);
//            bounded = true;
//        } else {
            q = new SpscLinkedQueue<Object>();
            bounded = false;
//        }
//        q = new DiagnosticSpscQueue<Object>(q, true);
        return new RxRingBuffer(q, SIZE, bounded);
    }

    @Deprecated
    public static RxRingBuffer getSpmcInstance() {
        // TODO right now this is returning an Spsc queue. Why did anything need Spmc before?
        Queue<Object> q;
        boolean bounded;
        if (UnsafeAccess.isUnsafeAvailable()) {
//            q = new AtomicArrayQueue(4, SIZE);
            q = new SpscArrayQueue<Object>(SIZE * 2); // the lookahead gives trouble with the effective capacity
//            q = new AtomicArrayQueuePadded(4, SIZE); // the lookahead gives trouble with the effective capacity
//            q = new AtomicArrayQueueUnsafe(4, SIZE); // the lookahead gives trouble with the effective capacity
            bounded = true;
        } else {
            q = new SpscLinkedQueue<Object>();
            bounded = false;
        }
//        q = new DiagnosticSpscQueue<Object>(q, false);
        return new RxRingBuffer(q, SIZE, bounded);
    }

    private static final NotificationLite<Object> on = NotificationLite.instance();

    private Queue<Object> queue;

    private final int size;
    /** The queue itself is bounded. */
    private final boolean isBounded;
    
    /** Keeps track how many items were produced. */
    private long producerIndexCached;
    /** Shows how many items were produced to other threads. */
    private volatile long producerIndexShared;
    /** Keeps track how many items were consumed. */
    private long consumerIndexCached;
    /** Shows how many items were consumed to other threads. */
    private volatile long consumerIndexShared;
    /**
     * We store the terminal state separately so it doesn't count against the size.
     * We don't just +1 the size since some of the queues require sizes that are a power of 2.
     * This is a subjective thing ... wanting to keep the size (ie 1024) the actual number of onNext
     * that can be sent rather than something like 1023 onNext + 1 terminal event. It also simplifies
     * checking that we have received only 1 terminal event, as we don't need to peek at the last item
     * or retain a boolean flag.
     */
    public volatile Object terminalState;

    // default size of ring buffer
    /**
     * 128 was chosen as the default based on the numbers below. A stream processing system may benefit from increasing to 512+.
     * 
     * <pre> {@code
     * ./gradlew benchmarks '-Pjmh=-f 1 -tu s -bm thrpt -wi 5 -i 5 -r 1 .*OperatorObserveOnPerf.*'
     * 
     * 1024
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   100642.874    24676.478    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     4095.901       90.730    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5        9.797        4.982    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 15536155.489   758579.454    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   156257.341     6324.176    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      157.099        7.143    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    16864.641     1826.877    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     4269.317      169.480    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5       13.393        1.047    ops/s
     * 
     * 512
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5    98945.980    48050.282    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     4111.149       95.987    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5       12.483        3.067    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16032469.143   620157.818    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   157997.290     5097.718    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      156.462        7.728    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    15813.984     8260.170    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     4358.334      251.609    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5       13.647        0.613    ops/s
     * 
     * 256
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   108489.834     2688.489    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     4526.674      728.019    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5       13.372        0.457    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16435113.709   311602.627    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   157611.204    13146.108    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      158.346        2.500    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    16976.775      968.191    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     6238.210     2060.387    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5       13.465        0.566    ops/s
     * 
     * 128
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   106887.027    29307.913    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     6713.891      202.989    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5       11.929        0.187    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16055774.724   350633.068    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   153403.821    17976.156    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      153.559       20.178    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    17172.274      236.816    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     7073.555      595.990    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5       11.855        1.093    ops/s
     * 
     * 32
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   106128.589    20986.201    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     6396.607       73.627    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5        7.643        0.668    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16012419.447   409004.521    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   157907.001     5772.849    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      155.308       23.853    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    16927.513      606.692    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     5191.084      244.876    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5        8.288        0.217    ops/s
     * 
     * 16
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   109974.741      839.064    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5     4538.912      173.561    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5        5.420        0.111    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16017466.785   768748.695    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   157934.065    13479.575    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      155.922       17.781    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    14903.686     3325.205    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5     3784.776     1054.131    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5        5.624        0.130    ops/s
     * 
     * 2
     * 
     * Benchmark                                         (size)   Mode   Samples        Score  Score error    Units
     * r.o.OperatorObserveOnPerf.observeOnComputation         1  thrpt         5   112663.216      899.005    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation      1000  thrpt         5      899.737        9.460    ops/s
     * r.o.OperatorObserveOnPerf.observeOnComputation   1000000  thrpt         5        0.999        0.100    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate           1  thrpt         5 16087325.336   783206.227    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate        1000  thrpt         5   156747.025     4880.489    ops/s
     * r.o.OperatorObserveOnPerf.observeOnImmediate     1000000  thrpt         5      156.645        3.810    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread           1  thrpt         5    15958.711      673.895    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread        1000  thrpt         5      884.624       47.692    ops/s
     * r.o.OperatorObserveOnPerf.observeOnNewThread     1000000  thrpt         5        1.173        0.100    ops/s
     * } </pre>
     */
    static int _size = 128;
    static {
        // lower default for Android (https://github.com/ReactiveX/RxJava/issues/1820)
        if (PlatformDependent.isAndroid()) {
            _size = 16;
        }

        // possible system property for overriding
        String sizeFromProperty = System.getProperty("rx.ring-buffer.size"); // also see IndexedRingBuffer
        if (sizeFromProperty != null) {
            try {
                _size = Integer.parseInt(sizeFromProperty);
            } catch (Exception e) {
                System.err.println("Failed to set 'rx.buffer.size' with value " + sizeFromProperty + " => " + e.getMessage());
            }
        }
    }
    public static final int SIZE = _size;

    /* test */ RxRingBuffer(Queue<Object> queue, int size, boolean bounded) {
        this.queue = queue;
        this.size = size;
        this.isBounded = bounded;
    }

    @Deprecated
    public void release() {
    }

    @Override
    public void unsubscribe() {
        release();
    }

    /* package accessible for unit tests */RxRingBuffer() {
        this(new SynchronizedQueue<Object>(SIZE), SIZE, false);
    }

    /**
     * 
     * @param o
     * @throws MissingBackpressureException
     *             if more onNext are sent than have been requested
     */
    public void onNext(Object o) throws MissingBackpressureException {
        Queue<Object> q = queue;
        if (q == null) {
            throw new IllegalStateException("This instance has been unsubscribed and the queue is no longer usable.");
        }
        
        if (isBounded) {
            if (!q.offer(on.next(o))) {
                throw new MissingBackpressureException();
            }
        } else
        if(producerQueueSize() < size) {
            produce();
            q.offer(on.next(o));
        } else {
            // throw exception since we exceeded size limit
            throw new MissingBackpressureException();
        }
        
//        if (!queue.offer(on.next(o))) {
//            throw new MissingBackpressureException();
//        }
    }

    public void onCompleted() {
        // we ignore terminal events if we already have one
        if (terminalState == null) {
            terminalState = on.completed();
        }
    }

    public void onError(Throwable t) {
        // we ignore terminal events if we already have one
        if (terminalState == null) {
            terminalState = on.error(t);
        }
    }

    public int available() {
        return size - count();
    }

    public int capacity() {
        return size;
    }

    /**
     * @return Returns the queue size from the view of the producer.
     */
    protected int producerQueueSize() {
        long diff = producerIndexCached - consumerIndexShared;
        if (diff > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)diff;
    }
    /**
     * @return Returns the queue size from the view of the consumer.
     */
    protected int consumerQueueSize() {
        long diff = producerIndexShared - consumerIndexCached;
        if (diff > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)diff;
    }
    protected void produce() {
        producerIndexShared = ++producerIndexCached;
    }
    protected void consume() {
        consumerIndexShared = ++consumerIndexCached;
    }
    
    public int count() {
        Queue<Object> q = queue;
        if (q == null) {
            return 0;
        }
        if (isBounded) {
            return q.size();
        }
        long diff = producerIndexShared - consumerIndexShared;
        if (diff > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)diff;
//        return queue.size();
    }

    public boolean isEmpty() {
        Queue<Object> q = queue;
        if (q == null) {
            return true;
        }
        return q.isEmpty();
    }

    public Object poll() {
        Queue<Object> q = queue;
        if (q == null) {
            // we are unsubscribed and have released the undelrying queue
            return null;
        }
        Object o;
        o = q.poll();
        /*
         * benjchristensen July 10 2014 => The check for 'queue.isEmpty()' came from a very rare concurrency bug where poll()
         * is invoked, then an "onNext + onCompleted/onError" arrives before hitting the if check below. In that case,
         * "o == null" and there is a terminal state, but now "queue.isEmpty()" and we should NOT return the terminalState.
         * 
         * The queue.size() check is a double-check that works to handle this, without needing to synchronize poll with on*
         * or needing to enqueue terminalState.
         * 
         * This did make me consider eliminating the 'terminalState' ref and enqueuing it ... but then that requires
         * a +1 of the size, or -1 of how many onNext can be sent. See comment on 'terminalState' above for why it
         * is currently the way it is.
         */
        if (o == null && terminalState != null && q.isEmpty()) {
            o = terminalState;
            // once emitted we clear so a poll loop will finish
            terminalState = null;
        } else {
            // decrement when we drain
            if (!isBounded) {
                consume();
            }
        }
        return o;
    }

    public Object peek() {
        Queue<Object> q = queue;
        if (q == null) {
            // we are unsubscribed and have released the undelrying queue
            return null;
        }
        Object o;
        o = q.peek();
        if (o == null && terminalState != null && q.isEmpty()) {
            o = terminalState;
        }
        return o;
    }

    public boolean isCompleted(Object o) {
        return on.isCompleted(o);
    }

    public boolean isError(Object o) {
        return on.isError(o);
    }

    public Object getValue(Object o) {
        return on.getValue(o);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean accept(Object o, Observer child) {
        return on.accept(child, o);
    }

    public Throwable asError(Object o) {
        return on.getError(o);
    }

    @Override
    public boolean isUnsubscribed() {
        return queue == null;
    }

}