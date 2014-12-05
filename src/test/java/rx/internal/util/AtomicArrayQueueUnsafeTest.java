package rx.internal.util;

import static org.junit.Assert.*;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import rx.exceptions.MissingBackpressureException;
import rx.internal.util.unsafe.AtomicArrayQueueUnsafe;

public class AtomicArrayQueueUnsafeTest {
    @Test
    public void testSimpleOfferPoll() {
        Queue<Object> saq = new AtomicArrayQueueUnsafe(8, 64 * 1024);
        for (int i = 0; i < 10000; i++) {
            saq.offer(i);
            assertEquals(i, saq.poll());
        }
    }
    @Test
    public void testTriggerOneGrowth() {
        Queue<Object> saq = new AtomicArrayQueueUnsafe(8, 64 * 1024);
        for (int i = 0; i < 16; i++) {
            saq.offer(i);
        }
        for (int i = 0; i < 16; i++) {
            assertEquals(i, saq.poll());
        }       
    }
    @Test
    public void testTriggerGrowthHalfwayReading() {
        Queue<Object> saq = new AtomicArrayQueueUnsafe(8, 16);
        for (int i = 0; i < 4; i++) {
            saq.offer(i);
        }
        for (int i = 0; i < 4; i++) {
            assertEquals(i, saq.poll());
        }
        for (int i = 4; i < 16; i++) {
            saq.offer(i);
        }
        for (int i = 4; i < 16; i++) {
            assertEquals(i, saq.poll());
        }
    }

    @Test
    public void testCapacityLimit() {
        Queue<Object> aaq = new AtomicArrayQueueUnsafe(8, 16);
        for (int i = 0; i < 16; i++) {
            assertTrue(aaq.offer(i));
        }
        assertFalse(aaq.offer(16));
        
        assertEquals(0, aaq.poll());

        assertTrue(aaq.offer(16));

    }
    
    static void await(CyclicBarrier cb) {
        try {
            cb.await();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (BrokenBarrierException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Test(timeout = 10000)
    public void testConcurrentBehavior() throws InterruptedException {
        for (int j = 0; j < 50; j++) {
            final Queue<Object> aaq = new AtomicArrayQueueUnsafe(8, 64 * 1024);
            
            final AtomicBoolean continuous = new AtomicBoolean(true);
            
            final int m = 1000 * 1000;
            
            final CyclicBarrier cb = new CyclicBarrier(2);
            
            Thread t1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    await(cb);
                    for (int i = 0; i < m; i++) {
                        aaq.offer(i);
                    }
                    System.out.println("Offer done.");
                }
            });
    
            Thread t2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    await(cb);
                    int last = -1;
                    while (!Thread.currentThread().isInterrupted() && last + 1 == m) {
                        Integer o = (Integer)aaq.poll();
                        if (o != null) {
                            int last0 = last;
                            last = o;
                            if (last0 + 1 != o) {
                                System.out.println("Discontinuity! " + last0 + " -> " + last);
                                continuous.set(false);
                                return;
                            }
                        }
                    }
                    System.out.println("Poll done.");
                }
            });
            
            t1.start();
            t2.start();
    
            t1.join();
            t2.join();
            
            assertTrue("Discontinuity!", continuous.get());
        }
    }
    @Test
    public void asRingBufferAddRemove1000() throws InterruptedException, MissingBackpressureException {
        RxRingBuffer ring = new RxRingBuffer(new AtomicArrayQueueUnsafe(4, RxRingBuffer.SIZE), RxRingBuffer.SIZE, true);
        for (int i = 0; i < RxRingBuffer.SIZE; i++) {
            ring.onNext("a");
        }
        for (int i = 0; i < RxRingBuffer.SIZE; i++) {
            assertEquals("a", ring.poll());
        }
    }
    @Test
    public void testSize() {
        AtomicArrayQueue queue = new AtomicArrayQueue(4, 16);
        for (int i = 0; i < 2; i++) {
            assertTrue(queue.offer(i));
        }
        assertEquals(2, queue.size());
    }
    @Test
    public void testSizeAfterGrowth() {
        AtomicArrayQueue queue = new AtomicArrayQueue(4, 16);
        for (int i = 0; i < 12; i++) {
            assertTrue(queue.offer(i));
        }
        assertEquals(12, queue.size());
    }
    @Test
    public void testPeek() {
        AtomicArrayQueue queue = new AtomicArrayQueue(4, 16);
        queue.offer(1);
        for (int i = 0; i < 10; i++) {
            assertEquals(1, queue.peek());
        }
        assertEquals(1, queue.poll());
        for (int i = 0; i < 10; i++) {
            assertEquals(null, queue.peek());
        }
        assertEquals(null, queue.poll());
    }
    @Test
    public void testPeekBeforeAfterGrow() {
        AtomicArrayQueue queue = new AtomicArrayQueue(4, 16);
        queue.offer(1);
        queue.offer(2);
        queue.offer(3);
        queue.offer(4);
        
        assertEquals(1, queue.peek());

        queue.offer(5);
        queue.offer(6);
        queue.offer(7);
        queue.offer(8);
        
        queue.poll();
        queue.poll();
        queue.poll();
        queue.poll();
        
        assertEquals(5, queue.peek());
    }
}
