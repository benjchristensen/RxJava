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

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Queue that wraps the SPSC methods to check if they are used concurrently
 * and throws an exception if so.
 * @param <T> the element type
 */
public final class DiagnosticSpscQueue<T> extends AbstractQueue<T> {
    final Queue<T> actual;
    final AtomicInteger writers;
    final AtomicInteger readers;
    final AtomicReference<Thread> readerThread;
    final AtomicReference<Thread> writerThread;
    final boolean watchThread;
    public DiagnosticSpscQueue(Queue<T> actual, boolean watchThread) {
        this.actual = actual;
        this.readers = new AtomicInteger();
        this.writers = new AtomicInteger();
        this.readerThread = new AtomicReference<Thread>();
        this.writerThread = new AtomicReference<Thread>();
        this.watchThread = watchThread;
    }
    void sleep() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ex) {
            // ignored
        }
    }
    void writerEnter() {
        if (watchThread) {
            for (;;) {
                Thread t = writerThread.get();
                if (t != null) {
                    System.out.println(Arrays.toString(t.getStackTrace()));
                    throw new IllegalStateException("Another writer thread: " + t + " (we are " + Thread.currentThread() + ")");
                }
                if (writerThread.compareAndSet(null, Thread.currentThread())) {
                    break;
                }
            }
        } else {
            int wc = writers.getAndIncrement();
            if (wc != 0) {
                throw new IllegalStateException("Multiple writers on enter: " + wc);
            }
        }
//        sleep();
    }
    void writerLeave() {
        if (watchThread) {
            writerThread.set(null);
        } else {
            int wc = writers.decrementAndGet();
            if (wc != 0) {
                throw new IllegalStateException("Multiple writers on enter: " + wc);
            }
        }
    }
    void readerEnter() {
        if (watchThread) {
            for (;;) {
                Thread t = readerThread.get();
                if (t != null) {
                    System.out.println(Arrays.toString(t.getStackTrace()));
                    throw new IllegalStateException("Another writer thread: " + t + " (we are " + Thread.currentThread() + ")");
                }
                if (readerThread.compareAndSet(null, Thread.currentThread())) {
                    break;
                }
            }
        } else {
            int rc = readers.getAndIncrement();
            if (rc != 0) {
                throw new IllegalStateException("Multiple readers on enter: " + rc);
            }
        }
//        sleep();
    }
    void readerLeave() {
        if (watchThread) {
            readerThread.set(null);
        } else {
            int rc = readers.decrementAndGet();
            if (rc != 0) {
                throw new IllegalStateException("Multiple writers on enter: " + rc);
            }
        }
    }
    @Override
    public boolean offer(T e) {
        writerEnter();
        try {
            return actual.offer(e);
        } finally {
            writerLeave();
        }
    }
    @Override
    public T poll() {
        readerEnter();
        try {
            return actual.poll();
        } finally {
            readerLeave();
        }
    }
    @Override
    public T peek() {
        readerEnter();
        try {
            return actual.peek();
        } finally {
            readerLeave();
        }
    }
    @Override
    public Iterator<T> iterator() {
        return actual.iterator();
    }
    @Override
    public int size() {
        readerEnter();
        try {
            return actual.size();
        } finally {
            readerLeave();
        }
    }
    
}
