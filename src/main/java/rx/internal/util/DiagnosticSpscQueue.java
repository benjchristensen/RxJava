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
    public DiagnosticSpscQueue(Queue<T> actual) {
        this.actual = actual;
        this.readers = new AtomicInteger();
        this.writers = new AtomicInteger();
        this.readerThread = new AtomicReference<Thread>();
        this.writerThread = new AtomicReference<Thread>();
        this.watchThread = true;
    }
    void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            // ignored
        }
    }
    void writerEnter() {
        int wc = writers.incrementAndGet();
        if (wc != 0) {
            writers.decrementAndGet();
            throw new IllegalStateException("Multiple writers on enter: " + wc);
        }
        sleep();
    }
    void writerLeave() {
        int wc = writers.decrementAndGet();
        if (wc != 0) {
            throw new IllegalStateException("Multiple writers on enter: " + wc);
        }
    }
    void readerEnter() {
        int rc = readers.incrementAndGet();
        if (rc != 0) {
            throw new IllegalStateException("Multiple readers on enter: " + rc);
        }
        sleep();
    }
    void readerLeave() {
        int rc = readers.decrementAndGet();
        if (rc != 0) {
            throw new IllegalStateException("Multiple writers on enter: " + rc);
        }
    }
    @Override
    public boolean offer(T e) {
        try {
            return actual.offer(e);
        } finally {
        }
    }
    @Override
    public T poll() {
        try {
            return actual.poll();
        } finally {
        }
    }
    @Override
    public T peek() {
        try {
            return actual.peek();
        } finally {
        }
    }
    @Override
    public Iterator<T> iterator() {
        return actual.iterator();
    }
    @Override
    public int size() {
        try {
            return actual.size();
        } finally {
        }
    }
    
}
