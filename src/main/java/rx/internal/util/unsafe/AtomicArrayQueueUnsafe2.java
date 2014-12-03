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

package rx.internal.util.unsafe;

import java.util.*;

import rx.internal.util.PlatformDependent;

/**
 * A single-producer single-consumer circular array which resizes to a higher capacity once it fills
 * its smaller buffer.
 */
public class AtomicArrayQueueUnsafe2 extends AbstractQueue<Object> {
    static final Object TOMBSTONE = new Object();
    static final long P_BUFFER;
    static final long ARRAY_OFFSET;
    static final int ARRAY_SCALE;
    static final int ARRAY_INDEX_SIZE;
    static final long HIGH_TRAFFIC_QUEUE_THRESHOLD;
    static {
        long _size = 128;
        if (PlatformDependent.isAndroid()) {
            _size = Long.MAX_VALUE;
        }

        // possible system property for overriding
        String sizeFromProperty = System.getProperty("rx.ring-buffer.resize-traffic");
        if (sizeFromProperty != null) {
            try {
                _size = Integer.parseInt(sizeFromProperty);
            } catch (Exception e) {
                System.err.println("Failed to set 'rx.ring-buffer.resize-traffic' with value " + sizeFromProperty + " => " + e.getMessage());
            }
        }
        HIGH_TRAFFIC_QUEUE_THRESHOLD = _size;
        
        try {
            P_BUFFER = UnsafeAccess.UNSAFE.objectFieldOffset(AtomicArrayQueueUnsafe2.class.getDeclaredField("buffer"));
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException();
        }
        int is = UnsafeAccess.UNSAFE.arrayIndexScale(Object[].class);
        int as;
        if (is == 4) {
            as = 2;
        } else
            if (is == 8) {
                as = 3;
            } else {
                throw new RuntimeException("Unsupported array index scale: " + is);
            }

        ARRAY_SCALE = as;
        ARRAY_INDEX_SIZE = is;
        ARRAY_OFFSET = UnsafeAccess.UNSAFE.arrayBaseOffset(Object[].class);
    }
    Object[] buffer;
    long readerIndex;
    long writerIndex;
    final int maxCapacity;
    private Object[] lpBuffer() {
        return (Object[])UnsafeAccess.UNSAFE.getObject(this, P_BUFFER);
    }
    private Object[] lvBuffer() {
        return (Object[])UnsafeAccess.UNSAFE.getObjectVolatile(this, P_BUFFER);
    }
    private void soBuffer(Object[] buffer) {
        UnsafeAccess.UNSAFE.putOrderedObject(this, P_BUFFER, buffer);
    }
    private Object lvElement(Object[] buffer, long offset) {
        return UnsafeAccess.UNSAFE.getObjectVolatile(buffer, offset);
    }
    private void soElement(Object[] buffer, long offset, Object value) {
        UnsafeAccess.UNSAFE.putOrderedObject(buffer, offset, value);
    }
    private boolean casElement(Object[] buffer, long offset, Object expected, Object value) {
        return UnsafeAccess.UNSAFE.compareAndSwapObject(buffer, offset, expected, value);
    }

    private long calcOffset(long index, int mask) {
        return ARRAY_OFFSET + ((index & mask) << ARRAY_SCALE);
    }
    static int roundToPowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    public AtomicArrayQueueUnsafe2(int initialCapacity, int maxCapacity) {
        int c0;
        if (initialCapacity >= 1 << 30) {
            c0 = 1 << 30;
        } else {
            c0 = roundToPowerOfTwo(initialCapacity);
        }
        int cm;
        if (maxCapacity >= 1 << 30) {
            cm = 1 << 30;
        } else {
            cm = roundToPowerOfTwo(maxCapacity);
        }
        this.maxCapacity = cm;

        soBuffer(new Object[c0]);;
    }

    Object[] grow(Object[] b, long wo, int n, int n2) {

        int arrayScale = ARRAY_SCALE;
        int arrayIndexSize = ARRAY_INDEX_SIZE;
        long arrayOffset = ARRAY_OFFSET;
        Object[] b2 = new Object[n2];

        // move the front to the back
        boolean caughtUp = false;
        for (long i = wo - arrayIndexSize, j = wo + (n << arrayScale); i >= arrayOffset; i -= arrayIndexSize, j -= arrayIndexSize) {
            Object o = lvElement(b, i);
            if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                caughtUp = true;
                break;
            } else
            soElement(b2, j, o);
        }
        // leave everything beyont the wrap point at its location
        if (!caughtUp) {
            long no = arrayOffset + (n << arrayScale);
            for (long i = no; i >= wo; i -= arrayIndexSize) {
                Object o = lvElement(b, i);
                if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                    break;
                }
                soElement(b2, i, o);
            }
        }

        soBuffer(b2);

        return b2;
    }

    @Override
    public boolean offer(Object o) {
        long wi = writerIndex;

        Object[] b = lpBuffer();

        int n = b.length - 1;
        long wo = calcOffset(wi, n);
        if (lvElement(b, wo) != null || wi >= HIGH_TRAFFIC_QUEUE_THRESHOLD) {
            int q = maxCapacity;
            if (n + 1 == q) {
                return false;
            }
            // grow
            b = grow(b, wo, n, q);
            wo = calcOffset(wi, q - 1);
        }
        soElement(b, wo, o);

        writerIndex = wi + 1;

        return true;
    }
    @Override
    public Object poll() {
        long ri = readerIndex;
        Object[] b = lpBuffer();
        for (;;) {
            int mask = b.length - 1;
            long ro = calcOffset(ri, mask);

            Object o = lvElement(b, ro);
            if (o == null) {
                return null;
            }
            if (mask + 1 == maxCapacity) {
                soElement(b, ro, null);
            } else
            if (o == TOMBSTONE || !casElement(b, ro, o, null)) {
                b = lvBuffer();
                continue;
            }
            readerIndex = ri + 1;
            return o;
        }
    }
    @Override
    public Object peek() {
        long ri = readerIndex;
        Object[] b = lpBuffer();
        for (;;) {
            int mask = b.length - 1;
            long ro = calcOffset(ri, mask);

            Object o = lvElement(b, ro);
            if (o != TOMBSTONE) {
                return o;
            }
            b = lvBuffer();
        }
    }
    @Override
    public boolean isEmpty() {
        return peek() == null;
    }
    @Override
    public int size() {
        int size = 0;
        long ri = readerIndex;
        Object[] b = lpBuffer();
        for (;;) {
            int mask = b.length - 1;
            long ro = calcOffset(ri, mask);
            Object o = lvElement(b, ro);
            if (o == null) {
                return size;
            } else
            if (o == TOMBSTONE) {
                b = lvBuffer();
                continue;
            }
            size++;
            ri++;
        }
    }
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
}
