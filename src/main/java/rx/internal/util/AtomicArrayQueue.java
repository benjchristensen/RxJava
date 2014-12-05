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

import rx.internal.util.unsafe.Pow2;

/**
 * 
 */
public class AtomicArrayQueue extends AbstractQueue<Object> {
    static final Object TOMBSTONE = new Object();
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
    }
    final int smallMask;
    final int largeMask;
    volatile AtomicReferenceArray<Object> buffer;
    long readerIndex;
    long writerIndex;
    public AtomicArrayQueue(int initial, int maxCapacity) {
        int is = Pow2.roundToPowerOfTwo(initial);
        int ms = Pow2.roundToPowerOfTwo(maxCapacity);
        
        this.smallMask = is - 1;
        this.largeMask = ms - 1;
    
        buffer = new AtomicReferenceArray<Object>(is);
    }
    
    long lpReaderIndex() {
        return readerIndex;
    }
    void spReaderIndex(long value) {
        readerIndex = value;
    }

    long lpWriterIndex() {
        return writerIndex;
    }
    void spWriterIndex(long value) {
        writerIndex = value;
    }

    AtomicReferenceArray<Object> lvBuffer() {
        return buffer;
    }
    void soBuffer(AtomicReferenceArray<Object> b) {
        buffer = b;
    }
    
    Object lvElement(AtomicReferenceArray<Object> b, int offset) {
        return b.get(offset);
    }
    void soElement(AtomicReferenceArray<Object> b, int offset, Object value) {
        b.lazySet(offset, value);
    }
    boolean casElement(AtomicReferenceArray<Object> b, int offset, Object expected, Object value) {
        return b.compareAndSet(offset, expected, value);
    }
    
    int offsetSmall(long index, int mask) {
        return ((int) index & mask);
    }
    
    int offsetLarge(long index, int mask) {
        return ((int) index & mask);
    }
    
    AtomicReferenceArray<Object> grow(AtomicReferenceArray<Object> b, long wi, int wo, int smallMask, int largeMask) {
        int len2 = (largeMask + 1);
        AtomicReferenceArray<Object> b2 = new AtomicReferenceArray<Object>(len2);
        
        boolean caughtUp = false;
        int j = offsetLarge(wi - 1, largeMask);
        for (int i = wo - 1; i >= 0; i--, j--) {
            Object o = lvElement(b, i);
            if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                caughtUp = true;
                break;
            }
            soElement(b2, j, o);
        }
        if (!caughtUp) {
            for (int i = smallMask; i >= wo; i--, j--) {
                Object o = lvElement(b, i);
                if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                    break;
                }
                soElement(b2, j, o);
            }
        }
        
        soBuffer(b2);
        
        return b2;
    }
    
    @Override
    public boolean offer(Object o) {
        long wi = lpWriterIndex();
        AtomicReferenceArray<Object> b = lvBuffer();
        int lm = largeMask;
        int bl = b.length();
        if (bl > lm) {
            int wo = offsetLarge(wi, lm);
            if (lvElement(b, wo) != null) {
                return false;
            }
            soElement(b, wo, o);
        } else {
            int sm = smallMask;
            int wo = offsetSmall(wi, sm);
            if (lvElement(b, wo) != null || wi >= HIGH_TRAFFIC_QUEUE_THRESHOLD) {
                b = grow(b, wi, wo, sm, lm);
                wo = offsetLarge(wi, lm);
                soElement(b, wo, o);
            } else {
                soElement(b, wo, o);
            }
        }
        spWriterIndex(wi + 1);
        return true;
    }
    @Override
    public Object poll() {
        int lm = largeMask;
        int sm = smallMask;
        long ri = lpReaderIndex();
        for (;;) {
            AtomicReferenceArray<Object> b = lvBuffer();
            if (b.length() > lm) {
                int ro = offsetLarge(ri, lm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return null;
                }
                soElement(b, ro, null);
                spReaderIndex(ri + 1);
                return o;
            } else {
                int ro = offsetSmall(ri, sm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return null;
                } else
                if (o == TOMBSTONE || !casElement(b, ro, o, null)) {
                    continue;
                }
                spReaderIndex(ri + 1);
                return o;
            }
        }
    }
    @Override
    public Object peek() {
        int lm = largeMask;
        int sm = smallMask;
        long ri = lpReaderIndex();
        for (;;) {
            AtomicReferenceArray<Object> b = lvBuffer();
            if (b.length() > lm) {
                int ro = offsetLarge(ri, lm);
                Object o = lvElement(b, ro);
                return o;
            } else {
                int ro = offsetSmall(ri, sm);
                Object o = lvElement(b, ro);
                if (o != TOMBSTONE) {
                    return o;
                }
            }
        }
    }
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
    @Override
    public int size() {
        int result = 0;
        int lm = largeMask;
        int sm = smallMask;
        long ri = lpReaderIndex();
        AtomicReferenceArray<Object> b = lvBuffer();
        for (;;) {
            if (b.length() > lm) {
                int ro = offsetLarge(ri, lm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return result;
                }
                result++;
                ri++;
            } else {
                int ro = offsetSmall(ri, sm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return result;
                } else
                if (o == TOMBSTONE) {
                    b = lvBuffer();
                } else {
                    result++;
                    ri++;
                }
            }
        }
    }
}
