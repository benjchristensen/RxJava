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
    static final int indexPad = 16; // 64 bytes between longs
    static final int indexShift = 4; // 64 bytes between array elements
    static final int HIGH_TRAFFIC = 128;
    final int smallMask;
    final int largeMask;
    long[] readerIndex;
    long[] writerIndex;
    final AtomicReference<AtomicReferenceArray<Object>> buffer;
    
    public AtomicArrayQueue(int initial, int maxCapacity) {
        int is = Pow2.roundToPowerOfTwo(initial);
        int ms = Pow2.roundToPowerOfTwo(maxCapacity);
        
        this.smallMask = is - 1;
        this.largeMask = ms - 1;
        
        readerIndex = new long[1];
        writerIndex = new long[1];
        AtomicReferenceArray<Object> b = new AtomicReferenceArray<Object>(is);
        buffer = new AtomicReference<AtomicReferenceArray<Object>>(b);
    }
    
    long lpReaderIndexSmall() {
        return readerIndex[0];
    }
    void spReaderIndexSmall(long value) {
        readerIndex[0] = value;
    }

    long lpWriterIndexSmall() {
        return writerIndex[0];
    }
    void spWriterIndexSmall(long value) {
        writerIndex[0] = value;
    }

    
    long lpReaderIndexLarge() {
        if (readerIndex.length == 1) {
            long ri = readerIndex[0];
            readerIndex = new long[2 * indexPad];
            readerIndex[indexPad] = ri;
            return ri;
        }
        return readerIndex[indexPad];
    }
    long lpWriterIndexLarge() {
        return writerIndex[indexPad];
    }
    
    void spReaderIndexLarge(long value) {
        readerIndex[indexPad] = value;
    }
    void spWriterIndexLarge(long value) {
        writerIndex[indexPad] = value;
    }
    
    AtomicReferenceArray<Object> lvBuffer() {
        return buffer.get();
    }
    void soBuffer(AtomicReferenceArray<Object> b) {
        buffer.lazySet(b);
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
        return indexPad + ((int) index & mask) << indexShift;
    }
    
    AtomicReferenceArray<Object> grow(AtomicReferenceArray<Object> b, long wi, int wo, int smallMask, int largeMask) {
        int len2 = indexPad * 2 + (largeMask + 1) << indexShift;
        AtomicReferenceArray<Object> b2 = new AtomicReferenceArray<Object>(len2);
        
        boolean caughtUp = false;
        int j = offsetLarge(wi, largeMask);
        for (int i = wo - 1; i >= 0; i--, j -= indexPad) {
            Object o = lvElement(b, i);
            if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                caughtUp = true;
                break;
            }
            soElement(b2, j, o);
        }
        if (!caughtUp) {
            for (int i = largeMask; i >= wo; i--, j -= indexPad) {
                Object o = lvElement(b, i);
                if (o == null || !casElement(b, i, o, TOMBSTONE)) {
                    break;
                }
                soElement(b2, j, o);
            }
        }
        
        writerIndex = new long[2 * indexPad];
        spWriterIndexLarge(wi);
        soBuffer(b2);
        
        return b2;
    }
    
    @Override
    public boolean offer(Object o) {
        AtomicReferenceArray<Object> b = lvBuffer();
        int lm = largeMask;
        if (b.length() > lm + 1) {
            long wi = lpWriterIndexLarge();
            int wo = offsetLarge(wi, lm);
            if (lvElement(b, wo) != null) {
                return false;
            }
            soElement(b, wo, o);
            spWriterIndexLarge(wi + 1);
        } else {
            int sm = smallMask;
            long wi = lpWriterIndexSmall();
            int wo = offsetSmall(wi, sm);
            if (lvElement(b, wo) != null || wi >= HIGH_TRAFFIC) {
                b = grow(b, wi, wo, sm, lm);
                wo = offsetLarge(wi, lm);
                soElement(b, wo, o);
                spWriterIndexLarge(wi + 1);
            } else {
                soElement(b, wo, o);
                spWriterIndexSmall(wi + 1);
            }
        }
        return true;
    }
    @Override
    public Object poll() {
        int lm = largeMask;
        int sm = smallMask;
        for (;;) {
            AtomicReferenceArray<Object> b = lvBuffer();
            if (b.length() > lm + 1) {
                long ri = lpReaderIndexLarge();
                int ro = offsetLarge(ri, lm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return null;
                }
                soElement(b, ro, null);
                spReaderIndexLarge(ri + 1);
            } else {
                long ri = lpReaderIndexSmall();
                int ro = offsetSmall(ri, sm);
                Object o = lvElement(b, ro);
                if (o == null) {
                    return null;
                } else
                if (o == TOMBSTONE || !casElement(b, ro, o, null)) {
                    continue;
                }
                spReaderIndexSmall(ri + 1);
                return o;
            }
        }
    }
    @Override
    public Object peek() {
        throw new UnsupportedOperationException();
    }
    @Override
    public Iterator<Object> iterator() {
        throw new UnsupportedOperationException();
    }
    @Override
    public int size() {
        return 0;
    }
}
