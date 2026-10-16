/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
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

package org.teiid.common.buffer.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.BufferReserveMode;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.CacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.impl.BufferManagerImpl.BatchManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;

@SuppressWarnings("nls")
public class TestBufferManagerImpl {

    @Test public void testReserve() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.setMaxProcessingKB(1024);
        bufferManager.setMaxReserveKB(1024);
        bufferManager.initialize();
        bufferManager.setNominalProcessingMemoryMax(512000);

        //restricted by nominal max
        assertEquals(512000, bufferManager.reserveBuffers(1024000, BufferReserveMode.NO_WAIT));
        //forced
        assertEquals(1024000, bufferManager.reserveBuffersBlocking(1024000, new long[] {0,0}, true));

        //not forced, so we get noting
        assertEquals(0, bufferManager.reserveBuffersBlocking(1024000, new long[] {0,0}, false));

        bufferManager.releaseBuffers(512000);
        //the difference between 1mb and 1000k
        assertEquals(24576, bufferManager.reserveBuffers(1024000, BufferReserveMode.NO_WAIT));
    }

    @Test public void testLargeReserve() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.setMaxReserveKB((1<<22) + 11);
        assertEquals(4194315, bufferManager.getMaxReserveKB());
    }

    @Test(expected=IOException.class) public void testFileStoreMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 59; //only about 1/6th will be usable by a single file store
            }
        });
        bufferManager.setMaxActivePlans(20);
        bufferManager.initialize();
        FileStore fs = bufferManager.createFileStore("x");
        fs.write(new byte[10], 0, 10);
    }

    @Test(expected=TeiidComponentException.class) public void testTupleBufferMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 640;
            }
        });
        bufferManager.setMaxReserveKB(10);
        bufferManager.setMaxActivePlans(20);
        bufferManager.setEnforceMaxBatchManagerSizeEstimate(true);
        bufferManager.initialize();
        TupleBuffer tb = bufferManager.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, String.class)), "x", TupleSourceType.PROCESSOR);
        //fill one batch, which should then exceed the max
        for (int i = 0; i < 1024; i++) {
            tb.addTuple(Arrays.asList("a"));
        }
    }

    @Test public void testTupleBufferSessionMax() throws Exception {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager() {
            @Override
            public long getMaxStorageSpace() {
                return 64000;
            }
        });
        bufferManager.setMaxReserveKB(10);
        bufferManager.setMaxActivePlans(10);
        bufferManager.setOptions(new Options().maxSessionBufferSizeEstimate(100000));
        bufferManager.initialize();
        CommandContext context = new CommandContext();
        context.setSession(new SessionMetadata());
        CommandContext.pushThreadLocalContext(context);
        try {
            List<TupleBuffer> tupleBuffers = new ArrayList<TupleBuffer>();
            for (int i = 0; i < 36; i++) {
                TupleBuffer tb = bufferManager.createTupleBuffer(Arrays.asList(new ElementSymbol("x", null, String.class)), "x", TupleSourceType.PROCESSOR);
                try {
                    for (int j = 0; j < 50; j++) {
                        tb.addTuple(Arrays.asList("a"));
                    }
                    tb.saveBatch();
                    if (i%2==0) {
                        tb.remove();
                    }
                } catch (TeiidComponentException e) {
                    assertEquals(34, i);
                    return;
                }
                tupleBuffers.add(tb);
            }
        } finally {
            CommandContext.popThreadLocalContext();
        }
        fail();
    }

    @Test
    public void testProcessorBatchSize(){
        BufferManager bm = BufferManagerFactory.createBufferManager();

        int processorBatchSize = bm.getProcessorBatchSize();

        List<ElementSymbol> elements = new ArrayList<ElementSymbol>();
        ElementSymbol a = new ElementSymbol("a");
        a.setType(DataTypeManager.DefaultDataClasses.INTEGER);

        //we use a somewhat high estimate of string size
        ElementSymbol b = new ElementSymbol("b");
        b.setType(DataTypeManager.DefaultDataClasses.STRING);

        elements.add(a);
        //fixed/small
        assertEquals(processorBatchSize * 8, bm.getProcessorBatchSize(elements));

        elements.add(b);
        //small
        assertEquals(processorBatchSize * 4, bm.getProcessorBatchSize(elements));

        elements.add(b);
        //moderately small
        assertEquals(processorBatchSize * 2, bm.getProcessorBatchSize(elements));

        elements.add(b);
        elements.add(b);
        //"normal"
        assertEquals(processorBatchSize, bm.getProcessorBatchSize(elements));

        elements.addAll(Collections.nCopies(28, b));
        //large
        assertEquals(processorBatchSize/2, bm.getProcessorBatchSize(elements));

        elements.addAll(Collections.nCopies(100, b));
        //huge
        assertEquals(processorBatchSize/4, bm.getProcessorBatchSize(elements));

        elements.addAll(Collections.nCopies(375, b));
        //extreme
        assertEquals(processorBatchSize/8, bm.getProcessorBatchSize(elements));
    }

    /**
     * DTS-89949: doEvictions' age-out branch used to read the entry's last-access time and
     * remove it from the eviction queue outside of synchronized(ce), racing with the
     * synchronized touch() done by a concurrent fastGet(retain=true). A touch that lands in
     * that window re-adds the entry under a new key, and the unsynchronized removal can then
     * read that new key and undo the touch's protection - leaving the entry in memoryEntries
     * with no corresponding entry in either eviction queue, an "orphan" that can never be
     * evicted again (the symptom logged as "Detected an unexpected number of orphaned heap
     * cache memory entries").
     *
     * This test forces that exact interleaving deterministically: it intercepts the age-out
     * branch's queue.remove(ce) call right before it reads the entry's key, lets a concurrent
     * "touch" run to completion, and only then lets the removal proceed. On unpatched code the
     * touch's re-add is visible to - and undone by - the unsynchronized removal, producing an
     * orphan. The fix's synchronized(ce) block instead forces the touch to wait until the
     * removal (and the key read inside it) has completed, so no entry is lost.
     */
    @Test(timeout = 20000) public void testConcurrentAgeOutEvictionRace() throws Exception {
        final BufferManagerImpl bufferManager = new BufferManagerImpl() {
            @Override
            boolean evict(CacheEntry ce) throws Exception {
                //simulate a concurrent persist that has not completed yet, as can
                //happen under the disk-pressure/high-concurrency conditions in DTS-89949
                return false;
            }
        };
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.initialize();

        //cancel the periodic background cleaner so it doesn't interfere with this
        //test's precise thread choreography
        Field cleanerField = BufferManagerImpl.class.getDeclaredField("cleaner");
        cleanerField.setAccessible(true);
        ((TimerTask) cleanerField.get(bufferManager)).cancel();

        Field clockField = BufferManagerImpl.class.getDeclaredField("readAttempts");
        clockField.setAccessible(true);
        AtomicLong clock = (AtomicLong) clockField.get(bufferManager);

        //intercepts the age-out branch's queue.remove(ce) call so a concurrent touch can be
        //forced to run to completion at the exact point the unsynchronized code reads the key
        class InterceptingEvictionQueue extends LrfuEvictionQueue<CacheEntry> {
            final CountDownLatch readyToRemove = new CountDownLatch(1);
            final CountDownLatch proceedWithRemove = new CountDownLatch(1);
            final AtomicBoolean armed = new AtomicBoolean(true);

            InterceptingEvictionQueue(AtomicLong clock) {
                super(clock);
            }

            @Override
            public boolean remove(CacheEntry value) {
                if (armed.compareAndSet(true, false)) {
                    readyToRemove.countDown();
                    try {
                        proceedWithRemove.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return super.remove(value);
            }
        }
        final InterceptingEvictionQueue interceptingQueue = new InterceptingEvictionQueue(clock);
        bufferManager.evictionQueue = interceptingQueue;

        final CacheEntry ce = new CacheEntry(new CacheKey(0L, 0, 0), 1, new Object(), null, true);
        bufferManager.memoryEntries.put(ce.getId(), ce);
        interceptingQueue.add(ce);
        clock.set(1 << 20); //now older than MAX_READ_AGE (1<<19) relative to its lastAccess of 0

        Thread evictor = new Thread() {
            public void run() {
                bufferManager.doEvictions(Long.MAX_VALUE, true, bufferManager.evictionQueue);
            }
        };
        evictor.start();

        assertTrue("evictor did not reach the age-out removal",
                interceptingQueue.readyToRemove.await(10, TimeUnit.SECONDS));

        //simulates a concurrent read that touches (re-protects) the same entry - exactly the
        //"someone is actively using this batch" scenario from the ticket
        Thread toucher = new Thread() {
            public void run() {
                bufferManager.fastGet(0L, true, true);
            }
        };
        toucher.start();
        //should complete quickly, unless the fix's synchronization is blocking it on the
        //evictor's monitor - either way we let the removal proceed next
        toucher.join(500);

        interceptingQueue.proceedWithRemove.countDown();

        evictor.join(10000);
        toucher.join(10000);
        assertFalse("evictor thread did not finish", evictor.isAlive());
        assertFalse("toucher thread did not finish", toucher.isAlive());

        assertEquals("Heap cache entry diverged from the eviction queues - an orphaned entry that can never be evicted",
                bufferManager.memoryEntries.size(),
                bufferManager.evictionQueue.getSize() + bufferManager.initialEvictionQueue.getSize());
    }

    /**
     * DTS-89949: doEvictions() initialized its local "evicted" flag to true and only
     * reassigned it from evict(ce)'s return value - so if evict() threw (e.g. a disk write
     * that fails partway through, plausible under the same disk-pressure conditions as the
     * synchronization race above), the exception was logged but the entry was still treated
     * as successfully evicted and dropped from memoryEntries, even though nothing was
     * actually persisted.
     */
    @Test public void testEvictionExceptionDoesNotDropEntry() throws Exception {
        final BufferManagerImpl bufferManager = new BufferManagerImpl() {
            @Override
            boolean evict(CacheEntry ce) throws Exception {
                //simulate a disk write that fails partway through, as can happen under
                //the disk-pressure conditions in DTS-89949
                throw new IOException("simulated disk write failure");
            }
        };
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.initialize();

        //cancel the periodic background cleaner so it doesn't race with this test
        Field cleanerField = BufferManagerImpl.class.getDeclaredField("cleaner");
        cleanerField.setAccessible(true);
        ((TimerTask) cleanerField.get(bufferManager)).cancel();

        Field clockField = BufferManagerImpl.class.getDeclaredField("readAttempts");
        clockField.setAccessible(true);
        AtomicLong clock = (AtomicLong) clockField.get(bufferManager);
        clock.set(1 << 20); //older than MAX_READ_AGE (1<<19) relative to a lastAccess of 0

        CacheEntry ce = new CacheEntry(new CacheKey(0L, 0, 0), 1, new Object(), null, true);
        bufferManager.memoryEntries.put(ce.getId(), ce);
        bufferManager.evictionQueue.add(ce);

        long freed = bufferManager.doEvictions(Long.MAX_VALUE, true, bufferManager.evictionQueue);

        assertEquals("nothing should be reported as freed when the write failed", 0, freed);
        assertTrue("entry must not be dropped from memoryEntries when evict() throws",
                bufferManager.memoryEntries.containsKey(0L));
    }

    @Test public void testRemovedException() throws TeiidComponentException {
        BufferManagerImpl bufferManager = new BufferManagerImpl();
        bufferManager.setCache(new MemoryStorageManager());
        bufferManager.initialize();
        BatchManagerImpl batchManager = bufferManager.createBatchManager(1L, new Class<?>[] {Integer.class});
        batchManager.describe(null); //should not error
        batchManager.remove();
        try {
            batchManager.getBatch(1L, false);
            fail();
        } catch (TeiidComponentException e) {

        }
    }

}
