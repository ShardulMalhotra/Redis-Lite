package com.shardul.redislite;

import com.shardul.redislite.persistence.AofLog;
import com.shardul.redislite.store.CacheStore;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class CacheStoreTest {

    @Test
    void basicSetAndGet() {
        CacheStore store = new CacheStore(10);
        store.set("k1", "v1", 0);
        assertEquals("v1", store.get("k1"));
    }

    @Test
    void lruEvictsLeastRecentlyUsed() {
        CacheStore store = new CacheStore(2); // capacity of 2

        store.set("a", "1", 0);
        store.set("b", "2", 0);
        store.get("a");            // "a" is now most-recently-used; "b" is LRU
        store.set("c", "3", 0);    // should evict "b", not "a"

        assertEquals("1", store.get("a"), "a should survive — it was accessed recently");
        assertNull(store.get("b"), "b should be evicted — it was the least recently used");
        assertEquals("3", store.get("c"));
    }

    @Test
    void ttlExpiryRemovesKeyLazily() throws InterruptedException {
        CacheStore store = new CacheStore(10);
        store.set("temp", "value", 1); // 1 second TTL

        assertEquals("value", store.get("temp"));
        Thread.sleep(1100);
        assertNull(store.get("temp"), "key should be expired after its TTL elapses");
    }

    @Test
    void concurrentIncrementsProduceCorrectFinalCount() throws Exception {
        CacheStore store = new CacheStore(100);
        AofLog aof = new AofLog("test-concurrent.aof");
        CommandProcessor processor = new CommandProcessor(store, aof);
        processor.process("SET counter 0", true);

        int threads = 20;
        int incrementsPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                for (int j = 0; j < incrementsPerThread; j++) {
                    processor.process("INCR counter", true);
                }
                latch.countDown();
            });
        }

        latch.await();
        pool.shutdown();
        aof.close();

        int expected = threads * incrementsPerThread;
        assertEquals(String.valueOf(expected), store.get("counter"),
                "no updates should be lost even under heavy concurrent access");

        new java.io.File("test-concurrent.aof").delete();
    }
}
