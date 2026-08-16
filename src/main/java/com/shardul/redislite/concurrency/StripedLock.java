package com.shardul.redislite.concurrency;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock striping: instead of one global lock (which would serialize EVERY
 * INCR/DECR across every key — a huge bottleneck) or a lock-per-key
 * (which wastes memory and is hard to garbage-collect safely), we hash
 * each key to one of a fixed number of lock "stripes".
 *
 * Two different keys will usually map to two different locks and can be
 * modified truly concurrently. Two keys that happen to hash to the same
 * stripe will be serialized against each other — a rare, acceptable
 * false-contention cost in exchange for O(1) memory and no per-key
 * lock lifecycle management.
 *
 * This is the same idea Java's own ConcurrentHashMap used internally
 * pre-Java 8 (segment locking) and that Guava's Striped<Lock> formalizes.
 */
public class StripedLock {

    private final ReentrantLock[] locks;

    public StripedLock(int numStripes) {
        locks = new ReentrantLock[numStripes];
        for (int i = 0; i < numStripes; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    public ReentrantLock forKey(String key) {
        int index = Math.floorMod(key.hashCode(), locks.length);
        return locks[index];
    }
}
