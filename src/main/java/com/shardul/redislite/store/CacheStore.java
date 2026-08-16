package com.shardul.redislite.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The heart of the project: an in-memory key-value store with
 *   - O(1) get/set backed by ConcurrentHashMap
 *   - Hand-rolled LRU eviction using an intrusive doubly-linked list
 *   - Lazy + active TTL expiry
 *
 * DESIGN NOTE (read this — it's the most interview-relevant part):
 * ConcurrentHashMap already gives us thread-safe, lock-free-ish get/put
 * for the map itself. But LRU *ordering* is extra state that lives
 * outside the map (the linked list), and that state has to stay
 * consistent even when multiple threads touch it at once. We use a
 * single ReentrantLock ("lruLock") to guard only the linked-list
 * pointer surgery — not the whole store, and not value reads.
 *
 * That means: GET on a key that already exists never blocks on the map
 * lookup itself, but the "move this node to the front of the LRU list"
 * step is serialized. This is a known bottleneck under very high
 * concurrency — production caches (e.g. Caffeine) solve it with
 * sharded/striped LRU structures or approximate algorithms like CLOCK
 * or W-TinyLFU instead of one exact global list. Naming this trade-off
 * explicitly is a great thing to bring up in an interview.
 */
public class CacheStore {

    private final ConcurrentHashMap<String, Node> map = new ConcurrentHashMap<>();
    private final ReentrantLock lruLock = new ReentrantLock();

    // Sentinel head/tail nodes simplify list surgery (no null-checking edge cases).
    private final Node head = new Node(null, null, -1);
    private final Node tail = new Node(null, null, -1);

    private final int maxEntries;

    public CacheStore(int maxEntries) {
        this.maxEntries = maxEntries;
        head.next = tail;
        tail.prev = head;
    }

    // ---------- Public API used by the command processor ----------

    public String get(String key) {
        Node node = map.get(key);
        if (node == null) return null;

        if (node.isExpired(now())) {
            removeNode(key, node);
            return null;
        }

        touch(node); // move to front: it was just accessed
        return node.value;
    }

    public void set(String key, String value, long ttlSeconds) {
        long expireAt = ttlSeconds > 0 ? now() + ttlSeconds * 1000 : -1;

        Node existing = map.get(key);
        if (existing != null) {
            existing.value = value;
            existing.expireAtMillis = expireAt;
            touch(existing);
            return;
        }

        Node node = new Node(key, value, expireAt);
        map.put(key, node);
        lruLock.lock();
        try {
            addToFront(node);
            evictIfOverCapacityLocked();
        } finally {
            lruLock.unlock();
        }
    }

    public boolean del(String key) {
        Node node = map.get(key);
        if (node == null) return false;
        removeNode(key, node);
        return true;
    }

    public boolean expire(String key, long ttlSeconds) {
        Node node = map.get(key);
        if (node == null || node.isExpired(now())) return false;
        node.expireAtMillis = ttlSeconds > 0 ? now() + ttlSeconds * 1000 : -1;
        return true;
    }

    /** Returns -2 if key doesn't exist, -1 if no TTL set, else seconds remaining. */
    public long ttl(String key) {
        Node node = map.get(key);
        if (node == null || node.isExpired(now())) return -2;
        if (node.expireAtMillis == -1) return -1;
        return Math.max(0, (node.expireAtMillis - now()) / 1000);
    }

    public int size() {
        return map.size();
    }

    public void flushAll() {
        lruLock.lock();
        try {
            map.clear();
            head.next = tail;
            tail.prev = head;
        } finally {
            lruLock.unlock();
        }
    }

    /**
     * Called by the TTL sweeper thread AND is the reason we need real
     * thread-safety here: this runs concurrently with client requests
     * being handled on the I/O thread.
     */
    public int sweepExpired() {
        long now = now();
        int removed = 0;
        for (Map.Entry<String, Node> entry : map.entrySet()) {
            if (entry.getValue().isExpired(now)) {
                removeNode(entry.getKey(), entry.getValue());
                removed++;
            }
        }
        return removed;
    }

    // ---------- Internal LRU list mechanics (all guarded by lruLock) ----------

    private void touch(Node node) {
        lruLock.lock();
        try {
            unlink(node);
            addToFront(node);
        } finally {
            lruLock.unlock();
        }
    }

    private void addToFront(Node node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void unlink(Node node) {
        if (node.prev != null) node.prev.next = node.next;
        if (node.next != null) node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    /** Must be called while holding lruLock. Evicts from the tail (least recently used). */
    private void evictIfOverCapacityLocked() {
        while (map.size() > maxEntries) {
            Node lru = tail.prev;
            if (lru == head) break; // list is empty, nothing to evict
            unlink(lru);
            map.remove(lru.key);
        }
    }

    private void removeNode(String key, Node node) {
        map.remove(key);
        lruLock.lock();
        try {
            unlink(node);
        } finally {
            lruLock.unlock();
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
