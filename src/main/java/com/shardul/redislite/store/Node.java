package com.shardul.redislite.store;

/**
 * A node in the intrusive doubly-linked list used for LRU ordering.
 *
 * "Intrusive" means the linked-list pointers (prev/next) live directly on
 * the object we store in the hash map, instead of wrapping values in a
 * separate list-node type. This is exactly how java.util.LinkedHashMap
 * implements access-order internally, and it's what lets us do O(1)
 * "move to front" and O(1) eviction from the tail.
 */
public class Node {
    final String key;
    volatile String value;
    volatile long expireAtMillis; // -1 means "no expiry"

    Node prev;
    Node next;

    Node(String key, String value, long expireAtMillis) {
        this.key = key;
        this.value = value;
        this.expireAtMillis = expireAtMillis;
    }

    boolean isExpired(long nowMillis) {
        return expireAtMillis != -1 && expireAtMillis <= nowMillis;
    }
}
