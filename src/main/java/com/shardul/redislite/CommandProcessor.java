package com.shardul.redislite;

import com.shardul.redislite.concurrency.StripedLock;
import com.shardul.redislite.persistence.AofLog;
import com.shardul.redislite.protocol.CommandParser;
import com.shardul.redislite.store.CacheStore;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The dispatcher: takes one raw protocol line, figures out which command
 * it is, executes it against the CacheStore, and — for anything that
 * MUTATES state — logs it to the AOF before returning success.
 *
 * WHY LOG BEFORE RETURNING SUCCESS: if we returned "+OK" to the client
 * and THEN crashed before writing to the AOF, the client would believe
 * a write succeeded that isn't actually durable. This is the same
 * "write-ahead" ordering guarantee every real WAL-based system relies on.
 */
public class CommandProcessor {

    private final CacheStore store;
    private final AofLog aof;
    private final StripedLock stripedLock = new StripedLock(64);

    /** replaying=true means "don't re-log to AOF" — used during startup replay. */
    public CommandProcessor(CacheStore store, AofLog aof) {
        this.store = store;
        this.aof = aof;
    }

    public String process(String rawLine) {
        return process(rawLine, false);
    }

    public String process(String rawLine, boolean replaying) {
        List<String> tokens = CommandParser.tokenize(rawLine);
        if (tokens.isEmpty()) return "-ERR empty command";

        String cmd = tokens.get(0).toUpperCase();
        try {
            switch (cmd) {
                case "PING":
                    return "+PONG";

                case "SET": {
                    requireArgs(tokens, 3);
                    String key = tokens.get(1);
                    String value = tokens.get(2);
                    long ttl = 0;
                    if (tokens.size() >= 5 && tokens.get(3).equalsIgnoreCase("EX")) {
                        ttl = Long.parseLong(tokens.get(4));
                    }
                    store.set(key, value, ttl);
                    if (!replaying) aof.append(rawLine);
                    return "+OK";
                }

                case "GET": {
                    requireArgs(tokens, 2);
                    String value = store.get(tokens.get(1));
                    return value == null ? "$-1" : "$" + value;
                }

                case "DEL": {
                    requireArgs(tokens, 2);
                    boolean removed = store.del(tokens.get(1));
                    if (!replaying && removed) aof.append(rawLine);
                    return ":" + (removed ? 1 : 0);
                }

                case "EXPIRE": {
                    requireArgs(tokens, 3);
                    boolean ok = store.expire(tokens.get(1), Long.parseLong(tokens.get(2)));
                    if (!replaying && ok) aof.append(rawLine);
                    return ":" + (ok ? 1 : 0);
                }

                case "TTL": {
                    requireArgs(tokens, 2);
                    return ":" + store.ttl(tokens.get(1));
                }

                case "INCR":
                case "DECR": {
                    requireArgs(tokens, 2);
                    String key = tokens.get(1);
                    long delta = cmd.equals("INCR") ? 1 : -1;
                    long result = atomicIncrement(key, delta);
                    if (!replaying) aof.append(rawLine);
                    return ":" + result;
                }

                case "FLUSHALL":
                    store.flushAll();
                    if (!replaying) aof.truncate();
                    return "+OK";

                case "DBSIZE":
                    return ":" + store.size();

                default:
                    return "-ERR unknown command '" + cmd + "'";
            }
        } catch (NumberFormatException e) {
            return "-ERR value is not an integer";
        } catch (IllegalArgumentException e) {
            return "-ERR " + e.getMessage();
        }
    }

    /**
     * Read-modify-write under a striped lock so concurrent INCR calls on
     * the SAME key can never lose an update. Different keys usually hit
     * different stripes and proceed fully in parallel.
     */
    private long atomicIncrement(String key, long delta) {
        ReentrantLock lock = stripedLock.forKey(key);
        lock.lock();
        try {
            String current = store.get(key);
            long value = (current == null) ? 0 : Long.parseLong(current);
            value += delta;
            store.set(key, Long.toString(value), 0);
            return value;
        } finally {
            lock.unlock();
        }
    }

    private void requireArgs(List<String> tokens, int min) {
        if (tokens.size() < min) {
            throw new IllegalArgumentException("wrong number of arguments for '" + tokens.get(0) + "'");
        }
    }
}
