package com.shardul.redislite;

/**
 * TCP is a byte STREAM, not a message protocol — a single client write
 * of "GET foo\n" might arrive across two separate read() calls (e.g.
 * "GET fo" then "o\n"), or two commands might arrive in one read() call
 * ("GET foo\nGET bar\n"). This class accumulates raw bytes per-connection
 * until we can pull complete newline-terminated lines out of it.
 *
 * This buffering problem is the single most common bug source in raw
 * socket programming, and handling it correctly is a big part of why
 * this project is worth more than "call a library."
 */
public class ConnectionState {
    private final StringBuilder buffer = new StringBuilder();

    public void append(String chunk) {
        buffer.append(chunk);
    }

    /** Extracts and removes the first complete line, or null if none is buffered yet. */
    public String pollLine() {
        int newlineIndex = buffer.indexOf("\n");
        if (newlineIndex == -1) return null;

        String line = buffer.substring(0, newlineIndex).replace("\r", "");
        buffer.delete(0, newlineIndex + 1);
        return line;
    }
}
