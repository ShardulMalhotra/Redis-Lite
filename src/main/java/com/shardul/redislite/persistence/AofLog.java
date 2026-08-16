package com.shardul.redislite.persistence;

import java.io.*;
import java.nio.file.*;
import java.util.function.Consumer;

/**
 * Append-Only File persistence, the same idea Redis's AOF mode and every
 * write-ahead log (databases, Kafka, etc.) use:
 *
 *   1. Every mutating command is appended to a file as plain text,
 *      one command per line, BEFORE we consider the write "done".
 *   2. On startup, we replay the entire file from the top, re-running
 *      every command against an empty store, which reconstructs the
 *      exact state the store was in before it went down.
 *
 * DURABILITY TRADE-OFF (worth knowing cold for interviews):
 * We flush() after every single write, which is durable but slow
 * (a syscall per command). Redis actually lets you choose:
 *   - always: fsync every write (safest, slowest)
 *   - everysec: fsync once per second (Redis's default — good balance)
 *   - no: let the OS decide when to flush (fastest, least safe)
 * We implement "always" here for simplicity and correctness; the
 * README explains how you'd extend this to "everysec".
 */
public class AofLog implements Closeable {

    private final Path path;
    private BufferedWriter writer;

    public AofLog(String filePath) throws IOException {
        this.path = Paths.get(filePath);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        // APPEND mode: we never overwrite history, only add to it.
        this.writer = new BufferedWriter(new FileWriter(path.toFile(), true));
    }

    /** Appends one command line to the log and forces it to disk. */
    public synchronized void append(String commandLine) {
        try {
            writer.write(commandLine);
            writer.newLine();
            writer.flush(); // "always" fsync policy — see class javadoc
        } catch (IOException e) {
            throw new RuntimeException("AOF write failed — treating as fatal, " +
                    "since a lost write here breaks durability guarantees", e);
        }
    }

    /** Replays every previously logged command through the given handler. */
    public void replay(Consumer<String> commandHandler) throws IOException {
        if (!Files.exists(path)) return;
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    commandHandler.accept(line);
                }
            }
        }
    }

    /** Wipes the log — used by FLUSHALL so we don't replay stale deletes forever. */
    public synchronized void truncate() {
        try {
            writer.close();
            writer = new BufferedWriter(new FileWriter(path.toFile(), false));
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate AOF", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}
