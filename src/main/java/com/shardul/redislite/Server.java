package com.shardul.redislite;

import com.shardul.redislite.persistence.AofLog;
import com.shardul.redislite.store.CacheStore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The TCP server, built on a single-threaded NIO Selector event loop —
 * deliberately mirroring how real Redis works: one thread handles ALL
 * client I/O and command execution, so command execution itself never
 * needs locking (there's only ever one thread running command logic
 * at any instant). Only background threads (the TTL sweeper here)
 * introduce real concurrency, which is why CacheStore still needs
 * proper locks.
 *
 * Why single-threaded instead of "thread per connection"?
 *   - Thread-per-connection doesn't scale past a few thousand
 *     connections (thread stacks, context-switch overhead).
 *   - A single event loop with non-blocking sockets can handle tens of
 *     thousands of idle-ish connections cheaply — this is the same
 *     model behind Node.js, nginx, and Redis itself.
 */
public class Server {

    private static final int PORT = 6380;
    private static final int MAX_ENTRIES = 10_000;
    private static final String AOF_FILE = "redislite.aof";
    private static final int BUFFER_SIZE = 4096;

    public static void main(String[] args) throws IOException {
        new Server().run();
    }

    public void run() throws IOException {
        CacheStore store = new CacheStore(MAX_ENTRIES);
        AofLog aof = new AofLog(AOF_FILE);
        CommandProcessor processor = new CommandProcessor(store, aof);

        replayAofOnStartup(aof, processor);
        startTtlSweeper(store);

        Selector selector = Selector.open();
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("redis-lite listening on port " + PORT);

        while (true) {
            selector.select(); // blocks until at least one channel is ready
            Set<SelectionKey> readyKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = readyKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                iterator.remove(); // must remove manually — selector won't do it for you

                try {
                    if (key.isAcceptable()) {
                        acceptConnection(key, selector);
                    } else if (key.isReadable()) {
                        handleRead(key, processor);
                    }
                } catch (IOException e) {
                    closeQuietly(key);
                }
            }
        }
    }

    private void acceptConnection(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
        clientKey.attach(new ConnectionState());
    }

    private void handleRead(SelectionKey key, CommandProcessor processor) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ConnectionState state = (ConnectionState) key.attachment();

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead;
        try {
            bytesRead = channel.read(buffer);
        } catch (IOException e) {
            closeQuietly(key);
            return;
        }

        if (bytesRead == -1) { // client closed the connection
            closeQuietly(key);
            return;
        }

        buffer.flip();
        String chunk = StandardCharsets.UTF_8.decode(buffer).toString();
        state.append(chunk);

        String line;
        StringBuilder responses = new StringBuilder();
        while ((line = state.pollLine()) != null) {
            if (line.isBlank()) continue;
            String response = processor.process(line);
            responses.append(response).append("\n");
        }

        if (responses.length() > 0) {
            channel.write(ByteBuffer.wrap(responses.toString().getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void replayAofOnStartup(AofLog aof, CommandProcessor processor) throws IOException {
        int[] count = {0};
        aof.replay(line -> {
            processor.process(line, true); // true = "replaying", don't re-log
            count[0]++;
        });
        if (count[0] > 0) {
            System.out.println("Replayed " + count[0] + " commands from AOF");
        }
    }

    /** Runs on its own daemon thread — concurrently with the I/O loop, hence the need for locks in CacheStore. */
    private void startTtlSweeper(CacheStore store) {
        ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ttl-sweeper");
            t.setDaemon(true);
            return t;
        });
        sweeper.scheduleAtFixedRate(store::sweepExpired, 1, 1, TimeUnit.SECONDS);
    }

    private void closeQuietly(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException ignored) {
        }
        key.cancel();
    }
}
