# redis-lite

A distributed in-memory cache engine built from scratch in raw Java — no Spring, no caching library, no external framework.

Implements the core mechanics Redis uses under the hood: a single-threaded NIO event loop, a hand-rolled LRU eviction structure, TTL expiry, atomic counters under lock striping, and crash-recoverable append-only-file (AOF) persistence.

---

## Demo

**Server startup + live commands over TCP:**

```
$ java -jar target/redis-lite.jar
redis-lite listening on port 6380
```

```
$ telnet localhost 6380

> PING                     +PONG
> SET user:1 shardul       +OK
> GET user:1               $shardul
> SET counter 10           +OK
> INCR counter             :11
> INCR counter             :12
> DECR counter             :11
> SET temp hello EX 2      +OK
> TTL temp                 :2
> DEL user:1               :1
> GET user:1               $-1
> DBSIZE                   :1
> FLUSHALL                 +OK
> DBSIZE                   :0
```

**Test suite (4/4 passing):**

```
[INFO] Running com.shardul.redislite.CacheStoreTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.186 s
[INFO] BUILD SUCCESS
```

---

## Why this project exists

Most backend portfolios have a CRUD app sitting on top of a database, or a wrapper around an existing library. This project sits *underneath* that layer — it doesn't call Redis, it re-implements the core mechanics Redis itself uses: non-blocking sockets, an intrusive LRU linked list, write-ahead logging, and lock striping.

---

## Architecture

```
Client (telnet / your app)
        │  raw TCP, custom text protocol
        ▼
┌───────────────────────────────┐
│   Server (NIO Selector loop)  │  ← single thread handles all client I/O
│   ConnectionState per client  │  ← buffers partial TCP reads into lines
└───────────────┬───────────────┘
                │ parsed command line
                ▼
┌───────────────────────────────┐
│      CommandProcessor         │  ← parses, dispatches, logs mutations
└───────┬───────────────┬───────┘
        ▼               ▼
┌───────────────┐  ┌────────────────┐
│  CacheStore   │  │    AofLog      │
│  CHM + LRU   │  │  append-only   │
│  linked list  │  │  file, replay  │
│  + TTL        │  │  on startup    │
└───────┬───────┘  └────────────────┘
        ▲
┌───────┴───────┐
│  TTL Sweeper  │  ← daemon thread, sweeps expired keys every 1s
└───────────────┘
```

### Key design decisions

| Concern | Decision | Why |
|---|---|---|
| I/O model | Single-threaded NIO Selector | Same model as Redis/nginx — scales to thousands of idle connections without thread-per-connection overhead |
| Map | `ConcurrentHashMap` | Lock-free reads, bucket-level writes |
| LRU ordering | Intrusive doubly-linked list | O(1) move-to-front and tail eviction, same as `LinkedHashMap` access-order internals |
| LRU lock | Single `ReentrantLock` | Guards only pointer surgery, not value reads |
| INCR/DECR atomicity | 64-stripe `StripedLock` | Different keys hit different stripes and run fully in parallel; same key serializes |
| Persistence | Append-only file (AOF) | Every mutation logged before returning `+OK` — write-ahead ordering guarantee |

---

## Protocol

Plain-text, newline-delimited — usable directly with `telnet` or `nc`:

| Command | Response | Notes |
|---|---|---|
| `PING` | `+PONG` | |
| `SET key value` | `+OK` | |
| `SET key value EX 60` | `+OK` | expires in 60 seconds |
| `GET key` | `$value` or `$-1` | `$-1` = missing or expired |
| `DEL key` | `:1` or `:0` | |
| `EXPIRE key 30` | `:1` or `:0` | |
| `TTL key` | `:seconds` | `-1` = no TTL, `-2` = doesn't exist |
| `INCR key` | `:newValue` | atomic |
| `DECR key` | `:newValue` | atomic |
| `DBSIZE` | `:count` | |
| `FLUSHALL` | `+OK` | wipes store + AOF |

---

## Verified Behavior

### Persistence (crash recovery)

Server was killed with `kill -9` and restarted. Startup log showed:

```
Replayed 7 commands from AOF
```

After restart, `GET counter` correctly returned `11` — fully rebuilt from the write log. The deleted key stayed deleted.

### Concurrency (`concurrentIncrementsProduceCorrectFinalCount`)

20 threads × 200 concurrent `INCR` calls on the same key. Final count is exactly **4000 every run** — the striped-lock design prevents lost updates.

```java
// 20 threads, 200 increments each = 4000 expected
assertEquals("4000", store.get("counter"));
```

---

## How to run

**Locally (JDK 17+):**
```bash
mvn package
java -jar target/redis-lite.jar
# listens on port 6380
```

**Docker (recommended):**
```bash
docker compose up --build
```

**Talk to it:**
```bash
telnet localhost 6380
SET hello world
GET hello
```

---

## Project structure

```
src/main/java/com/shardul/redislite/
├── Server.java               # NIO Selector event loop
├── ConnectionState.java      # per-client TCP read buffer
├── CommandProcessor.java     # command dispatch + AOF logging
├── protocol/
│   └── CommandParser.java    # space/quote-aware tokenizer
├── store/
│   ├── CacheStore.java       # CHM + LRU list + TTL
│   └── Node.java             # intrusive linked-list node
├── concurrency/
│   └── StripedLock.java      # 64-stripe lock array
└── persistence/
    └── AofLog.java           # append-only file + replay
```

---

## Known limitations

1. **AOF TTL replay uses relative time** — `SET key val EX 2` replayed 10 seconds later gives a fresh 2-second window instead of treating it as already-expired. Fix: store `EXPIREAT <absolute-ms>` in the log instead of `EX <seconds>`.

2. **LRU uses one global lock** — reads/writes to the map are fully concurrent, but "move to front" operations serialize. Production caches (Caffeine) shard this with approximate algorithms like W-TinyLFU.

3. **Single-node only** — no replication or sharding. Natural next steps: leader-follower log shipping, consistent-hash-based sharding.

4. **Not binary-safe** — space/newline-delimited text protocol. Real Redis uses length-prefixed binary framing (RESP) to handle arbitrary byte values.

---
