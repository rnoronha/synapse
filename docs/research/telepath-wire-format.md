# Telepath Wire Format for Router Query Extraction

**Bead:** synapse-w9b
**Date:** 2026-05-04
**Status:** Complete

## Executive Summary

The telepath wire format uses raw msgpack objects with no length-prefix framing — msgpack's self-delimiting encoding *is* the framing. A synchronous router can identify message types and extract Storm query text by reading a small, fixed prefix of bytes. For **plain TCP**, `MSG_PEEK` works: the router peeks at the first ~128 bytes to classify and extract the query, then passes the fd via `sendmsg`/`SCM_RIGHTS` — the worker re-reads the same bytes. For **SSL/TLS**, this approach **does not work** because `MSG_PEEK` sees encrypted bytes and SSL session state cannot be transferred with the fd. SSL connections require either upstream termination or a proxy/relay model.

## Findings

### 1. Msgpack Framing Format

**No explicit framing.** Telepath writes raw msgpack-serialized objects directly to the socket with no length prefix, delimiter, or envelope. Message boundaries are determined entirely by msgpack's self-delimiting binary format.

Evidence from `synapse/lib/link.py`:
- `tx()` calls `pack(mesg)` → `s_msgpack.en(mesg)` → `msgpack.packb()`, then writes raw bytes
- `rx()` reads up to 16 MiB chunks and feeds them to `msgpack.Unpacker` (streaming decoder) which yields complete objects as they become parseable

Evidence from `synapse/lib/msgpack.py`:
- The `Unpk` class wraps `msgpack.Unpacker` — a streaming decoder that buffers partial input and emits objects when complete
- No framing metadata is added or expected

**Implication for router:** The router cannot know the total message size without parsing the msgpack structure. However, it can parse incrementally — feed bytes to an `Unpacker` until a complete object is emitted.

### 2. t2:init Wire Format and Storm Query Location

Every telepath message is a 2-element tuple: `(type_string, info_dict)`.

**tele:syn** (first message on a new connection — handshake):
```
('tele:syn', {'auth': ..., 'vers': (3, 0), 'name': '<share_name>'})
```

**t2:init** (first message on pool connections — carries the RPC call):
```
('t2:init', {
    'todo': (method_name, args_tuple, kwargs_dict),
    'name': '<share_name>',
    'sess': '<session_id>'
})
```

For Storm queries, `todo` is one of:
- `('storm', (query_text,), {})` or `('storm', (query_text,), {'opts': {...}})`
- `('callStorm', (query_text,), {})` or `('callStorm', (query_text,), {'opts': {...}})`

The **Storm query text** is at `mesg[1]['todo'][1][0]`.

**Byte-level layout of a t2:init message** (verified with `msgpack.packb`):

| Offset | Hex | Meaning |
|--------|-----|---------|
| 0 | `92` | fixarray(2) — top-level tuple |
| 1 | `a7` | fixstr(7) — type string length |
| 2–8 | `74 32 3a 69 6e 69 74` | `"t2:init"` |
| 9 | `83` | fixmap(3) — info dict with 3 keys |
| 10 | `a4` | fixstr(4) — first key length |
| 11–14 | `74 6f 64 6f` | `"todo"` |
| 15 | `93` | fixarray(3) — todo tuple |
| 16 | `a5` or `a9` | fixstr(5) `"storm"` or fixstr(9) `"callStorm"` |
| 17–21 or 17–25 | | method name bytes |
| next | `91` | fixarray(1) — args tuple |
| next | varies | query string (fixstr/str8/str16 header + UTF-8 bytes) |

For `method="storm"`: query string header is at byte **23**, query text at byte **24** (fixstr) or **25** (str8) or **26** (str16).

For `method="callStorm"`: query string header is at byte **27**, query text at byte **28/29/30**.

**Validation required:** The router must check that the args fixarray length byte is `0x91` (exactly 1 element) before extracting the query. If `0x90` (empty args), there is no query to extract. If `0x92` or higher, the first element is still the query text.

**Dict key ordering:** The `'todo'` key is always first because Python 3.7+ preserves dict insertion order and the telepath client code constructs the dict with `'todo'` first. This is a code convention, not a protocol guarantee.

**tele:syn vs t2:init discrimination:** Byte `[1]` differs — `0xa8` (fixstr 8) for `"tele:syn"` vs `0xa7` (fixstr 7) for `"t2:init"`. However, other message types share the same byte[1] value (e.g., `t2:fini` and `t2:genr` are also 7 chars). This discrimination is valid **only for the first inbound message on a client→server connection**, where the only possible types are `tele:syn` (handshake link) and `t2:init` (pool link). The router should compare the full type string (bytes[2:9]) rather than relying on byte[1] alone.

### 3. MSG_PEEK Feasibility

**Yes, for plain TCP.** `recv(N, MSG_PEEK)` reads up to N bytes from the kernel receive buffer without consuming them. Subsequent `recv()` calls (by the same or a different process holding the fd) will return the same data.

Verified with a TCP socket pair test: `recv(5, MSG_PEEK)` returned `b"hello"`, then `recv(11)` returned `b"hello world"` — the peeked bytes were not consumed.

**No, for SSL/TLS.** `MSG_PEEK` does not work with SSL sockets because:
1. `SSL_read()` in OpenSSL does not support the `MSG_PEEK` flag
2. Python's `ssl.SSLSocket.recv()` raises `ValueError` for non-zero flags (including `MSG_PEEK`)
3. On the raw socket, `MSG_PEEK` would see encrypted TLS record bytes, not msgpack

### 4. Minimum Bytes to Extract Storm Query Text

The router needs to read in **two phases**:

**Phase 1 — Classification (9 bytes):** Read bytes `[0:9]` to determine if the message is `tele:syn` or `t2:init`.

**Phase 2 — Query extraction (variable):** For `t2:init`, continue reading to extract the query. The minimum depends on method name and query length:

| Method | Query length | Min bytes to read query |
|--------|-------------|------------------------|
| `storm` | < 32 chars (fixstr) | 24 + query_len |
| `storm` | < 256 chars (str8) | 25 + query_len |
| `storm` | < 65536 chars (str16) | 26 + query_len |
| `callStorm` | < 32 chars (fixstr) | 28 + query_len |
| `callStorm` | < 256 chars (str8) | 29 + query_len |
| `callStorm` | < 65536 chars (str16) | 30 + query_len |

**Practical recommendation:** Use a streaming msgpack `Unpacker` as the primary parsing strategy. Feed peeked bytes to the unpacker incrementally — this is resilient to dict key reordering, variable-length fields, and partial reads. The loop is: peek available bytes → feed to unpacker → if a complete object is emitted, extract the query by key name → if not, wait (via `select`/`poll` with timeout) and peek again.

**Important:** `recv(N, MSG_PEEK)` returns whatever is currently in the kernel receive buffer, which may be fewer than N bytes (TCP segmentation, slow sender). The router **must** loop on partial reads rather than assuming a single peek returns enough data. Set a maximum peek timeout (e.g., 5 seconds) after which the connection is routed to a default worker.

As an optimization, fixed byte offsets can be used if structural invariants are validated first: check that bytes[2:9] spell the expected type, bytes[10:15] spell `"todo"`, and byte 22/26 is `0x91` (fixarray of 1). If any check fails, fall back to the streaming parser.

### 5. MSG_PEEK + sendmsg fd Passing with SSL/TLS

**Does not work.** Three independent problems:

1. **Encrypted bytes:** `MSG_PEEK` on the raw TCP socket sees TLS ciphertext, not msgpack. The router cannot extract the Storm query without decrypting.

2. **SSL state is per-process:** The SSL session (symmetric keys, sequence numbers, buffered plaintext) lives in userspace memory. Passing the fd via `sendmsg`/`SCM_RIGHTS` transfers only the kernel-level TCP socket. The receiving process has no SSL context for this connection.

3. **No re-read after SSL_read:** Even if the router performs SSL termination and reads the first message, that data is consumed from the SSL buffer. The worker cannot re-read it from the fd.

**Alternatives for SSL/TLS connections:**

| Approach | Complexity | Latency | Compatibility |
|----------|-----------|---------|---------------|
| **A. Upstream SSL termination** (load balancer or stunnel) | Low (router unchanged) | +1 hop | Router sees plain TCP, MSG_PEEK works |
| **B. Router terminates SSL, reads first msg, relays via UDS** | Medium (prior art exists in `Link.getSpawnInfo()`) | +1 copy | Router must proxy entire connection |
| **C. Router terminates SSL, buffers first msg, passes decrypted bytes + fd** | High | Minimal | Requires custom protocol between router and worker |

**Recommendation:** Option A (upstream SSL termination) is simplest and keeps the router synchronous and stateless. If the router must handle SSL directly, Option B (full proxy) is the most reliable.

## Open Questions

1. **Dict key ordering guarantee:** The `'todo'` key being first in the msgpack dict relies on Python dict insertion order. The msgpack spec does NOT guarantee map key ordering. If a non-Python client or a future refactor changes the key order, fixed-offset byte parsing breaks silently. **Mitigation:** Use streaming `Unpacker` as primary approach; fixed offsets only as a validated optimization.

2. **Non-Storm t2:init messages:** The router will see t2:init messages for non-Storm methods (e.g., `getModelDict`, `getCellInfo`, `addNode`, `addNodes`). The classification logic needs a routing policy: `storm`/`callStorm` → query-based routing; `addNode`/`addNodes` → writer pool; everything else → default pool.

3. **Pipeline t2:init messages:** The `Pipeline` class in telepath.py sends **multiple t2:init messages on the same link**. The router's "peek once, then pass fd" model assumes one message per connection. Pipeline connections may need special handling.

4. **Slow client DoS:** A synchronous router blocks on `recv(MSG_PEEK)` waiting for bytes. A slowloris-style client that connects but sends data very slowly blocks all routing. The router needs non-blocking I/O with `select`/`epoll` and a per-connection timeout.

5. **Error handling for non-telepath connections:** Port scanners, health checks, or misconfigured clients may connect and send non-msgpack data. The router must handle parse failures gracefully (close connection or route to default worker).

## Sources

- `synapse/lib/msgpack.py` — msgpack encoding primitives, uses v5/2.0 spec with `use_bin_type=True` (this repo)
- `synapse/lib/link.py` — link-level framing and I/O, `getSpawnInfo()` TLS relay pattern (this repo)
- `synapse/telepath.py` — telepath protocol, handshake, Proxy class (this repo)
- `synapse/daemon.py` — server-side message dispatch, t2:init handler (this repo)
- Python `socket` module — MSG_PEEK behavior (stdlib)
- Python `ssl` module — SSLSocket.recv raises ValueError for non-zero flags (stdlib)
