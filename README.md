# streamable-formats-research

Research into archive/container formats that support streaming multiple interleaved data streams through a single UNIX pipe.

## Main Task

### Problem Statement

Traditional UNIX pipes carry exactly one byte stream from one process to another. This is perfectly adequate when a tool produces a single output. However, many real-world processing tasks naturally produce **more than one output simultaneously** — for example:

- An archiver that writes a data stream and a checksum/manifest stream in parallel.
- A transcoder that separates audio and video tracks.
- A classifier that routes records into multiple categories as they are processed.
- A build system that multiplexes log and artifact streams.

The standard UNIX file descriptor table offers workarounds:

| File Descriptor | Conventional Use |
|---|---|
| 0 (stdin) | Input |
| 1 (stdout) | Primary output |
| 2 (stderr) | Error messages |
| 3, 4, 5 … | Additional I/O (rarely used by convention) |

Using `stderr` as a second output channel is possible but semantically wrong and fragile. Using higher-numbered file descriptors (3, 4, 5, …) is technically possible at the OS level and **works well for simple cases** — tools like GPG (`--status-fd 3`) and bubblewrap (`--json-status-fd N`) use this pattern successfully. However, higher FDs don't compose across pipeline stages (`|` only connects FD 1→0), require per-tool flag conventions, and aren't portable to all shells or Windows (see [Higher Pipes — Can They Actually Work?](#higher-pipes-fd-3-4-5---can-they-actually-work)).

For general-purpose multi-stream pipelines (arbitrary named streams, composable across stages, portable across shells and platforms), the most practical alternative is to **multiplex several logical streams into a single byte stream** — i.e., to send a container or archive format through stdout, where the container supports genuine chunk-level interleaving of multiple member files/streams.

### Research Goals

Evaluate existing archive and container formats against the following criteria:

1. **Popularity** — how widely deployed the format is; availability of tooling.
2. **Streaming compatibility** — can the format be **written in a single forward pass** without seeking back to patch previously-written bytes? A format "supports streaming" if a writer can produce a complete, valid output by only appending data — never modifying bytes that have already been emitted. (Correspondingly, can a reader consume the format in a single forward pass?)
3. **Existing implementations that support chunk interleaving** — tools or libraries that actively interleave chunks from multiple members rather than writing each member end-to-end before starting the next.
4. **Theoretical support of interleaving on the wire** — does the format specification allow or describe interleaving, even if no common implementation exploits it?

---

## Archive / Container Format Comparison

### Summary Table

| Format | Year | Popularity | Write-streaming (no patching) | Read-streaming (forward-only) | Interleaving: implementations | Interleaving: theoretical |
|---|---|---|---|---|---|---|
| TAR | 1979 | ★★★★★ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| CPIO | 1977 | ★★★☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| ar | 1971 | ★★☆☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| ZIP | 1989 | ★★★★★ | ⚠️ Yes with data descriptors | ❌ Requires seek for central dir | ❌ None known | ❌ Not in spec |
| 7-Zip | 1999 | ★★★★☆ | ❌ No (must patch start header) | ❌ No | ❌ None known | ❌ Not in spec |
| RAR | 1993 | ★★★☆☆ | ⚠️ Partial (may need patching) | ⚠️ Partial | ❌ None known | ❌ Not in spec |
| Ogg | 2003 | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| Matroska / MKV | 2002 | ★★★★☆ | ⚠️ Yes if SeekHead omitted | ✅ Yes (without SeekHead) | ✅ Native interleaving | ✅ Yes — in spec |
| WebM | 2010 | ★★★☆☆ | ⚠️ Same as Matroska | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| MPEG-TS | 1995 | ★★★★☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| MPEG-PS | 1993 | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| ASF / WMV / WMA | 1996 | ★★★☆☆ | ⚠️ Header needs file size | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| CAF | 2005 | ★★☆☆☆ | ✅ Yes (size -1 = unknown) | ✅ Yes | ✅ Audio tracks | ✅ Yes — in spec |
| HTTP/2 framing | 2015 | ★★★★★ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec (RFC 7540) |
| SSH channels | 1995 | ★★★★★ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec (RFC 4254) |
| HTTP/1.1 chunked | 1997 | ★★★★★ | ✅ Yes | ✅ Yes | ❌ None known (but viable for pipes) | ✅ Via chunk extensions (RFC 7230 §4.1.1) — works over pipes, not over HTTP infra |
| MIME multipart | 1996 | ★★★★★ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Parts are sequential, not interleaved |
| ISO 9660 | 1988 | ★★★★☆ | ❌ No (requires pre-computed sector layout) | ❌ No (random-access by design) | ❌ None known | ❌ Not in spec |
| WIM | 2006 | ★★★☆☆ | ❌ No (must patch header with resource table offset) | ❌ No | ❌ None known | ❌ Not in spec |
| CAB | 1997 | ★★★☆☆ | ❌ No (header contains folder/file counts and offsets) | ⚠️ Partial (forward scan possible) | ❌ None known | ❌ Not in spec |
| MP4 / ISOBMFF | 2001 | ★★★★★ | ⚠️ Only fragmented MP4 (fMP4) | ⚠️ Only fragmented MP4 | ✅ Native interleaving | ✅ Yes — in spec (ISO 14496-12) |
| AVI / RIFF | 1992 | ★★★★☆ | ❌ No (RIFF header needs total size) | ⚠️ Partial (index at end) | ✅ Native interleaving | ✅ Yes — in spec |
| IFF | 1985 | ★★☆☆☆ | ⚠️ Chunk sizes needed upfront | ✅ Yes | ❌ None known | ❌ Not in spec |
| FLV | 2002 | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| WARC | 2009 | ★★★☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Sequential records |
| XAR | 2007 | ★★☆☆☆ | ❌ No (XML TOC at beginning references heap) | ❌ No | ❌ None known | ❌ Not in spec |
| NUT | 2003 | ★☆☆☆☆ | ✅ Yes | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| Avro OCF | 2009 | ★★★☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Single schema per file |
| Protobuf (delimited) | 2008 | ★★★★☆ | ✅ Yes | ✅ Yes | ❌ None known | ⚠️ Possible with field tags |
| QUIC | 2021 | ★★★★☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec (RFC 9000) |
| LHA / LZH | 1988 | ★★☆☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| SCTP | 2000 | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec (RFC 4960) |
| WebSocket | 2011 | ★★★★★ | ✅ Yes | ✅ Yes | ❌ None (single channel) | ❌ Not in spec (mux extension expired) |
| D-Bus | 2006 | ★★★★☆ | ✅ Yes | ✅ Yes | ⚠️ Bus-level routing (not wire-level) | ⚠️ Application-level only |
| 9P | 1995 | ★★☆☆☆ | ✅ Yes | ✅ Yes | ✅ Native (tag-based) | ✅ Yes — in spec |
| Cap'n Proto RPC | 2013 | ★★☆☆☆ | ✅ Yes | ✅ Yes | ✅ Native (question IDs) | ✅ Yes — in spec |
| CBOR sequences | 2020 | ★★★☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ⚠️ Possible with convention |
| NDJSON / JSON Lines | 2013 | ★★★★☆ | ✅ Yes | ✅ Yes | ❌ None known | ⚠️ Possible with convention |
| MessagePack | 2008 | ★★★★☆ | ✅ Yes | ✅ Yes | ❌ None known | ⚠️ Possible with envelope |
| Apache Parquet | 2013 | ★★★★★ | ❌ No (footer-based metadata) | ❌ No (random-access by design) | ❌ None known | ❌ Not in spec |
| gRPC | 2015 | ★★★★★ | ✅ Yes (via HTTP/2) | ✅ Yes (via HTTP/2) | ✅ Native (via HTTP/2 streams) | ✅ Yes — inherits HTTP/2 multiplexing |
| AMQP 1.0 | 2012 | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native (session/link multiplexing) | ✅ Yes — in spec (ISO 19464) |
| MQTT 5.0 | 2019 | ★★★★☆ | ✅ Yes | ✅ Yes | ⚠️ Topic-based (not stream-based) | ⚠️ Via topic multiplexing |
| Framing (custom) | — | N/A | ✅ Yes | ✅ Yes | ✅ By design | ✅ By design |

---

### Master Comparison Table

All 45 formats evaluated across every key criterion in a single table. This consolidates the summary table, general-purpose candidates, 7z extractability, stream naming, tombstone markers, and wire format into one reference:

| Format | Year | Popularity | Write-stream (no patch) | Interleaving | 7z extract | File names | Tombstone | Wire format | Category |
|---|---|---|---|---|---|---|---|---|---|
| **TAR** | 1979 | ★★★★★ | ✅ | ❌ Native; ✅ via chunk hack | ✅ | ✅ Full paths | ✅ Two zero blocks | Binary | Archive |
| **CPIO** | 1977 | ★★★☆☆ | ✅ | ❌ | ✅ | ✅ Full paths | ✅ `TRAILER!!!` | Binary | Archive |
| **ar** | 1971 | ★★☆☆☆ | ✅ | ❌ | ✅ | ✅ Short names | ❌ None | Binary | Archive |
| **ZIP** | 1989 | ★★★★★ | ⚠️ Data descriptors | ❌ | ✅ | ✅ Full paths | ✅ EOCD | Binary | Archive |
| **7-Zip** | 1999 | ★★★★☆ | ❌ Patching | ❌ | ✅ | ✅ Full paths | ✅ EndHeader | Binary | Archive |
| **RAR** | 1993 | ★★★☆☆ | ⚠️ Partial | ❌ | ✅ | ✅ Full paths | ✅ HEAD_ENDER | Binary | Archive |
| **ISO 9660** | 1988 | ★★★★☆ | ❌ Pre-computed | ❌ | ✅ | ✅ Full paths | ✅ VD Terminator | Binary | Archive |
| **WIM** | 2006 | ★★★☆☆ | ❌ Patching | ❌ | ✅ | ✅ Full paths | ⚠️ Header checksum | Binary | Archive |
| **CAB** | 1997 | ★★★☆☆ | ❌ Pre-computed | ❌ | ✅ | ✅ Full paths | ✅ Declared size | Binary | Archive |
| **XAR** | 2007 | ★★☆☆☆ | ❌ Pre-computed | ❌ | ✅ | ✅ Full paths | ⚠️ TOC sizes | Binary | Archive |
| **LHA/LZH** | 1988 | ★★☆☆☆ | ✅ | ❌ | ✅ | ✅ Full paths | ✅ Zero sentinel | Binary | Archive |
| **Ogg** | 2003 | ★★★☆☆ | ✅ | ✅ Native | ❌ | ❌ Numeric IDs | ✅ EOS flag/stream | Binary | Container |
| **Matroska/MKV** | 2002 | ★★★★☆ | ⚠️ Conditional | ✅ Native | ❌ | ✅ Track names | ⚠️ None standard | Binary | Multimedia |
| **WebM** | 2010 | ★★★☆☆ | ⚠️ Conditional | ✅ Native | ❌ | ✅ Track names | ⚠️ None standard | Binary | Multimedia |
| **MPEG-TS** | 1995 | ★★★★☆ | ✅ | ✅ Native | ❌ | ❌ PID numbers | ❌ None (broadcast) | Binary | Multimedia |
| **MPEG-PS** | 1993 | ★★★☆☆ | ✅ | ✅ Native | ❌ | ❌ Stream IDs | ✅ End code | Binary | Multimedia |
| **ASF** | 1996 | ★★★☆☆ | ⚠️ Sentinel values | ✅ Native | ❌ | ❌ Stream numbers | ⚠️ Packet count | Binary | Multimedia |
| **CAF** | 2005 | ★★☆☆☆ | ✅ | ✅ Tracks | ❌ | ❌ Track indices | ⚠️ Ambiguous | Binary | Multimedia |
| **MP4/fMP4** | 2001 | ★★★★★ | ⚠️ fMP4 only | ✅ Native | ❌ | ❌ Track IDs | ⚠️ Optional | Binary | Multimedia |
| **AVI/RIFF** | 1992 | ★★★★☆ | ❌ Size in header | ✅ Native | ❌ | ❌ FourCC tags | ⚠️ Size mismatch | Binary | Multimedia |
| **IFF** | 1985 | ★★☆☆☆ | ⚠️ Sizes upfront | ❌ | ❌ | ❌ Type codes | ⚠️ Size mismatch | Binary | Container |
| **FLV** | 2002 | ★★★☆☆ | ✅ | ✅ Native | ❌ | ❌ Tag types | ❌ None (EOF) | Binary | Multimedia |
| **NUT** | 2003 | ★☆☆☆☆ | ✅ | ✅ Native | ❌ | ⚠️ Metadata | ✅ EOR/stream | Binary | Multimedia |
| **WARC** | 2009 | ★★★☆☆ | ✅ | ❌ | ❌ | ✅ WARC-Target-URI | ⚠️ Content-Length | Text+Binary | Data |
| **Avro OCF** | 2009 | ★★★☆☆ | ✅ | ❌ | ❌ | ❌ Schema only | ⚠️ Sync markers | Binary | Data |
| **Protobuf delimited** | 2008 | ★★★★☆ | ✅ | ❌ Standard | ❌ | ❌ Field tags | ❌ None (EOF) | Binary | Data |
| **HTTP/2 framing** | 2015 | ★★★★★ | ✅ | ✅ Native | ❌ | ⚠️ Via headers | ✅ END_STREAM/stream | Binary | Protocol |
| **SSH channels** | 1995 | ★★★★★ | ✅ | ✅ Native | ❌ | ⚠️ Channel type | ✅ CLOSE/channel | Binary | Protocol |
| **HTTP/1.1 chunked** | 1997 | ★★★★★ | ✅ | ⚠️ Via extensions | ❌ | ❌ N/A | ✅ Zero-length chunk | Text | Protocol |
| **MIME multipart** | 1996 | ★★★★★ | ✅ | ❌ | ❌ | ⚠️ Content-Disposition | ✅ Close boundary | Text | Protocol |
| **QUIC** | 2021 | ★★★★☆ | ✅ | ✅ Native | ❌ | ❌ Numeric IDs | ✅ FIN bit/stream | Binary | Protocol |
| **SCTP** | 2000 | ★★★☆☆ | ✅ | ✅ Native | ❌ | ❌ Numeric IDs | ✅ SHUTDOWN | Binary | Protocol |
| **WebSocket** | 2011 | ★★★★★ | ✅ | ❌ | ❌ | ❌ N/A | ✅ Close frame | Binary | Protocol |
| **D-Bus** | 2006 | ★★★★☆ | ✅ | ⚠️ Bus-level | ❌ | ⚠️ Object paths | ❌ None | Binary | IPC |
| **9P** | 1995 | ★★☆☆☆ | ✅ | ✅ Tag-based | ❌ | ✅ File paths | ⚠️ Tclunk per fid | Binary | Protocol |
| **Cap'n Proto RPC** | 2013 | ★★☆☆☆ | ✅ | ✅ Question IDs | ❌ | ❌ Question IDs | ✅ Per-question | Binary | RPC |
| **CBOR sequences** | 2020 | ★★★☆☆ | ✅ | ❌ Standard | ❌ | ❌ N/A | ❌ None (EOF) | Binary | Data |
| **NDJSON** | 2013 | ★★★★☆ | ✅ | ❌ Standard | ❌ | ❌ N/A | ❌ None (EOF) | Text | Data |
| **MessagePack** | 2008 | ★★★★☆ | ✅ | ❌ Standard | ❌ | ❌ N/A | ❌ None (EOF) | Binary | Data |
| **Apache Parquet** | 2013 | ★★★★★ | ❌ Footer-based | ❌ | ❌ | ✅ Column names | ✅ Footer magic | Binary | Data |
| **gRPC** | 2015 | ★★★★★ | ✅ | ✅ Via HTTP/2 | ❌ | ⚠️ Via metadata | ✅ END_STREAM | Binary | RPC |
| **AMQP 1.0** | 2012 | ★★★☆☆ | ✅ | ✅ Native | ❌ | ⚠️ Link names | ✅ Detach/Close | Binary | Protocol |
| **MQTT 5.0** | 2019 | ★★★★☆ | ✅ | ⚠️ Via topics | ❌ | ✅ Topic strings | ✅ DISCONNECT | Binary | Protocol |
| **Custom LTV** | — | N/A | ✅ | ✅ By design | ❌ | ✅ If designed in | ✅ If designed in | Binary | Custom |

**Reading this table**: ✅ = fully supported, ⚠️ = conditional/partial, ❌ = not supported. "Interleaving" means native concurrent multi-stream interleaving. "7z extract" means `7z l file.ext` works. "Tombstone" means clean finalization detection (container-level or per-stream). The "via chunk hack" note on TAR refers to the interleaving workaround described below.

**Key observations from the unified view**:
- **11 formats are 7z-extractable** — all archives, **none** with native interleaving
- **18 formats support native interleaving** — none are 7z-extractable (gRPC and AMQP 1.0 join the multiplexing group)
- **TAR (with chunk hack)** is the **only** entry that spans both columns — 7z-extractable AND interleaving (via naming convention)
- All multiplexing protocols (HTTP/2, QUIC, SCTP, SSH, gRPC, AMQP) use **numeric stream IDs** or protocol-specific addressing — file names require application-level mapping (except MQTT's topic strings)
- **WebSocket** is the highest-popularity format that explicitly lacks multiplexing — confirming that single-stream framing ≠ multiplexing
- **Apache Parquet** is the highest-popularity format that cannot be streamed at all — footer-based metadata requires the entire file before reading
- Every format is **binary** except HTTP/1.1 chunked (text), MIME multipart (text), NDJSON (text), and WARC (hybrid)

---

### Detailed Analysis

#### TAR (Tape Archive)

**Popularity**: ★★★★★ — ubiquitous on all UNIX-like systems; the de-facto standard for UNIX archiving.

**Format overview**: TAR is a purely sequential format. Each member consists of a 512-byte header block immediately followed by the file's data padded to a multiple of 512 bytes. A two-block all-zero trailer marks end-of-archive. Extensions (ustar / POSIX.1-2001 / GNU tar / pax) add long filenames, extended metadata, and sparse file support via additional header types, but the fundamental sequential layout is unchanged.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. A TAR stream can be produced in a single forward pass; each header's size field is filled before its data is written, and no previously-written bytes are ever modified. This is why `tar -c | gzip | ssh host tar -xz` works reliably.

**Sequential-read streaming**: ✅ Fully supported. A TAR reader needs only a forward scan.

**Chunk interleaving (implementations)**: ❌ None known. All standard TAR implementations (`GNU tar`, `bsdtar`, `libarchive`) write each member file completely (header + all data blocks) before starting the next member. There is no mechanism to pause in the middle of a member's data and insert data for another member.

**Chunk interleaving (theoretical)**: ❌ The TAR specification provides no mechanism for interleaving data blocks _within_ a single member — each member's data must be contiguous after its header. However, nothing in the spec prevents writing **many small members** with a naming convention that simulates interleaving (see "TAR-based interleaving workaround" below). Additionally, several TAR mechanisms have been investigated for "real" interleaving:

- **GNU tar sparse files** (`--sparse`): Sparse file support stores a data map (offset + size pairs) allowing non-contiguous data segments within a single member. However, all segments of a sparse member must appear contiguously in the archive — no other member's data can appear between them. Sparse files are for efficiently representing files with holes (zero-filled gaps), not for interleaving multiple files.
- **GNU tar multi-volume** (`--multi-volume`): Multi-volume support splits a large member across multiple physical volumes using continuation headers (`M` type). But continuation records are for spanning tape boundaries, not for interleaving — the continued member's data picks up exactly where the previous volume left off, with no intervening data from other members.
- **PAX extended headers**: The `pax` format allows arbitrary key-value metadata before each member. A convention could attach stream identifiers or chunk sequence numbers, but this doesn't change the fundamental constraint: each member's data blocks are contiguous.
- **Hard/symbolic links**: TAR link entries (`1`/`2` type flags) reference other members by name, but carry no data payload — they're not useful for data interleaving.

**Bottom line on TAR "real" interleaving**: The **only** way to achieve interleaved multi-stream data in a valid TAR file is the **chunk-per-member convention** — writing each data chunk as a separate, small TAR member with a naming scheme that encodes the stream identity and chunk order. This IS a real technique that produces valid, standards-compliant TAR files extractable by any TAR tool. It just has higher overhead (512-byte header per chunk) than purpose-built multiplexing formats.

**Conclusion for multi-stream use**: TAR is excellent for single-pass streaming of a flat sequence of files. It cannot interleave data blocks within a member, but the chunk-per-member convention achieves effective interleaving at the member level — producing valid, 7z-extractable TAR files. This makes TAR the strongest practical candidate when compatibility with existing tools is a hard requirement.

---

#### CPIO (Copy In/Out)

**Popularity**: ★★★☆☆ — used by RPM packages, Linux initramfs images, and some backup tools. Less visible to end users than TAR.

**Format overview**: CPIO has several variants (binary, old ASCII, new ASCII `newc`, CRC `newc`). All variants place a per-file header immediately before each file's data. A special `TRAILER!!!` entry marks end-of-archive. Like TAR, the format is inherently sequential.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each header is written with the file size, followed by the data.

**Sequential-read streaming**: ✅ Fully supported.

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec. The same theoretical extension caveats as TAR apply.

**Conclusion for multi-stream use**: Same as TAR — good for sequential single-pass streaming, unusable for chunk interleaving without a new format layer.

---

#### ar (Unix Archive)

**Popularity**: ★★☆☆☆ — primarily used for static library archives (`.a`) and Debian `.deb` packages. Rarely used directly by end users.

**Format overview**: `ar` is the simplest archive format: a magic header `!<arch>\n` followed by a sequence of member records, each with a fixed-width ASCII header (60 bytes) and the member data. No compression. No seeking required.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each member header contains the size, followed by the data.

**Sequential-read streaming**: ✅ Fully supported.

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec.

**Conclusion for multi-stream use**: Too simple and niche for multi-stream use.

---

#### ZIP

**Popularity**: ★★★★★ — the most widely used archive format on Windows and for cross-platform distribution.

**Format overview**: ZIP stores each member as a Local File Header followed by (optionally compressed) data, followed by an optional Data Descriptor. A **Central Directory** at the very end of the file contains a summary of all members with their offsets. Most ZIP readers navigate via the Central Directory rather than scanning sequentially, making seek access essential for normal use.

**Sequential-write streaming**: ⚠️ Partially possible — no patching needed if using Data Descriptors (general-purpose bit 3 set). With bit 3, the CRC-32 and size fields in the Local File Header are zeroed; the actual values follow the compressed data in a Data Descriptor record. The Central Directory is appended at the very end — this is pure forward-only writing, no bytes are patched. However, not all ZIP readers support Data Descriptors correctly, and without bit 3 the writer must know sizes/CRCs before writing the Local File Header (requiring either buffering or seeking back to patch).

**Sequential-read streaming**: ❌ Technically difficult. `unzip` traditionally seeks to the end to read the Central Directory first. Some implementations (notably `funzip` and streaming-aware extractors) can scan forward through Local File Headers, but this is unreliable because Local File Headers may have zeroed size fields when Data Descriptors are used, requiring the reader to parse compressed data to find the end of each member. This is a known, fundamental limitation.

**Chunk interleaving (implementations)**: ❌ None known. The ZIP format has no mechanism for interleaving.

**Chunk interleaving (theoretical)**: ❌ Not in spec.

**Conclusion for multi-stream use**: The Central Directory requirement makes ZIP a poor streaming format and completely unsuitable for multi-stream interleaving.

---

#### 7-Zip (7z)

**Popularity**: ★★★★☆ — very popular for high-compression archives, especially on Windows. Growing cross-platform adoption.

**Format overview**: 7-Zip stores all metadata (headers, filenames, sizes, checksums, compression parameters) in a header block that is typically located **at the end** of the archive. The compressed payload(s) are stored in the body. Solid compression mode groups multiple files into a single large compressed stream.

**Sequential-write streaming**: ❌ Not supported — **requires patching**. The 7z format begins with a 32-byte SignatureHeader at offset 0 that contains the offset and size of the EndHeader (metadata block at the end of the archive). Since this offset is not known until all compressed data has been written, the writer must seek back to byte 12 and patch the SignatureHeader after writing is complete. Standard `7z` therefore requires a seekable output and cannot write to a pipe.

**Sequential-read streaming**: ❌ Not supported without special handling. Some tools write the header at the start, but this is non-standard.

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec. Solid archives actually reduce granularity by merging everything into one stream.

**Conclusion for multi-stream use**: 7-Zip's design is fundamentally incompatible with streaming and interleaving. Not suitable.

---

#### RAR

**Popularity**: ★★★☆☆ — popular for file sharing and compression, especially on Windows. Proprietary format.

**Format overview**: RAR uses a block-based format with typed blocks for file headers, file data, end-of-archive, comments, etc. RAR4 and RAR5 have different block layouts. Recovery records are optionally appended.

**Sequential-write streaming**: ⚠️ Partial — **may require patching**. RAR file headers include CRC and size fields. In some modes, WinRAR can write to a pipe, but the archive end marker and recovery records may require seek-back to finalize. Whether patching is needed depends on the specific RAR version and options used.

**Sequential-read streaming**: ⚠️ Partial. The RAR specification and implementations can support forward scanning in some cases, but seek-based access is preferred.

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec.

**Conclusion for multi-stream use**: Proprietary, and not designed for interleaving. Not suitable.

---

#### Ogg

**Popularity**: ★★★☆☆ — widely used as the container for Vorbis audio, Theora video, Opus audio, FLAC audio, and other codecs in the open-source ecosystem.

**Format overview**: The Ogg container format (RFC 3533) is defined as a **general-purpose bitstream encapsulation format** — the RFC itself is codec-agnostic and describes a transport for arbitrary logical bitstreams, not just multimedia. Each page belongs to exactly one **logical bitstream**, identified by a 32-bit serial number in the page header. Pages from different logical bitstreams are freely interleaved in the physical bitstream. A physical bitstream may contain:

- A single logical bitstream (simple file).
- Multiple **sequential** logical bitstreams (chained streams, one ends before the next begins).
- Multiple **concurrent** logical bitstreams (grouped streams, beginning of all streams before any data pages — used for audio+video multiplexing).

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each Ogg page is self-contained with its own header, CRC, and segment table; pages are written sequentially and no previously-written bytes are ever modified.

**Sequential-read streaming**: ✅ Fully supported. Pages can be demultiplexed by serial number in a single forward pass.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. `oggenc` (encoding), `ffmpeg` (via libavformat), and `libogg` all handle multi-stream interleaved Ogg files. For example, an Ogg file with concurrent Vorbis (audio) and Theora (video) streams is the standard encoding.

**Chunk interleaving (theoretical)**: ✅ Fully specified in RFC 3533. The Ogg page structure explicitly carries a stream serial number and granule position to support arbitrary interleaving.

**Conclusion for multi-stream use**: Ogg is technically well-suited for multi-stream interleaved streaming. RFC 3533 defines it as a **general-purpose bitstream encapsulation format** — the spec itself is codec-agnostic.

**However, in practice Ogg is entirely a multimedia format.** No general-purpose archive tool recognizes it — you cannot `7z x file.ogg` or `tar tf file.ogg`. Every existing implementation (`libogg`, `oggenc`, `oggz`, `ffmpeg`, VLC) is audio/video-oriented. The `.ogg`/`.ogv`/`.oga` extensions are universally associated with multimedia. File managers, archive utilities, and operating systems all classify Ogg as "audio/video container." Using Ogg for arbitrary data multiplexing would be spec-compliant but tooling-incompatible — a general-purpose CLI multiplexer would need to write Ogg pages directly (the format is simple: 27-byte header + segment table + data, ~200 lines of C), but no user would expect `file.ogg` to contain non-multimedia data, and no existing tool would help them inspect or extract it.

This gap between "general-purpose by specification" and "multimedia-only in practice" is Ogg's fundamental limitation as a candidate for general-purpose pipe multiplexing.

---

#### Matroska / MKV

**Popularity**: ★★★★☆ — the dominant open container for high-quality video (`.mkv`), audio (`.mka`), and subtitles (`.mks`) on PC. WebM is a restricted subset used on the web.

**Format overview**: Matroska is built on **EBML** (Extensible Binary Meta Language), a self-describing hierarchical binary format similar in concept to XML but binary. A Matroska file contains a `Segment` element which holds `Cluster` elements. Each `Cluster` contains `SimpleBlock` or `BlockGroup` elements, each tagged with a **TrackNumber**. Tracks are declared in a `TrackEntry` section at the start.

**Sequential-write streaming**: ⚠️ Possible without patching, but conditional. In streaming mode, the `SeekHead` element (which contains offsets to other top-level elements and would require patching) can be **omitted entirely**. The `Segment` element can use an unknown-size EBML length (all 1-bits), and `Cluster` elements are written sequentially. This produces a valid Matroska stream with no bytes ever patched. `ffmpeg -f matroska pipe:1` uses this mode. However, the resulting file lacks random-access metadata — it is a true stream, not a seekable file.

**Sequential-read streaming**: ✅ Fully supported. A reader can scan through clusters and decode individual track blocks without a `SeekHead`; forward-only reading works correctly.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. All Matroska muxers (`ffmpeg`, `mkvmerge`) interleave blocks from different tracks (video, audio, subtitle, chapters, attachments) at the cluster level. Audio and video blocks are interleaved to minimize buffer requirements during playback.

**Chunk interleaving (theoretical)**: ✅ Fully specified. The Matroska specification requires interleaved clusters for compliant multiplexed files and defines timecode-ordered interleaving semantics.

**Conclusion for multi-stream use**: Matroska/EBML is an excellent multi-stream interleaved container, well-specified and with broad tooling. The main barrier to use as a general-purpose pipe format is its complexity and multimedia-oriented tooling.

---

#### WebM

**Popularity**: ★★★☆☆ — subset of Matroska used on the web, supported natively by all major browsers.

**Format overview**: WebM is a constrained profile of Matroska (EBML) allowing only VP8/VP9/AV1 video and Vorbis/Opus audio tracks. Structural interleaving is identical to Matroska.

**Sequential-write streaming**: ⚠️ Same as Matroska.

**Sequential-read streaming**: ✅ Same as Matroska.

**Chunk interleaving (implementations)**: ✅ Same as Matroska.

**Chunk interleaving (theoretical)**: ✅ Same as Matroska.

**Conclusion for multi-stream use**: Same as Matroska; even more constrained codec-wise.

---

#### MPEG-TS (MPEG Transport Stream)

**Popularity**: ★★★★☆ — the standard broadcast and streaming container for digital television (DVB, ATSC, IPTV), Blu-ray discs, HLS (HTTP Live Streaming), and real-time streaming.

**Format overview**: MPEG-TS is built on fixed-size **188-byte packets**. Each packet carries a 13-bit **PID** (Packet Identifier) that associates it with a specific elementary stream (video, audio, subtitle, data). A **PAT** (Program Association Table) and **PMT** (Program Map Table) at well-known PIDs describe the composition of programs. This fixed-packet structure makes MPEG-TS trivially splittable and error-resilient.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each 188-byte packet is self-contained and written forward-only. Broadcast encoders produce a continuous MPEG-TS stream in real time with no seek-back.

**Sequential-read streaming**: ✅ Fully supported. Packets can be demultiplexed in a single forward scan by PID. Error recovery is possible even with packet loss (unlike most archive formats).

**Chunk interleaving (implementations)**: ✅ Native to the format. Every MPEG-TS encoder (`ffmpeg`, hardware broadcast encoders, GStreamer) interleaves packets from different elementary streams. This is the core design of the format.

**Chunk interleaving (theoretical)**: ✅ Fully specified in ISO 13818-1. Interleaving by PID is mandatory and fundamental.

**Conclusion for multi-stream use**: MPEG-TS is arguably the most mature and robust format for streaming interleaved data. It was designed specifically for unreliable broadcast channels. Its fixed 188-byte packet size adds up to ~3% overhead (4-byte header per 184 bytes payload) but enables random access and synchronization anywhere in the stream. The tooling (ffmpeg, VLC, GStreamer, mpegts libraries) is excellent. The main limitation for general-purpose use is that it is heavily oriented toward multimedia PIDs and requires PAT/PMT metadata, which adds conceptual overhead when used for non-AV data.

---

#### MPEG-PS (MPEG Program Stream)

**Popularity**: ★★★☆☆ — used in DVD-Video, SVCD, and early digital video applications. Less common now than MPEG-TS.

**Format overview**: MPEG-PS uses variable-length **pack** and **packet** structures rather than fixed-size transport packets. Elementary streams are identified by **stream IDs** in PES (Packetized Elementary Stream) headers.

**Sequential-write streaming**: ✅ No patching needed — packs and PES packets are self-contained and written sequentially.

**Sequential-read streaming**: ✅ Supported.

**Chunk interleaving (implementations)**: ✅ Yes — all MPEG-PS muxers interleave PES packets from different streams.

**Chunk interleaving (theoretical)**: ✅ Specified in ISO 13818-1.

**Conclusion for multi-stream use**: Less suited than MPEG-TS for general use due to lower error resilience and declining adoption.

---

#### ASF (Advanced Systems Format) / WMV / WMA

**Popularity**: ★★★☆☆ — Microsoft's container for Windows Media Video and Audio. Common on Windows but rare elsewhere.

**Format overview**: ASF is a packet-based format with an object hierarchy. A **Header Object** describes streams and properties. **Data Object** contains interleaved **Data Packets**, each containing one or more **payloads** tagged by **Stream Number**.

**Sequential-write streaming**: ⚠️ Conditional — the ASF Header Object contains a `File Properties Object` with `File Size` and `Data Packets Count` fields. In live/streaming scenarios, these are set to zero/sentinel values and never patched, producing a valid stream. When writing to a seekable file, implementations typically seek back to patch these fields. So no-patching streaming is possible but produces an imprecise header.

**Sequential-read streaming**: ✅ Supported after reading the header.

**Chunk interleaving (implementations)**: ✅ Yes — Windows Media Encoder and ffmpeg interleave packets from different streams.

**Chunk interleaving (theoretical)**: ✅ Specified in the ASF specification. Multiple payloads per packet are supported.

**Conclusion for multi-stream use**: Technically capable, but proprietary and primarily Windows-oriented. Not a good choice for open/cross-platform use.

---

#### CAF (Core Audio Format)

**Popularity**: ★★☆☆☆ — Apple's audio container format. Used on macOS and iOS.

**Format overview**: CAF uses a chunk-based layout similar in spirit to RIFF/AIFF but 64-bit clean. Chunks are identified by 4-byte type codes. The format supports multiple audio tracks and metadata.

**Sequential-write streaming**: ✅ No patching needed — setting chunk size to `-1` signals that the size is unknown (streaming mode). All data is written forward-only.

**Sequential-read streaming**: ✅ Supported.

**Chunk interleaving (implementations)**: ✅ For audio channels/tracks within the file.

**Chunk interleaving (theoretical)**: ✅ Defined in the CAF specification.

**Conclusion for multi-stream use**: Apple-centric and audio-only in practice. Not suitable for general-purpose multi-stream piping.

---

#### HTTP/2 Framing (RFC 7540 / RFC 9113)

**Popularity**: ★★★★★ — the dominant web transport protocol since 2015. Every major browser, web server, CDN, and proxy supports HTTP/2. Libraries exist in every language (nghttp2 in C, h2 in Rust/Python, net/http in Go, etc.).

**Format overview**: HTTP/2's binary framing layer is a **general-purpose stream multiplexer**. Each frame has a 9-byte header: 3-byte length, 1-byte type, 1-byte flags, and a 31-bit **stream identifier**. Multiple streams are interleaved over a single connection. While HTTP/2 defines frame types for HTTP semantics (HEADERS, DATA, etc.), the underlying framing layer is a clean length-prefixed multiplexer.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each frame is self-contained with its length in the header; frames are written forward-only.

**Sequential-read streaming**: ✅ Fully supported. A reader demultiplexes by stream ID in a single forward pass.

**Chunk interleaving (implementations)**: ✅ Native — every HTTP/2 implementation interleaves frames from concurrent streams. This is the core design of the protocol.

**Chunk interleaving (theoretical)**: ✅ Fully specified in RFC 7540 § 5 (Streams and Multiplexing). Stream multiplexing is the primary feature of HTTP/2 over HTTP/1.1.

**Conclusion for multi-stream use**: HTTP/2 framing is the most widely deployed general-purpose multiplexing format in existence. It is **not multimedia-specific** — it was designed for arbitrary data streams. The framing layer alone (without HTTP semantics) maps cleanly to "stream N, chunk M" use cases. The main drawback is that HTTP/2 carries significant protocol-level complexity beyond just framing (flow control, HPACK header compression, stream priorities, SETTINGS negotiation) — extracting just the framing layer means ignoring most of the spec. Overhead is ~9 bytes per frame (~0.05% at 16 KB payloads).

---

#### SSH Channel Protocol (RFC 4254)

**Popularity**: ★★★★★ — SSH is installed on virtually every server and developer machine worldwide. OpenSSH, libssh, libssh2, Paramiko, golang.org/x/crypto/ssh — implementations exist in every major language.

**Format overview**: The SSH connection protocol (RFC 4254) multiplexes multiple **channels** over a single encrypted SSH connection. Each channel has a numeric ID and data is sent in `SSH_MSG_CHANNEL_DATA` messages tagged with the channel number. Channels can be opened, closed, and flow-controlled independently.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Channel data messages are written forward-only.

**Sequential-read streaming**: ✅ Fully supported. Messages are demultiplexed by channel ID in a single forward pass.

**Chunk interleaving (implementations)**: ✅ Native — every SSH implementation interleaves data from multiple channels (e.g., port forwarding, X11, shell sessions simultaneously).

**Chunk interleaving (theoretical)**: ✅ Fully specified in RFC 4254. Channel multiplexing is the core design of the SSH connection protocol.

**Conclusion for multi-stream use**: SSH channels are a proven general-purpose multiplexing mechanism with 30 years of production use. However, SSH carries substantial protocol overhead (encryption, key exchange, MAC, connection setup) that is unnecessary for local piping between processes. Using SSH channel framing without the full SSH protocol would mean extracting a small part of a large spec. More suitable as prior art than as a direct candidate.

---

#### HTTP/1.1 Response Format (RFC 7230 — Chunked Encoding + Multipart)

**Popularity**: ★★★★★ — HTTP/1.1 is arguably the most universally implemented application protocol in history. Every programming language, every OS, every networked device supports it. The chunked transfer encoding and MIME multipart mechanisms are understood by billions of deployed clients and servers.

**Format overview**: HTTP/1.1 offers two mechanisms potentially relevant to multi-stream piping:

1. **Chunked Transfer Encoding** (RFC 7230 §4.1): Allows streaming a response body of unknown length. Each chunk is framed as `<hex-length>\r\n<data>\r\n`, terminated by a zero-length chunk. This is a single-stream framing mechanism — it solves the "unknown total size" problem but does **not** natively multiplex multiple streams.

2. **MIME Multipart responses** (RFC 2046 / used in HTTP as `multipart/mixed`, `multipart/byteranges`): Allows a single HTTP response to contain multiple body parts separated by a boundary string. Each part has its own headers (`Content-Type`, `Content-Range`, etc.) and body. However, parts are **sequential** — each part must be complete before the next boundary and next part begin.

**Chunked encoding with chunk extensions for multiplexing**: RFC 7230 §4.1.1 defines **chunk extensions** — semicolon-delimited key-value pairs appended to the chunk size line:
```
1a;stream=0\r\n
<26 bytes of data for stream 0>\r\n
2f;stream=1\r\n
<47 bytes of data for stream 1>\r\n
0\r\n
\r\n
```
This is syntactically valid HTTP/1.1. Existing HTTP clients/proxies would parse the chunks correctly (they are required to accept and may ignore unknown extensions per the RFC). In theory, a multiplexer could tag each chunk with a stream identifier, and a custom demultiplexer could reconstruct the original streams.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Chunked encoding was designed specifically for forward-only streaming of unknown-length content.

**Sequential-read streaming**: ✅ Fully supported. Chunks are self-delimiting and parseable in a single forward pass.

**Chunk interleaving (implementations)**: ❌ No known implementation uses chunk extensions for stream multiplexing **over HTTP**. All existing HTTP/1.1 usage treats chunked encoding as a single-stream framing. MIME multipart responses are sequential (one part at a time), not interleaved.

**Chunk interleaving (theoretical / pipe use)**: ✅ **Actually viable for pipes.** Chunk extensions are fully specified in RFC 7230 §4.1.1 — adding `;stream=N` to each chunk is syntactically valid, well-defined, and unambiguous. For **pipe use** (not going through HTTP proxies), this is a practical multiplexing approach:

- The format is **text-based** — human-readable and debuggable with `cat`, `head`, `hexdump`, standard text tools.
- The framing is **simple to implement** — a parser only needs to read a hex number + optional extensions + CRLF, then read that many bytes + CRLF.
- Chunk extensions are **part of the spec**, not a hack — RFC 7230 §4.1.1 defines the syntax explicitly, and §4.1.2 even defines trailer headers after the final chunk.
- **Overhead is low**: hex-length (1–8 chars) + `;stream=N` (~10 chars) + `\r\n` (2 bytes) + data + `\r\n` (2 bytes) = ~15–22 bytes per chunk. At 64 KiB payloads: ~0.03%.

The reason this doesn't work **over HTTP infrastructure** is that proxies/CDNs/clients would silently strip or ignore the `;stream=N` extension and flatten all chunks into one concatenated body. But over a **pipe**, there is no intermediate infrastructure — the producer writes directly to the consumer, so the stream IDs are preserved end-to-end.

**Limitations of the chunk-extension approach**:
- No existing tooling supports it — you'd need a custom muxer/demuxer (but the same is true for any custom LTV format).
- MIME `multipart/mixed` boundaries delimit **complete sequential parts**, not interleaved chunks. There is no standard way to interleave MIME parts.
- Not 7z-extractable.
- HTTP/2 was invented because HTTP/1.1 lacked native multiplexing **for the web** — but for pipes, the simpler text-based framing may be an advantage.

**Overhead analysis**:
- Chunked encoding per chunk: hex-length (1–8 chars) + `;stream=N` (~10 chars) + `\r\n` (2 bytes) + data + `\r\n` (2 bytes) = ~15–22 bytes overhead per chunk. At 64 KiB payloads: ~0.03%.
- MIME multipart per part: boundary line (~30–70 bytes) + part headers (~50–100 bytes) + `\r\n` separators = ~100–200 bytes per part. But parts are sequential, not interleaved chunks.

**Conclusion for multi-stream use**: HTTP/1.1 chunked encoding with `;stream=N` extensions is **a viable text-based multiplexing approach for pipes**. It is well-specified (RFC 7230), text-based (human-readable), low-overhead (~0.03% at 64 KiB), and trivial to implement. The key limitation is that **no existing tool supports this convention** — but the same is true for any custom format. The advantage over binary LTV is debuggability; the disadvantage is slightly higher parsing overhead and the need for CRLF delimiter handling. Over HTTP infrastructure it wouldn't work (proxies flatten stream IDs), but over a pipe, the producer and consumer are directly connected and the extensions are preserved.

This makes HTTP/1.1 chunked encoding a strong candidate alongside Ogg and custom LTV for pipe multiplexing — especially when text-based debuggability is valued.

---

#### ISO 9660 (CD-ROM / Optical Disc Image)

**Popularity**: ★★★★☆ — the universal filesystem format for CD-ROMs, also widely used for disc images (`.iso`). Supported by every operating system.

**Format overview**: ISO 9660 is a **read-only filesystem**, not an archive format. It stores a volume descriptor set at fixed sectors near the beginning (sectors 16–N), followed by path tables and directory records that reference files by absolute sector offsets. Extensions include Rock Ridge (POSIX attributes), Joliet (Unicode names), and El Torito (bootable images). The format is designed for random access on optical media, with directory records containing sector offsets and lengths for all files.

**Sequential-write streaming**: ❌ Not supported — **requires pre-computation**. The volume descriptors (at sector 16+) contain the root directory record's sector offset and the path table location. Directory records contain sector offsets for all files. A writer must therefore know the layout of the entire filesystem before writing the volume descriptors. Tools like `mkisofs`/`genisoimage` compute the complete layout in memory and then write everything in one pass, but this requires holding the full directory tree in memory and knowing all file sizes upfront. The format cannot be incrementally written as files are produced.

**Sequential-read streaming**: ❌ Not practical. ISO 9660 is designed for random-access seek-based reading. While it's technically possible to scan sectors forward, the format assumes readers will jump to specific sectors via offsets in directory records.

**Chunk interleaving (implementations)**: ❌ None. ISO 9660 stores each file as a contiguous extent of sectors.

**Chunk interleaving (theoretical)**: ❌ Not in spec. ISO 9660 §6.6.1 defines an "interleave" mode for files (interleave gap size + interleave unit size fields in directory records), but this is for spreading a single file across non-contiguous sectors on optical media for performance reasons — not for multiplexing different files/streams.

**Conclusion for multi-stream use**: ISO 9660 is a filesystem, not a streaming format. Entirely unsuitable for pipe-based multi-stream use.

---

#### WIM (Windows Imaging Format)

**Popularity**: ★★★☆☆ — Microsoft's format for Windows deployment images. Used by DISM, ImageX, and the Windows installer for OS images. Not commonly used outside Windows deployment.

**Format overview**: WIM is a **file-based capture format** designed to store complete filesystem snapshots. A WIM file has a header at offset 0, followed by resource data (optionally compressed and deduplicated), and a metadata/integrity table at the end. The header contains a resource table offset and size, XML metadata offset, and optional integrity table offset — all of which point to structures at the end of the file.

**Sequential-write streaming**: ❌ Not supported — **requires patching**. The WIM header at offset 0 must be updated with the resource table offset and size after all resource data has been written. Microsoft's `wimlib` (`wimcapture`) writes resources first, then appends the lookup table, XML data, and integrity table, and finally seeks back to patch the header. This is a fundamental design requirement, not an implementation choice.

**Sequential-read streaming**: ❌ Not practical. Readers need the resource table (at end-of-file offset from the header) to locate individual files.

**Chunk interleaving (implementations)**: ❌ None known. Resources are stored sequentially.

**Chunk interleaving (theoretical)**: ❌ Not in spec.

**Conclusion for multi-stream use**: WIM is a deployment-image format requiring seek access. Not suitable for streaming or interleaving.

---

#### CAB (Microsoft Cabinet)

**Popularity**: ★★★☆☆ — Microsoft's standard archive format for software distribution (Windows installers, `.msi`, driver packages, Windows Update). Common on Windows, rare elsewhere.

**Format overview**: A CAB file starts with a **CFHEADER** structure containing the total cabinet size, file count, folder count, and an offset to the first `CFFILE` entry. This is followed by `CFFOLDER` structures (each describing a compression method and offset to compressed data), then `CFFILE` structures (describing files within folders), then the compressed data blocks. Multi-cabinet spanning is supported (prev/next cabinet references in the header).

**Sequential-write streaming**: ❌ Not supported — **requires patching or pre-computation**. The cabinet header contains the total cabinet size and the offset to the first `CFFILE` entry, both of which are unknown until all data has been compressed. Microsoft's `makecab` and `libmspack` compute the layout before writing. While the data blocks themselves could theoretically be streamed, the header references fixed offsets that must be written first.

**Sequential-read streaming**: ⚠️ Partially possible. A reader could scan forward through the structures if the header is present, since the header points to the first CFFILE entry and data follows in order. However, the header must be complete before reading begins, so this is not true streaming in the pipe sense.

**Chunk interleaving (implementations)**: ❌ None known. Files within a folder share a single compressed data stream; different folders are sequential.

**Chunk interleaving (theoretical)**: ❌ Not in spec. CAB's "folder" concept groups files into a single compressed stream, which is the opposite of interleaving — it merges multiple files into one stream for better compression.

**Conclusion for multi-stream use**: CAB is a Windows-specific installation archive that requires pre-computed headers. Not suitable for streaming or interleaving.

---

#### MP4 / ISOBMFF (ISO Base Media File Format)

**Popularity**: ★★★★★ — the dominant multimedia container format today. Used for `.mp4`, `.m4a`, `.m4v`, DASH streaming, Apple HLS (fragmented MP4), and the basis for HEIF/AVIF image containers. Specified in ISO 14496-12.

**Format overview**: ISOBMFF structures data as a hierarchy of **boxes** (atoms). A regular MP4 file has a `moov` box (movie metadata: track descriptions, sample tables, chunk offsets) and one or more `mdat` boxes (actual media data). The `moov` box contains absolute byte offsets (`stco`/`co64` entries) pointing into `mdat`. **Fragmented MP4** (fMP4) replaces the monolithic `moov`/`mdat` structure with a sequence of **fragments**: each fragment has a `moof` (Movie Fragment) box followed by an `mdat` box. The `moof` contains relative offsets within its own `mdat`, enabling sequential writing.

**Sequential-write streaming**: ⚠️ **Only fragmented MP4 (fMP4)** — regular MP4 requires the `moov` box (which contains byte offsets into `mdat`) to be written either before or after the media data, requiring either pre-computation of sizes or seeking back to patch. With fMP4, each `moof`+`mdat` pair is self-contained and can be written incrementally. This is how DASH and HLS live streaming work.

**Sequential-read streaming**: ⚠️ **Only fragmented MP4** — regular MP4 with `moov` at end requires seeking. With `moov` at start (and no `stco` patching), forward reading works but writing required pre-computation. fMP4 fragments can be read sequentially.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. In regular MP4, audio and video samples within `mdat` are interleaved by chunk offsets. In fMP4, each fragment can contain samples from multiple tracks interleaved. All major tools (`ffmpeg`, `MP4Box`, `Bento4`) produce interleaved output.

**Chunk interleaving (theoretical)**: ✅ Fully specified in ISO 14496-12. Track interleaving is a core design feature.

**Conclusion for multi-stream use**: fMP4 is technically capable of streaming and interleaving, and is the basis for modern live video delivery. However, the box/atom structure is complex, multimedia-specific (track descriptions, timescales, sample flags), and requires understanding of ISO 14496-12 to produce valid output. Not practical for general-purpose data multiplexing.

---

#### AVI / RIFF (Resource Interchange File Format)

**Popularity**: ★★★★☆ — AVI was the dominant video container on Windows from 1992 through the mid-2000s. RIFF (the parent format) is also used for WAV audio and WebP images. Still widely supported but largely superseded by MP4/MKV for new content.

**Format overview**: RIFF is a tagged chunk format developed by Microsoft and IBM in 1991, inspired by EA's IFF (1985). Every chunk has a 4-byte FourCC type identifier and a 4-byte little-endian size. Chunks are grouped into `LIST` containers. AVI (`RIFF 'AVI '`) uses a `LIST 'hdrl'` (header list with stream descriptions), a `LIST 'movi'` (interleaved audio/video chunks identified by stream index: `00dc` for video stream 0, `01wb` for audio stream 1, etc.), and an optional `idx1` index at the end. RIFF itself is conceptually general-purpose — any FourCC-tagged data can be placed in chunks.

**Sequential-write streaming**: ❌ Not supported — **RIFF header requires total size**. The outermost RIFF chunk header contains the total file size (minus 8 bytes). While the interleaved data chunks in `LIST 'movi'` can be written sequentially, the top-level RIFF size and the `LIST 'movi'` size must either be pre-computed or patched. OpenDML extensions (AVI 2.0) allow `AVIX` continuation chunks for files > 2 GB but still require size fields.

**Sequential-read streaming**: ⚠️ Partial. The `LIST 'movi'` chunks can be read forward-only by scanning FourCC+size pairs. However, the `idx1` index at the end is needed for seeking, and the header must be parsed first for stream descriptions.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. AVI interleaves audio and video chunks within `LIST 'movi'`: a typical layout alternates `00dc` (video frame), `01wb` (audio chunk), `00dc`, `01wb`, etc. All AVI muxers (`ffmpeg`, `VirtualDub`, `mencoder`) produce interleaved output.

**Chunk interleaving (theoretical)**: ✅ Fully specified. RIFF's chunk model allows arbitrary interleaving of differently-typed chunks.

**Conclusion for multi-stream use**: RIFF/AVI's chunk model is conceptually general-purpose (FourCC-tagged chunks), but the mandatory size fields in RIFF/LIST headers prevent true streaming. Interleaving is native and well-proven. Historically important as one of the first widely-deployed interleaved containers, but not suitable for pipe use due to header size requirements.

---

#### IFF (Interchange File Format)

**Popularity**: ★★☆☆☆ — created by Electronic Arts in 1985 for the Amiga platform. Historically significant as the **first widely-used general-purpose tagged container format**. Influenced RIFF/AVI/WAV (Microsoft's adaptation) and by extension the entire family of chunk-based containers. Used for Amiga IFF/ILBM images, 8SVX audio, AIFF audio (Apple's adaptation). Rarely encountered today outside retro computing.

**Format overview**: IFF uses a simple chunk structure: each chunk has a 4-byte type ID and a 4-byte big-endian size, followed by that many bytes of data. Chunks are grouped into `FORM`, `LIST`, or `CAT ` (concatenation) containers, each of which also has a type+size header. The format is explicitly designed as a general-purpose data interchange standard — the 1985 specification describes it as "a standard for interchange of data between programs." IFF was one of the first formats to use the now-ubiquitous type-length-value (TLV) pattern for structured binary data.

**Sequential-write streaming**: ⚠️ Conditional. Each chunk's size must be known when its header is written. If chunk sizes are known upfront (e.g., fixed-size records), no patching is needed. If sizes are unknown, the writer must either buffer the entire chunk or seek back to patch the size field. `FORM`/`LIST`/`CAT ` group sizes have the same constraint.

**Sequential-read streaming**: ✅ Supported. Chunks can be read sequentially by reading type+size, then skipping or processing size bytes.

**Chunk interleaving (implementations)**: ❌ None known. IFF files typically contain a single `FORM` with sequential chunks.

**Chunk interleaving (theoretical)**: ❌ Not in spec. While `CAT ` groups could theoretically contain interleaved `FORM`s, the spec does not describe interleaving.

**Conclusion for multi-stream use**: Historically important as the progenitor of TLV-based container formats (RIFF, AVI, AIFF). The chunk model is general-purpose by design, but mandatory upfront chunk sizes prevent streaming of variable-length data, and no interleaving mechanism exists.

---

#### FLV (Flash Video)

**Popularity**: ★★★☆☆ — was the dominant web video format from ~2005–2012 (YouTube, Twitch, etc., before the shift to MP4/DASH/HLS). Still used internally by RTMP streaming (which carries FLV-structured data). Declining in relevance since Adobe discontinued Flash in 2020.

**Format overview**: FLV has a 9-byte file header followed by a sequence of **FLV tags**. Each tag has a 1-byte type (0x08=audio, 0x09=video, 0x12=script data), a 3-byte data size, a 4-byte timestamp (3 bytes + 1 extension byte), a 3-byte stream ID (always 0 in practice), and then the tag data. After each tag, a 4-byte "PreviousTagSize" field enables backward scanning. The flat tag sequence design was specifically chosen for streaming — FLV was built for progressive download and live RTMP streaming.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each tag is self-contained (type + size + timestamp + data) and can be written immediately. No header fields reference future byte offsets.

**Sequential-read streaming**: ✅ Fully supported. Tags can be demultiplexed in a single forward pass.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. Audio tags and video tags (and script data tags) are interleaved by timestamp in the tag sequence. All FLV muxers (`ffmpeg`, Flash Media Server, OBS/RTMP) produce interleaved output.

**Chunk interleaving (theoretical)**: ✅ Fully specified. The tag-based structure with type field is designed for interleaved audio/video delivery.

**Conclusion for multi-stream use**: FLV's flat tag structure is one of the simplest streaming-capable interleaved formats. However, the stream ID field (always 0 in practice) was never used for multi-stream multiplexing — FLV carries one audio + one video + one data stream per file. The format is also deprecated and tightly tied to the Flash/RTMP ecosystem.

---

#### WARC (Web ARChive Format)

**Popularity**: ★★★☆☆ — the standard format for web archiving, used by the Internet Archive's Wayback Machine, national libraries, Common Crawl, and all major web archiving tools. ISO 28500:2017 standardized.

**Format overview**: WARC is a sequential record format. Each record begins with `WARC/1.0\r\n` followed by named headers (key: value, similar to HTTP headers), a blank line, and then the record payload. Headers include `WARC-Type` (warcinfo, request, response, resource, etc.), `Content-Length`, `WARC-Record-ID` (UUID-based URI), and `WARC-Date`. Records are separated by `\r\n\r\n`. The format is explicitly designed for **sequential writing and reading** — web crawlers append records as pages are fetched.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each record is self-contained (headers + content-length + payload). Records are appended sequentially. No global index or size fields.

**Sequential-read streaming**: ✅ Fully supported. Records can be read in a single forward pass.

**Chunk interleaving (implementations)**: ❌ None known. WARC records are complete, self-contained units (an entire HTTP response or resource). There is no mechanism to split a resource across multiple records and interleave them.

**Chunk interleaving (theoretical)**: ❌ Not in spec. Each WARC record encapsulates a complete resource. The `WARC-Segment-Number` header supports segmentation of very large resources, but segments must appear in order and cannot be interleaved with other records.

**Conclusion for multi-stream use**: WARC is an interesting **non-multimedia general-purpose** sequential format with ISO standardization. Its HTTP-like header syntax is human-readable and extensible. However, it's fundamentally sequential (like TAR/CPIO) with no interleaving, making it unsuitable for concurrent multi-stream multiplexing.

---

#### XAR (eXtensible ARchive)

**Popularity**: ★★☆☆☆ — created by Apple in 2007 for macOS `.pkg` installer packages. Used internally by Xcode and macOS installer infrastructure. Rare outside the Apple ecosystem.

**Format overview**: XAR has a simple three-part structure: a **header** (magic + size + TOC length + TOC checksum info), an **XML table of contents** (full file listing with names, sizes, checksums, compression types, and **byte offsets/lengths into the heap**), and a **heap** (concatenated, optionally compressed file data). The XML TOC is complete and self-contained — it maps every file to an exact (offset, length) pair in the heap.

**Sequential-write streaming**: ❌ Not supported. The XML TOC must be written before the heap, but it contains byte offsets and lengths for heap data that hasn't been written yet. This requires either pre-computation of all compressed sizes or buffering all data, then writing TOC + heap.

**Sequential-read streaming**: ❌ Not supported as a stream. However, once the TOC is parsed, heap data can be read in one forward pass (entries are typically stored in the same order as listed in the TOC).

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec. The heap is a flat concatenation of file data; the TOC indexes into it by offset.

**Conclusion for multi-stream use**: XAR's design (complete TOC upfront referencing a flat heap) is the opposite of streaming. Not suitable for pipe use.

---

#### NUT (NUT Open Container Format)

**Popularity**: ★☆☆☆☆ — designed by FFmpeg/MPlayer developers in 2003 as a "better" multimedia container that fixes limitations of AVI, MP4, and Matroska. Supported by FFmpeg but never achieved significant adoption. Occasionally used for intermediate processing in FFmpeg pipelines.

**Format overview**: NUT uses a frame-based structure with **startcodes** (8-byte sync patterns) for error recovery. The main header describes streams (number, type, codec, timebase). Frames have a header with stream ID, PTS, size, and flags. NUT was specifically designed for **streaming** — headers can be repeated periodically for mid-stream joining (like MPEG-TS), frames are self-delimiting, and the format supports both forward-only reading and error recovery via startcode scanning.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Frames are written sequentially with per-frame headers. Main headers can be repeated for join-in-progress support.

**Sequential-read streaming**: ✅ Fully supported. Frames can be demultiplexed in a single forward pass. Startcodes enable resynchronization after errors.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. `ffmpeg -f nut` produces interleaved output with audio and video frames multiplexed by timestamp.

**Chunk interleaving (theoretical)**: ✅ Fully specified. The frame header contains a stream ID; frames from different streams are interleaved by design.

**Conclusion for multi-stream use**: NUT was purpose-built to be a clean streaming-capable interleaved container. It has the right technical properties (streaming, interleaving, error recovery, per-stream identifiers). However, it has negligible adoption outside FFmpeg, no independent implementations, and is entirely multimedia-focused.

---

#### Avro Object Container File (Apache Avro)

**Popularity**: ★★★☆☆ — widely used in the Apache Hadoop/Kafka ecosystem for data serialization and storage. Avro is the default serialization format for Apache Kafka and is supported by Spark, Flink, and most big-data tools.

**Format overview**: An Avro Object Container File (OCF) starts with a **file header** containing magic bytes, metadata (including the JSON schema for all records), and a 16-byte random sync marker. The file then contains a sequence of **data blocks**: each block has a count of records, the serialized size in bytes, the compressed record data, and the sync marker for error recovery. The sync marker enables splitting/joining files at block boundaries.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Blocks are written sequentially; each block is self-contained (count + size + data + sync marker). New blocks are appended as records become available.

**Sequential-read streaming**: ✅ Fully supported. Blocks can be read and decompressed in order.

**Chunk interleaving (implementations)**: ❌ None known. An Avro OCF contains records conforming to a **single schema**. There is no mechanism for multiple concurrent schemas or streams within one file.

**Chunk interleaving (theoretical)**: ❌ Not in spec. The single-schema design is fundamental — an Avro OCF represents a table (homogeneous rows), not a multiplexed stream of heterogeneous data.

**Conclusion for multi-stream use**: Avro OCF is a well-designed streaming format for homogeneous data (all records share one schema). Not suitable for multi-stream multiplexing of heterogeneous data. Interesting as prior art for block-based streaming with sync markers.

---

#### Protocol Buffers (Delimited / Length-Prefixed Messages)

**Popularity**: ★★★★☆ — Google's Protocol Buffers (protobuf) is one of the most widely used serialization formats. The "delimited" convention (length-prefixed messages in a stream) is used by gRPC, Google internal systems, and many open-source tools. Not a formal container specification — the delimited framing is a convention, not part of the protobuf spec itself.

**Format overview**: A delimited protobuf stream consists of messages prefixed by their serialized size as a varint (variable-length integer encoding). Each message is an independent protobuf-encoded unit. This convention is used by Java's `writeDelimitedTo()`/`parseDelimitedFrom()`, gRPC's wire format (5-byte header: 1-byte compressed flag + 4-byte big-endian length), and many ad-hoc implementations.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each message is length-prefixed and self-contained.

**Sequential-read streaming**: ✅ Fully supported. Read varint length, read that many bytes, decode message, repeat.

**Chunk interleaving (implementations)**: ❌ None known as a standard. gRPC carries a single message type per stream. Interleaving would require an envelope message with a stream ID field.

**Chunk interleaving (theoretical)**: ⚠️ Possible with an envelope. A protobuf message could contain a `stream_id` field and an `oneof` payload, but this is application-level design, not part of any protobuf standard.

**Conclusion for multi-stream use**: Protobuf delimited messages are an excellent minimal framing mechanism (varint length prefix + payload) with massive ecosystem support. Multi-stream multiplexing would require a custom envelope message — essentially reinventing LTV framing on top of protobuf. More practical as a serialization layer *within* a multiplexing container than as the container itself.

---

#### QUIC (RFC 9000)

**Popularity**: ★★★★☆ — QUIC is a modern transport protocol standardized in 2021 (RFC 9000), used by HTTP/3 (RFC 9114). Deployed by Google, Cloudflare, Facebook/Meta, and Akamai. Carries ~30% of global web traffic as of 2024.

**Format overview**: QUIC is a multiplexed, encrypted transport protocol over UDP. It provides **multiple independent streams** within a single connection, each identified by a 62-bit stream ID. Streams are fully independent — no head-of-line blocking between streams (unlike HTTP/2 over TCP). Data is carried in STREAM frames with stream ID, offset, length, and a FIN bit for stream termination. QUIC also supports unidirectional and bidirectional streams, and DATAGRAM frames (RFC 9221) for unreliable delivery.

**Sequential-write streaming**: ✅ Fully supported — each STREAM frame is self-contained with stream ID + offset + data.

**Sequential-read streaming**: ✅ Fully supported per stream.

**Chunk interleaving (implementations)**: ✅ Native multiplexing. All QUIC implementations (`quiche`, `quinn`, `ngtcp2`, Chromium, `s2n-quic`) multiplex streams at the frame level.

**Chunk interleaving (theoretical)**: ✅ Core design feature — RFC 9000 §2 defines streams as the fundamental multiplexing unit.

**Conclusion for multi-stream use**: QUIC is the state-of-the-art in transport-layer multiplexing: independent streams, no HOL blocking, built-in encryption, per-stream FIN. However, it is a **transport protocol**, not a container format — it requires a QUIC stack (TLS 1.3, congestion control, packet loss recovery), runs over UDP, and is designed for network communication, not local pipe multiplexing. Like HTTP/2 and SSH, it's excellent prior art but impractical for `cmd1 | cmd2` use.

---

#### LHA / LZH

**Popularity**: ★★☆☆☆ — created in 1988 by Haruyasu Yoshizaki. Was extremely popular in Japan and the Amiga community through the 1990s. Used for Japanese software distribution and the Aminet archive. Rare today outside retro computing; some use persists in embedded systems and Japanese legacy software.

**Format overview**: LHA is a header-per-file archive format. Each file entry has a variable-length header containing the filename, compressed/original sizes, timestamps, CRC-16, and compression method identifier, immediately followed by the compressed file data. No central directory or global index — the archive is a pure sequential concatenation of header+data pairs, terminated by a header with size 0.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each header contains the compressed size, written after compression completes for that file. No global index or back-references.

**Sequential-read streaming**: ✅ Fully supported. Files can be extracted in a single forward pass.

**Chunk interleaving (implementations)**: ❌ None known.

**Chunk interleaving (theoretical)**: ❌ Not in spec. Like TAR/CPIO, each file must be written completely before the next begins.

**Conclusion for multi-stream use**: LHA is another sequential archive format in the TAR/CPIO family — good for streaming but without interleaving.

---

#### SCTP (Stream Control Transmission Protocol — RFC 4960)

**Popularity**: ★★★☆☆ — standardized in 2000 (RFC 2960, updated to RFC 4960 in 2007). Used in telecom signaling (SS7 over IP / SIGTRAN), 4G/5G core networks (Diameter, S1AP), and WebRTC data channels. Supported by Linux and FreeBSD kernels natively. Limited adoption outside telecom — most applications use TCP or QUIC instead.

**Format overview**: SCTP is a **multi-stream transport protocol** — distinct from TCP in that it natively supports **multiple independent streams** within a single association (connection). Each DATA chunk carries a stream identifier (16-bit) and stream sequence number. Streams are independent: head-of-line blocking on one stream does not block others. SCTP also supports multi-homing (multiple IP addresses per endpoint) and message-oriented delivery (preserving message boundaries, unlike TCP's byte stream).

**Sequential-write streaming**: ✅ Fully supported — DATA chunks are self-contained with stream ID + sequence number + payload.

**Sequential-read streaming**: ✅ Fully supported per stream.

**Chunk interleaving (implementations)**: ✅ Native multiplexing. The Linux kernel SCTP stack and `lksctp-tools` natively multiplex data across streams.

**Chunk interleaving (theoretical)**: ✅ Core design feature — RFC 4960 §1.3.4 defines multi-streaming as a fundamental protocol capability.

**Conclusion for multi-stream use**: SCTP is the **only IETF-standardized transport protocol with native multi-streaming** (predating QUIC by 21 years). Its 16-bit stream IDs and independent stream semantics map perfectly to multi-stream piping. However, like QUIC and SSH, it is a **kernel-level transport protocol** — using it for local pipe multiplexing would require `socketpair(AF_INET, SOCK_STREAM, IPPROTO_SCTP)` and a full SCTP stack, which is unnecessarily heavy for `cmd1 | cmd2`. Excellent prior art for multi-stream design, particularly the independent stream concept.

---

#### WebSocket (RFC 6455)

**Popularity**: ★★★★★ — universal support in all browsers, web servers, and application frameworks since 2011. Libraries in every language. The standard protocol for real-time web communication (chat, live updates, gaming, collaborative editing).

**Format overview**: WebSocket provides a **framed message protocol** over a single TCP connection. After an HTTP/1.1 upgrade handshake, communication proceeds via binary frames: each frame has an opcode (text/binary/close/ping/pong), a payload length (7/16/64-bit), optional masking (client→server), and the payload data. Messages can be split across multiple frames via fragmentation (FIN bit = 0 for continuation frames).

**Sequential-write streaming**: ✅ Fully supported — frames are self-contained and written forward-only.

**Sequential-read streaming**: ✅ Fully supported.

**Chunk interleaving (implementations)**: ❌ None. WebSocket is a **single logical channel** — there is no stream ID or multiplexing. RFC 6455 §5.4 explicitly prohibits interleaving fragments from different messages: "control frames MAY be injected in the middle of a fragmented message" but data frames from different messages cannot be interleaved.

**Chunk interleaving (theoretical)**: ❌ Not in spec. WebSocket was designed as a single bidirectional channel. The WebSocket community recognized this limitation, leading to the **WebSocket Multiplexing Extension** draft (draft-ietf-hybi-websocket-multiplexing, expired 2013) which proposed adding channel IDs — but it was never standardized and has no implementations. The industry moved to HTTP/2 and WebTransport instead.

**Conclusion for multi-stream use**: WebSocket is an excellent single-stream framing protocol with universal support, but it explicitly lacks multiplexing. The failed multiplexing extension and the industry's move to HTTP/2/WebTransport confirm that multiplexing was the recognized gap.

---

#### D-Bus Wire Protocol

**Popularity**: ★★★★☆ — the standard IPC mechanism on Linux desktops (GNOME, KDE, systemd). Every Linux desktop application uses D-Bus for inter-process communication. `dbus-daemon` runs on virtually all Linux systems. Implementations: libdbus, sd-bus (systemd), GDBus (GLib), zbus (Rust).

**Format overview**: D-Bus uses a **message-based binary protocol** over UNIX domain sockets (or TCP). Messages are typed (method call, method return, error, signal) and contain a header with fields: message type, flags, serial number, destination, sender, interface, member, path, and a body of typed data using D-Bus's type system. The wire format uses native-endian alignment with 8-byte message header alignment. Multiple clients communicate via a message bus daemon (`dbus-daemon`) that routes messages by destination.

**Sequential-write streaming**: ✅ Messages are self-contained and written forward-only — no patching needed.

**Sequential-read streaming**: ✅ Messages can be parsed sequentially.

**Chunk interleaving (implementations)**: ⚠️ Partial — D-Bus multiplexes messages from many senders/receivers, but this is managed by the bus daemon, not at the wire format level. The wire protocol itself is a sequential stream of messages. There is no native "stream" concept — each message is independent.

**Chunk interleaving (theoretical)**: ⚠️ Messages can carry different object paths/interfaces, functioning as a form of multiplexing, but this is application-level routing, not wire-format multiplexing.

**Conclusion for multi-stream use**: D-Bus is a heavyweight IPC system for desktop service communication, not a general-purpose data streaming format. Its message-based model with typed fields is fundamentally different from the byte-stream multiplexing needed for pipe use. The protocol overhead (alignment, type system, bus daemon) makes it impractical for high-throughput stream multiplexing.

---

#### 9P (Plan 9 File Protocol)

**Popularity**: ★★☆☆☆ — the native file protocol of Plan 9 from Bell Labs (1995). Gained wider use through Linux's `v9fs` (9P filesystem client in the kernel), QEMU/KVM's `virtio-9p` for host-guest file sharing, and Windows Subsystem for Linux (WSL2 uses 9P for Windows↔Linux file access). Implementations: Linux kernel, QEMU, `diod`, several Go/Rust libraries.

**Format overview**: 9P is a **request-response file access protocol**. Each message has a 4-byte size, 1-byte type (Tversion/Rversion, Tauth/Rauth, Tattach/Rattach, Twalk/Rwalk, Topen/Ropen, Tread/Rread, Twrite/Rwrite, Tclunk/Rclunk, etc.), and a 2-byte **tag** (request/response correlation ID). The tag field enables **multiplexed concurrent requests** — a client can send multiple Tread/Twrite requests with different tags without waiting for responses. File IDs (fids) identify open files. 9P2000.L extends the protocol with Linux-specific operations.

**Sequential-write streaming**: ✅ Messages are self-contained (size + type + tag + payload) — written forward-only.

**Sequential-read streaming**: ✅ Messages parsed sequentially.

**Chunk interleaving (implementations)**: ✅ Native — 9P clients multiplex file operations using tags. Linux's `v9fs` issues concurrent reads/writes on different files using different tags over a single transport connection.

**Chunk interleaving (theoretical)**: ✅ Core design — tags enable concurrent access to multiple files over a single connection. The protocol explicitly supports multiple outstanding requests.

**Conclusion for multi-stream use**: 9P is a clean multiplexed file access protocol with real adoption (Linux kernel, WSL2, QEMU). Its tag-based request/response multiplexing maps to multi-stream pipe use conceptually, but 9P is a **file operations protocol** (walk, open, read, write, close), not a stream framing format. Using 9P for pure data multiplexing would mean implementing the full file-access state machine. More relevant as prior art (tags, fids) than as a direct candidate.

---

#### Cap'n Proto RPC

**Popularity**: ★★☆☆☆ — created by Kenton Varda (former Google protobuf lead) in 2013. Used by Cloudflare Workers, Sandstorm.io, and some infrastructure projects. Implementations in C++, Rust, Go, Java, Python. Less widely deployed than protobuf/gRPC but technically notable.

**Format overview**: Cap'n Proto defines both a **serialization format** (zero-copy, flat memory layout) and an **RPC protocol**. The RPC protocol uses a message-based format over a stream transport (TCP, UNIX socket, pipe). Each message has a segment table + segments. The RPC layer adds **question IDs** for request/response correlation and supports **pipelining** (sending follow-up requests before receiving the response to the first). Messages are framed with a segment count and segment sizes, then the raw segment data.

**Sequential-write streaming**: ✅ Messages are self-contained and written forward-only.

**Sequential-read streaming**: ✅ Messages parsed by reading segment table then segments.

**Chunk interleaving (implementations)**: ✅ The RPC protocol multiplexes multiple method calls and responses using question IDs. Multiple concurrent RPCs are interleaved at the message level.

**Chunk interleaving (theoretical)**: ✅ Question-based multiplexing is a core RPC feature.

**Conclusion for multi-stream use**: Cap'n Proto's message framing (segment table + segments) is interesting as a zero-copy-friendly format, and the RPC layer's question-based multiplexing is relevant prior art. However, it's an RPC framework, not a stream multiplexing container — using it for pure data streaming would require implementing the RPC protocol. The zero-copy design is notable for high-throughput pipe use.

---

#### CBOR Sequences (RFC 8742)

**Popularity**: ★★★☆☆ — CBOR (Concise Binary Object Representation, RFC 8949) is increasingly used in IoT (CoAP, COSE), WebAuthn/FIDO2, and CDDL-based specifications. CBOR Sequences (RFC 8742, 2020) extend CBOR to support streaming by defining a sequence of concatenated CBOR data items without a wrapping container.

**Format overview**: A CBOR sequence is simply a concatenation of CBOR-encoded data items. Each item is self-delimiting (the type byte encodes the item type and length information — short lengths inline, longer lengths as 1/2/4/8-byte extensions). No framing header, no container structure — just one item after another. For multi-stream use, each item could be a CBOR map or array containing a stream ID and payload.

**Sequential-write streaming**: ✅ Fully supported — each item is self-contained and self-delimiting.

**Sequential-read streaming**: ✅ Fully supported — a parser reads items sequentially.

**Chunk interleaving (implementations)**: ❌ No standard mechanism. Multi-stream would require a convention (e.g., each item is `[stream_id, payload_bytes]`).

**Chunk interleaving (theoretical)**: ⚠️ Possible with application-level convention (tagging each item with a stream ID), but CBOR sequences themselves have no built-in multiplexing.

**Conclusion for multi-stream use**: CBOR sequences are a clean, standardized (IETF) binary streaming format. The self-delimiting encoding eliminates the need for explicit length framing. However, multi-stream multiplexing requires application-level conventions (custom CBOR structure per item). CBOR is more relevant as a serialization choice within a multiplexing container than as the multiplexer itself. The IETF standardization and growing IoT/security ecosystem adoption are positives.

---

#### NDJSON / JSON Lines (Newline-Delimited JSON)

**Popularity**: ★★★★☆ — NDJSON (also known as JSON Lines, JSONL, or JSON-seq) is the de-facto standard for streaming structured data between CLI tools. Used by Elasticsearch bulk API, Apache Spark, BigQuery, jq, ndjson-cli, and countless data pipelines. The convention emerged organically around 2013; formalized by jsonlines.org and partially by RFC 7464 (JSON Text Sequences, 2015).

**Format overview**: Each line is a complete, valid JSON value followed by a newline (`\n`). No framing header, no container structure, no length prefix — just concatenated JSON objects separated by newlines. This makes it trivially producible by `echo` and parseable by line-oriented tools (`grep`, `awk`, `jq`).

**Sequential-write streaming**: ✅ Fully supported — each line is self-contained. Writers flush one JSON object per line.

**Sequential-read streaming**: ✅ Fully supported — readers process one line at a time.

**Chunk interleaving (implementations)**: ❌ No standard mechanism. All existing tools assume a homogeneous stream of objects.

**Chunk interleaving (theoretical)**: ⚠️ Possible with a convention (e.g., each object contains a `"stream"` field), but NDJSON itself has no multiplexing concept.

**Conclusion for multi-stream use**: NDJSON is the most widely deployed text-based streaming format for structured data. Its simplicity is both its strength (zero overhead, universal tooling) and its limitation (no framing, no multiplexing, no tombstone). For multi-stream use, each line could include a stream identifier field, but this is a convention on top of NDJSON, not a feature of it. NDJSON is relevant as a **payload format** within a multiplexing container, not as the multiplexer itself.

---

#### MessagePack

**Popularity**: ★★★★☆ — MessagePack is a widely deployed binary serialization format ("like JSON but fast and small"). Used by Redis (RESP3 interop), Fluentd/Fluent Bit, Jupyter kernels (ZMQ+msgpack), Neovim RPC, and numerous APIs. Spec published 2008, implementations in 50+ languages.

**Format overview**: MessagePack encodes data in a self-delimiting binary format where each value's type and length are encoded in the first byte(s). Like CBOR, a stream of MessagePack values can be concatenated without a container — each value is self-describing. Binary maps, arrays, strings, integers, and raw bytes are all supported.

**Sequential-write streaming**: ✅ Fully supported — each value is self-contained.

**Sequential-read streaming**: ✅ Fully supported — a parser reads values sequentially.

**Chunk interleaving (implementations)**: ❌ No standard mechanism. Multi-stream would require a convention (e.g., each message is `[stream_id, payload_bytes]`).

**Chunk interleaving (theoretical)**: ⚠️ Possible with an envelope convention, but MessagePack itself has no multiplexing concept.

**Conclusion for multi-stream use**: MessagePack is a popular binary alternative to JSON with self-delimiting encoding. Like CBOR and Protobuf, it's a serialization format rather than a multiplexing container. Multi-stream use requires an application-level envelope. MessagePack is more relevant as a payload encoding within a multiplexing framing layer.

---

#### Apache Parquet

**Popularity**: ★★★★★ — Apache Parquet is the dominant columnar storage format for big data analytics. Used by Apache Spark, Apache Arrow, Pandas, DuckDB, Snowflake, BigQuery, Athena, and virtually every modern data warehouse. Created in 2013 by Twitter and Cloudera, now an Apache top-level project.

**Format overview**: Parquet is a **footer-based** columnar format. Data is organized into row groups containing column chunks, with all metadata (schema, row group locations, column statistics, min/max values) stored in a **footer at the end of the file**. A 4-byte magic number (`PAR1`) appears at both the beginning and end. The reader must seek to the end to find the footer before it can interpret any data.

**Sequential-write streaming**: ❌ Not supported — the footer containing all metadata must be written **after** all data. While row groups can be written sequentially, the file is not valid until the footer is appended. A writer must either buffer or seek back.

**Sequential-read streaming**: ❌ Not supported — the reader must read the footer (at end of file) first to know the schema, row group locations, and column chunk offsets. Forward-only reading is impossible.

**Chunk interleaving (implementations)**: ❌ None — Parquet is a single-schema columnar format; no concept of multiple independent streams.

**Chunk interleaving (theoretical)**: ❌ Not feasible — the format is fundamentally random-access by design.

**Conclusion for multi-stream use**: Parquet is a complete non-starter for streaming pipe use. Its footer-based design requires the entire file to exist before it can be read. Included because its extreme popularity (★★★★★) means users will ask "why not Parquet?" — and the answer is that columnar analytics formats are the opposite of streaming formats by design. Parquet is optimized for reading subsets of columns from stored files, not for producing data incrementally.

---

#### gRPC (Google Remote Procedure Call)

**Popularity**: ★★★★★ — gRPC is Google's open-source RPC framework, universally deployed for microservice communication. Used by Google Cloud, Kubernetes, Envoy, Buf, and thousands of production systems. Created in 2015, based on HTTP/2 + Protocol Buffers.

**Format overview**: gRPC runs on top of **HTTP/2** and inherits its binary framing and stream multiplexing. Each RPC call maps to an HTTP/2 stream. gRPC adds its own 5-byte length-prefixed message framing on top of HTTP/2 DATA frames: 1 byte compressed flag + 4 bytes message length + serialized protobuf message. Supports unary, server-streaming, client-streaming, and bidirectional streaming RPCs.

**Sequential-write streaming**: ✅ Fully supported — gRPC streaming RPCs produce messages incrementally.

**Sequential-read streaming**: ✅ Fully supported — messages arrive in order per stream.

**Chunk interleaving (implementations)**: ✅ Native — inherits HTTP/2's stream multiplexing. Multiple concurrent RPCs on the same connection are interleaved at the HTTP/2 frame level.

**Chunk interleaving (theoretical)**: ✅ Yes — gRPC explicitly supports multiple concurrent streams per connection.

**Conclusion for multi-stream use**: gRPC is the most widely deployed multiplexing RPC framework, but it inherits **all of HTTP/2's complexity** (HPACK compression, flow control, SETTINGS negotiation) plus its own protobuf schema requirement. Using gRPC for pipe multiplexing would be like using HTTP/2 directly but even heavier — requiring a full gRPC runtime (grpc-core is ~1M lines of C++). gRPC is important as **prior art** confirming that HTTP/2-based multiplexing works at scale, but it's impractical for lightweight pipe use.

---

#### AMQP 1.0 (Advanced Message Queuing Protocol)

**Popularity**: ★★★☆☆ — AMQP 1.0 is an ISO/IEC 19464 standardized messaging protocol used in enterprise messaging systems. Implemented by Apache Qpid, Azure Service Bus, Amazon MQ, and Red Hat AMQ. Note: AMQP 0-9-1 (used by RabbitMQ) is a different protocol; AMQP 1.0 (2012) is the OASIS/ISO standard.

**Format overview**: AMQP 1.0 is a **binary** peer-to-peer protocol with connection → session → link multiplexing. A single TCP connection carries multiple sessions, each session carries multiple links (unidirectional message channels), and each link carries messages. Message framing uses type-length-value with AMQP-specific type codes. The protocol includes flow control, delivery acknowledgments, and transaction support.

**Sequential-write streaming**: ✅ Fully supported — messages are sent incrementally over links.

**Sequential-read streaming**: ✅ Fully supported — messages arrive in order per link.

**Chunk interleaving (implementations)**: ✅ Native — multiple links on a session carry independent message streams, interleaved at the frame level. Sessions themselves can be multiplexed on a connection.

**Chunk interleaving (theoretical)**: ✅ Yes — the connection/session/link hierarchy is explicitly designed for multiplexing.

**Conclusion for multi-stream use**: AMQP 1.0 has the right multiplexing architecture (connection → session → link), but the protocol is **extremely heavyweight** for pipe use — it includes SASL authentication, flow control with link credit, delivery settlement (at-least-once/at-most-once/exactly-once), and a complex type system. The protocol requires a full state machine implementation. Valuable as prior art for hierarchical multiplexing design, but impractical for lightweight pipe framing.

---

#### MQTT 5.0 (Message Queuing Telemetry Transport)

**Popularity**: ★★★★☆ — MQTT is the dominant IoT messaging protocol, used by AWS IoT Core, Azure IoT Hub, Eclipse Mosquitto, HiveMQ, and billions of connected devices. MQTT 3.1.1 (2014) is an OASIS standard; MQTT 5.0 (2019) adds significant features including topic aliases, shared subscriptions, and user properties.

**Format overview**: MQTT is a **binary** publish-subscribe protocol. Clients publish messages to **topics** (UTF-8 string hierarchies like `sensors/temperature/room1`) and subscribe to topic filters with wildcards. Messages are routed through a broker. The wire format uses a 2+ byte fixed header (packet type + flags + remaining length as variable-byte integer) followed by a variable header and payload.

**Sequential-write streaming**: ✅ Fully supported — PUBLISH packets are sent incrementally.

**Sequential-read streaming**: ✅ Fully supported — packets arrive sequentially.

**Chunk interleaving (implementations)**: ⚠️ Via topic-based routing — multiple topics on the same connection carry different data streams. However, MQTT's pub/sub model requires a broker for routing; direct peer-to-peer multiplexing is not the intended use case.

**Chunk interleaving (theoretical)**: ⚠️ Topic-based multiplexing exists but is broker-mediated, not wire-level stream multiplexing.

**Conclusion for multi-stream use**: MQTT's topic-based naming (`sensors/temperature/room1`) is actually close to the file-name-per-stream requirement — topics are human-readable UTF-8 strings. However, MQTT is a **broker-mediated pub/sub protocol**, not a point-to-point framing format. Using MQTT for pipe multiplexing would require running a broker process or reimplementing the protocol as peer-to-peer. MQTT 5.0's user properties could carry metadata, and topic strings provide native naming. But the broker dependency and QoS overhead make it impractical for direct pipe use. Notable as the only protocol where "stream names" (topics) are first-class wire-level concepts.

---

#### Custom / Generic Framing Formats

When no existing format is suitable, a lightweight framing protocol can be designed. Several well-known examples exist:

| Framing | Description | Streaming | Interleaving |
|---|---|---|---|
| **Netstring** | `len:data,` — trivially parseable | ✅ | ✅ (with stream tag in data) |
| **MIME multipart** | `--boundary\r\nContent-*\r\n\r\ndata` | ✅ | ❌ (parts are sequential, not interleaved) |
| **HTTP/1.1 chunked** | hex-length + `;stream=N` + `\r\n` + data + `\r\n` | ✅ | ✅ (via chunk extensions — works over pipes; not over HTTP infra) |
| **HTTP/2 framing** | 9-byte frame header with stream ID | ✅ | ✅ Native |
| **MessagePack** | Self-delimiting binary encoding | ✅ | ✅ (with envelope) |
| **CBOR sequences** (RFC 8949 / RFC 8742) | Self-delimiting binary encoding | ✅ | ✅ (with envelope) |
| **Protocol Buffers** delimited | Length-prefixed PB messages | ✅ | ✅ (with stream field) |
| **LTV (Length-Tag-Value)** | Minimal custom framing | ✅ | ✅ Native |
| **NDJSON** | Newline-delimited JSON | ✅ | ✅ (with stream field) |

A custom LTV-style framing is the simplest possible approach:
```
[4-byte stream_id][4-byte payload_len][payload_len bytes of payload]
```
This allows a reader to demultiplex any number of streams in a single forward pass with O(1) memory overhead.

---

## Stream Finalization / Tombstone Markers

A critical requirement for any multi-stream pipe format is the ability to **distinguish a properly finalized stream from one that was truncated** (e.g., due to writer crash, `kill -9`, broken pipe, or I/O error). Without an explicit end-of-stream marker ("tombstone"), the reader cannot tell whether the stream ended cleanly or was interrupted mid-data.

### Why this matters

When a multiplexed stream is piped between processes, the reader must be able to detect:
1. **Clean completion**: all logical streams ended normally, the container is valid.
2. **Truncation**: the writer died or the pipe broke mid-write — the stream is incomplete.
3. **Per-stream completion**: in an interleaved multi-stream format, individual logical streams may finish at different times. The reader needs per-stream end markers to know when each sub-stream is complete.

Without tombstones, a reader that simply hits EOF cannot distinguish "the writer finished writing all data" from "the writer crashed after writing 60% of the data." This is especially important in pipelines where errors should propagate cleanly.

### Format-by-format finalization analysis

| Format | End-of-stream marker | Truncation detectable? | Per-stream end marker? |
|---|---|---|---|
| **TAR** | Two 512-byte all-zero blocks | ✅ Yes — missing zero blocks = truncated | ❌ N/A (single-stream) |
| **CPIO** | `TRAILER!!!` filename entry | ✅ Yes — missing trailer = truncated | ❌ N/A (single-stream) |
| **ar** | None (implicit EOF) | ❌ No — truncation indistinguishable from valid end¹ | ❌ N/A |
| **ZIP** | End of Central Directory record (EOCD) | ✅ Yes — missing EOCD = truncated | ❌ N/A |
| **7-Zip** | EndHeader at end | ✅ Yes — missing/invalid EndHeader = truncated | ❌ N/A |
| **RAR** | End-of-archive block (HEAD_ENDER) | ✅ Yes — missing ENDER = truncated | ❌ N/A |
| **ISO 9660** | Volume Descriptor Set Terminator | ✅ Yes — but requires valid descriptor set at start | ❌ N/A |
| **WIM** | Integrity table (optional) + complete header | ⚠️ Header checksum validates; but no explicit end marker | ❌ N/A |
| **CAB** | Header contains total cabinet size | ✅ Yes — actual size < declared size = truncated | ❌ N/A |
| **Ogg** | Page with EOS flag (0x04) per stream | ✅ Yes — missing EOS page = truncated | ✅ Yes — EOS flag per serial number |
| **Matroska/MKV** | None (implicit EOF or Segment size match) | ⚠️ Unknown-size segments have no end marker; CRC in some elements helps | ⚠️ No per-track end signal |
| **MPEG-TS** | None (continuous stream) | ❌ No² — designed for broadcast; truncation is normal operation | ❌ No per-PID end signal |
| **MPEG-PS** | MPEG_program_end_code (0x000001B9) | ✅ Yes — missing end code = truncated | ❌ No per-stream end signal |
| **ASF** | Simple Index Object (optional) + declared packet count | ⚠️ Packet count in header; but streaming mode uses sentinel values | ❌ No per-stream end |
| **CAF** | None (implicit EOF; chunk sizes guide reading) | ⚠️ If chunk size is -1 (streaming), truncation is ambiguous | ❌ N/A (audio-specific) |
| **HTTP/2 framing** | GOAWAY frame (connection) + END_STREAM flag (per-stream) | ✅ Yes — missing GOAWAY = unclean disconnect | ✅ Yes — END_STREAM flag per stream ID |
| **SSH channels** | SSH_MSG_CHANNEL_CLOSE per channel + SSH_MSG_DISCONNECT | ✅ Yes — missing close = unclean disconnect | ✅ Yes — CHANNEL_CLOSE per channel |
| **HTTP/1.1 chunked** | Zero-length chunk (`0\r\n\r\n`) + optional trailers | ✅ Yes — missing zero-chunk = truncated | ❌ N/A (single-stream) |
| **MIME multipart** | Closing boundary (`--boundary--`) | ✅ Yes — missing close boundary = truncated | ❌ Parts are sequential |
| **MP4/ISOBMFF** | None standard (moov/mfra optional) | ⚠️ Regular MP4 truncation detectable by incomplete moov; fMP4 has no mandatory end | ⚠️ No per-track end signal |
| **AVI/RIFF** | None (implicit EOF; RIFF size field) | ⚠️ RIFF header size mismatch = truncated; but optional idx1 absence is ambiguous | ❌ No per-stream end signal |
| **IFF** | None (implicit EOF; FORM size field) | ⚠️ FORM size mismatch = truncated | ❌ N/A |
| **FLV** | None (implicit EOF) | ⚠️ Incomplete last tag detectable (PreviousTagSize mismatch) | ❌ No per-stream end signal |
| **WARC** | None (implicit EOF) | ⚠️ Incomplete last record detectable (Content-Length mismatch) | ❌ N/A (single-stream) |
| **XAR** | None (TOC is upfront; heap length implied) | ⚠️ TOC-declared sizes vs actual heap = detectable | ❌ N/A |
| **NUT** | EOR (End of Relevance) frame per stream | ✅ Yes — missing EOR = truncated | ✅ Yes — per stream ID |
| **Avro OCF** | None (implicit EOF; sync markers per block) | ⚠️ Incomplete block (missing sync marker) = truncated | ❌ N/A (single-schema) |
| **Protobuf delimited** | None (implicit EOF) | ⚠️ Incomplete varint or short payload = truncated | ❌ N/A (convention-dependent) |
| **QUIC** | FIN bit per stream + CONNECTION_CLOSE | ✅ Yes — missing FIN = incomplete stream | ✅ Yes — FIN bit per stream ID |
| **LHA/LZH** | Zero-size header sentinel | ✅ Yes — missing sentinel = truncated | ❌ N/A (single-stream) |
| **SCTP** | SHUTDOWN/ABORT chunks | ✅ Yes — missing SHUTDOWN = unclean | ❌ No per-stream end signal |
| **WebSocket** | Close frame (opcode 0x8) | ✅ Yes — missing close = truncated | ❌ N/A (single-stream) |
| **D-Bus** | None (message-based; no explicit end) | ⚠️ Incomplete message header = truncated | ❌ N/A |
| **9P** | Tclunk per fid (not per stream) | ⚠️ Protocol-level; not stream-level | ⚠️ Per-fid only |
| **Cap'n Proto RPC** | Finish message per question | ✅ Yes — per-question completion | ⚠️ Per-question, not per-stream |
| **CBOR sequences** | None (implicit EOF) | ⚠️ Incomplete item detectable | ❌ N/A |
| **NDJSON** | None (implicit EOF) | ⚠️ Incomplete line/JSON detectable | ❌ N/A |
| **MessagePack** | None (implicit EOF) | ⚠️ Incomplete value detectable | ❌ N/A |
| **Apache Parquet** | Footer magic (`PAR1`) at end | ✅ Missing footer = truncated | ❌ N/A (not streaming) |
| **gRPC** | END_STREAM (via HTTP/2) + Trailers | ✅ Per-stream via HTTP/2 | ✅ Yes — inherits HTTP/2 END_STREAM |
| **AMQP 1.0** | Detach (per link) + Close (per session/connection) | ✅ Yes — missing close = unclean | ✅ Yes — Detach per link |
| **MQTT 5.0** | DISCONNECT packet | ✅ Yes — missing DISCONNECT = unclean | ❌ No per-topic end signal |
| **Custom LTV** | Depends on design — typically a zero-length sentinel or explicit END frame | Designer's choice — **should** include an end marker | Designer's choice |

¹ In `ar`, the reader knows each member's size from its header, so truncation *within* a member is detectable (fewer bytes than declared). But truncation *between* members is indistinguishable from a valid archive with fewer members.

² MPEG-TS is designed for broadcast where the stream may be joined or left at any point. There is no concept of "complete" — this is a feature for broadcast but a problem for pipeline use.

### Best-in-class: formats with per-stream tombstones

For advanced multi-stream pipe use where individual streams may finish independently, **per-stream finalization** provides the strongest guarantees — the reader knows when each individual logical stream is complete, not just the overall container. Eight formats provide this natively (though a container-level tombstone is sufficient for most use cases):

1. **Ogg** — each logical bitstream has an explicit **EOS (End of Stream) flag** in the last page's header for that stream. A reader can detect per-stream completion and distinguish it from truncation. The Ogg page CRC-32 also provides integrity checking for each page.

2. **HTTP/2 framing** — each stream can be terminated with a frame carrying the **END_STREAM** flag (bit 0). A GOAWAY frame signals connection-level shutdown. Together, these provide both per-stream and connection-level finalization.

3. **SSH channels** — **SSH_MSG_CHANNEL_CLOSE** explicitly terminates each channel. **SSH_MSG_DISCONNECT** terminates the connection.

4. **NUT** — provides an **EOR (End of Relevance)** frame per stream, explicitly marking when a stream has no more data. Combined with startcode-based sync, this enables clean per-stream finalization.

5. **QUIC** — the **FIN bit** on STREAM frames explicitly marks the end of each stream. **CONNECTION_CLOSE** terminates the entire connection. Per-stream finalization is a core protocol feature.

6. **SCTP** — while SCTP does not have per-stream end signals (SHUTDOWN terminates the entire association), its independent stream model means a higher-level protocol can implement per-stream finalization. Included for completeness alongside other transport protocols.

7. **gRPC** — inherits HTTP/2's **END_STREAM** flag for per-RPC finalization. gRPC trailers carry status codes and error messages, providing richer per-stream completion semantics than raw HTTP/2.

8. **AMQP 1.0** — **Detach** performative explicitly closes individual links (message streams). **Close** terminates sessions and connections. The connection/session/link hierarchy provides per-stream finalization at multiple granularities.

Formats like TAR (two zero blocks), CPIO (`TRAILER!!!`), and HTTP/1.1 chunked (zero-length chunk) have *container-level* end markers but no per-stream finalization — because they don't support multiple concurrent streams.

### Implication for format choice

At minimum, a multi-stream pipe format **must** provide a **container-level tombstone** — a marker that lets the reader distinguish "the writer finished cleanly" from "the writer crashed mid-stream." Formats like TAR (two zero blocks), CPIO (`TRAILER!!!`), and HTTP/1.1 chunked (zero-length chunk) already satisfy this requirement. A single container-level end marker is sufficient for most pipe use cases: if the tombstone is present, the reader knows all streams completed; if it's missing, the reader knows the writer was interrupted.

Per-stream tombstones (Ogg EOS, HTTP/2 END_STREAM, NUT EOR, QUIC FIN) are a **nice-to-have** for advanced scenarios (e.g., one stream finishing early while others continue), but are not strictly necessary if the container-level marker covers the "did the whole pipeline succeed?" question.

This means MPEG-TS and FLV (no end markers at all) remain ruled out for clean termination detection. But formats with container-level markers — including TAR — satisfy the tombstone requirement. A custom LTV format **should** include at least a container-level end marker (e.g., a zero-length sentinel or a dedicated END frame type).

---

## Higher Pipes (FD 3, 4, 5 …) — Can They Actually Work?

UNIX processes inherit file descriptors from their parent. In principle, any number of FDs can be used for communication between processes connected via `pipe(2)` system calls. **Higher FDs are fully functional at the OS level and ARE used successfully by real-world tools** — the question is whether they scale to a general-purpose multi-stream piping solution.

### OS / Kernel Level

At the OS level, higher file descriptors are fully supported by all POSIX systems. There is no inherent limit preventing FD 3, 4, 5, … from being used as pipes. The `pipe(2)` system call returns two FDs (read end and write end) that can be any available integer. The `RLIMIT_NOFILE` resource limit controls the maximum number of open FDs per process (typically 1024 soft, 65536 hard on Linux), not the specific numbers.

### Real-world tools that use higher FDs successfully

Higher FDs are not theoretical — several well-known tools use them in production:

| Tool | Flag | FD usage | Purpose |
|---|---|---|---|
| **GPG** | `--status-fd N` | Machine-readable status on FD N | Separate signing/encryption status from data output |
| **Bubblewrap** (Flatpak) | `--json-status-fd N` | JSON container status on FD N | Monitor sandbox startup while capturing container output |
| **apt-get** | `-o APT::Status-Fd=N` | Progress reporting on FD N | Machine-parseable progress separate from log output |
| **GnuPG pinentry** | Built-in | FD 3 for passphrase communication | Secure passphrase channel separate from user I/O |
| **ksh coproc** | `|&` / `print -p` | FD 3 and 4 by convention | Bidirectional IPC with coprocess |

These tools prove that higher FDs **work well for a specific pattern**: a single producer tool writing a secondary stream (status, progress, metadata) alongside its primary stdout output, consumed by a single known caller.

### Working examples (bash)

```bash
# ✅ WORKS: Single tool with status FD (the GPG pattern)
gpg --decrypt --status-fd 3 file.gpg 3>status.txt > decrypted.dat

# ✅ WORKS: Process substitution — FD 3 piped to a processor
exec 3> >(jq '.status' > progress.json)
my_tool --status-fd 3
exec 3>&-

# ✅ WORKS: Two streams from one producer via process substitution
my_tool > >(handle_data) 2> >(handle_errors) 3> >(handle_metadata)

# ✅ WORKS: Named pipe for FD 3 between two commands
mkfifo /tmp/fd3_pipe
producer --status-fd 3 3>/tmp/fd3_pipe &
consumer < /tmp/fd3_pipe
rm /tmp/fd3_pipe

# ✅ WORKS: coproc for bidirectional communication (bash 4.0+)
coproc DB { sqlite3 mydb.sqlite; }
echo "SELECT count(*) FROM users;" >&${DB[1]}
read count <&${DB[0]}
```

### Where higher FDs break down

#### Problem 1: No `n|` pipe syntax exists

The `|` operator is **hardcoded** to connect FD 1 (stdout) → FD 0 (stdin). No shell supports `cmd1 3| cmd2`:

```bash
# ❌ DOES NOT WORK in any shell:
producer --data-fd 1 --status-fd 3  3|  status_consumer

# ✅ Workaround (verbose):
mkfifo /tmp/status_pipe
producer --status-fd 3 3>/tmp/status_pipe | data_consumer &
status_consumer < /tmp/status_pipe
```

#### Problem 2: Multi-stage pipelines don't compose

The `|` operator only connects one FD pair per stage. A pipeline `stage1 | stage2 | stage3` where each stage produces multiple output streams requires exponentially complex plumbing:

```bash
# Goal: stage1 produces data (FD 1) + metadata (FD 3)
#        stage2 consumes both, produces data (FD 1) + metadata (FD 3)
#        stage3 consumes both

# ❌ DOES NOT WORK:
stage1 | stage2 | stage3
# stage2 only receives stage1's stdout; FD 3 is lost

# ✅ Workaround (fragile, 5 lines instead of 1):
mkfifo /tmp/s1_meta /tmp/s2_meta
stage1 3>/tmp/s1_meta | stage2 3</tmp/s1_meta 3>/tmp/s2_meta | stage3 3</tmp/s2_meta
rm /tmp/s1_meta /tmp/s2_meta

# ✅ With a container format (clean, composable):
stage1_mux | stage2_demux_remux | stage3_demux
```

The named-pipe workaround works for **2-3 stages** but becomes unwieldy for longer pipelines or when the number of streams varies per stage. Every stage must know exactly which FDs to expect from its predecessor.

#### Problem 3: Tool discoverability

With `|`, tools universally know to read stdin and write stdout — this convention is so strong that tools work together without coordination. Higher FDs have no such convention:

```bash
# Standard pipe: any tool that reads stdin works
producer | consumer          # consumer reads FD 0 — universal

# Higher FDs: consumer must be explicitly told
producer --metadata-fd 3 3>/tmp/meta | consumer
meta_consumer < /tmp/meta   # must know to look for metadata
```

There is no established convention for "FD 3 = metadata" or "FD 4 = progress." Each tool invents its own flag (`--status-fd`, `--json-status-fd`, `-o APT::Status-Fd=`), and the calling script must wire them up explicitly.

#### Problem 4: Cross-platform and cross-shell portability

| Capability | bash | zsh | ksh | fish | dash/sh | Nushell | Windows cmd | PowerShell |
|---|---|---|---|---|---|---|---|---|
| `n>file` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `n>&m` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `>(cmd)` process subst. | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `coproc` | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Named pipes (FIFO) | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ❌ | ❌ |

- **Nushell** deliberately omits higher-FD support — cross-platform design (Windows uses HANDLEs, not integer FDs). See [nushell/nushell#15650](https://github.com/nushell/nushell/issues/15650).
- **Windows FD 3+ support** — a layered picture:
  - **Kernel/Win32 API**: Windows uses **HANDLEs** (opaque pointers), not integer FDs. I/O is done via `CreateFile()`, `ReadFile()`, `WriteFile()` etc. There are no FDs 0, 1, 2 — instead, `GetStdHandle(STD_INPUT_HANDLE)` / `STD_OUTPUT_HANDLE` / `STD_ERROR_HANDLE` return the three standard handles.
  - **C Runtime (CRT)**: The MSVC/UCRT provides a POSIX-like FD layer via `_open()`, `_read()`, `_write()`, `_pipe()`, `_dup2()`. These functions maintain an internal table mapping integer FDs (0, 1, 2, 3, …) to underlying HANDLEs. So **C/C++ code CAN use FD 3+ on Windows** through CRT functions — `_pipe()` creates a pair of FDs, `_dup2()` can assign them to specific numbers. However, this is a CRT-internal abstraction, not a system-wide concept.
  - **Child process inheritance**: `CreateProcess()` passes exactly 3 handles to the child via `STARTUPINFO` (`hStdInput`, `hStdOutput`, `hStdError`). There is no built-in mechanism for "FD 3." Since Vista, `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` can pass arbitrary extra HANDLEs, but the child must know which HANDLEs to use (typically communicated via environment variables or command-line arguments with the numeric HANDLE value — not integer FDs).
  - **Shell level**: `cmd.exe` supports only `<`, `>`, `>>`, `2>`, `2>&1` — no arbitrary FD redirection. PowerShell uses .NET streams, not integer FDs. There is **no shell-level way to redirect FD 3+** on Windows.
  - **Bottom line**: Windows has FD 3+ only as a CRT-internal abstraction. The GPG/bubblewrap `--status-fd 3` pattern requires either a POSIX emulation layer (MSYS2, Cygwin, WSL) or custom C code using `_pipe()` + `_dup2()` in the parent process. There is no Windows shell equivalent of `cmd 3>/dev/fd/3`.
- **Process substitution** (`>(cmd)`) only works in bash, zsh, and ksh — not in POSIX sh, fish, or Nushell.

#### Higher-FD redirection syntax: which FD numbers actually work?

POSIX (IEEE 1003.1) specifies that the `[n]>word` redirection syntax takes a **single digit** for `n` — meaning only FDs 0–9 are guaranteed portable. Shells like bash, zsh, and ksh **extend** this to support multi-digit FD numbers (15, 99, etc.), but POSIX sh / dash do not. This is a critical portability constraint when using FDs above 9.

Tested behavior for `cmd 7>file` (single-digit FD redirect) vs `cmd 15>>file` (multi-digit FD redirect):

| Syntax | bash | zsh | ksh93 | dash / POSIX sh | fish | Nushell | Windows cmd | PowerShell |
|---|---|---|---|---|---|---|---|---|
| `7>file` | ✅ FD 7 | ✅ FD 7 | ✅ FD 7 | ✅ FD 7 | ❌ | ❌ | ❌ | ❌ |
| `7>>file` | ✅ FD 7 | ✅ FD 7 | ✅ FD 7 | ✅ FD 7 | ❌ | ❌ | ❌ | ❌ |
| `15>file` | ✅ FD 15 | ✅ FD 15 | ✅ FD 15 | ❌ `15` = arg | ❌ | ❌ | ❌ | ❌ |
| `15>>file` | ✅ FD 15 | ✅ FD 15 | ✅ FD 15 | ❌ `15` = arg | ❌ | ❌ | ❌ | ❌ |
| `99>file` | ✅ FD 99 | ✅ FD 99 | ✅ FD 99 | ❌ `99` = arg | ❌ | ❌ | ❌ | ❌ |
| `7>&1` (dup) | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| `15>&1` (dup) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |

Key findings:
- **bash, zsh, ksh93**: Support arbitrary multi-digit FD numbers in redirections. `15>file`, `99>>file` all work as FD redirects.
- **dash / POSIX sh**: Only supports single-digit FDs (0–9) per POSIX spec. `15>file` is parsed as `echo 15` with stdout redirected to `file` — the `15` becomes a command argument, not a FD number. This is a **silent misparse**, not an error.
- **fish**: No `n>file` syntax for arbitrary FDs. Fish uses `2>` for stderr but does not support general FD redirection.
- **Nushell, Windows cmd, PowerShell**: No FD concept at the shell level.

**Implication**: Using FDs ≥ 10 is limited to bash/zsh/ksh scripts. Scripts using `15>` or `99>` will **silently break** in dash/sh (the number becomes a command argument). FDs 3–9 are portable across all POSIX shells.

### Verdict: When to use higher FDs vs container formats

| Criterion | Higher FDs (3+) | Container format over stdout |
|---|---|---|
| **Works for** | 1 producer + 1–2 side streams | N producers, N streams, arbitrary topology |
| **Shell support** | bash/zsh/ksh (3 of 6) | All shells, all platforms |
| **Composability** | Poor — breaks `cmd1 \| cmd2 \| cmd3` | Natural — standard pipe chains work |
| **Convention** | No standard; each tool invents flags | Self-describing format in the stream |
| **Tool discoverability** | Consumer must know about each FD | Consumer parses the format — one protocol |
| **Cross-platform** | POSIX only (not Windows/Nushell) | Universal |
| **Overhead** | Zero (kernel pipe, no framing) | 0.01–12.5% depending on format |
| **Latency** | Zero (direct kernel pipe) | Minimal (chunk-level framing) |
| **Setup complexity** | Low for 1 FD, high for N FDs | Constant regardless of stream count |
| **Debugging** | `strace -f` / `lsof` to trace FDs | Inspect stream with format-aware tools |

**Bottom line**: Higher FDs **are a legitimate solution** for the pattern "one tool, one side channel" (GPG, bubblewrap, apt). They have **zero overhead** and **zero latency** — when they work, they're optimal. But they **don't scale** to general-purpose multi-stream pipelines because:

1. No shell supports `n|` syntax — composable multi-stage pipelines require named pipes or process substitution
2. No convention exists for FD assignment — every tool invents its own flags
3. Portability is limited to POSIX shells on POSIX systems (excludes Nushell, Windows, fish for advanced features)
4. The number of streams must be known at pipeline setup time — dynamic stream creation is impossible

For **general-purpose multi-stream piping** (arbitrary number of named streams, composable across pipeline stages, portable across shells and platforms), **multiplexing into a single stdout stream via a container format** remains the more practical approach. Higher FDs and container formats are complementary, not competing — use FDs for known side channels, container formats for general multiplexing.

---

## Conclusions and Recommendations

### Which established format is the best fit?

The ideal candidate would be a format as popular and well-established as ZIP or TAR, but with native support for both streaming write (no patching) and multi-stream interleaving. Unfortunately, **no archive format with ZIP/TAR-level ubiquity supports interleaving** — this is the core gap:

- **TAR** (1979, ★★★★★) — streams perfectly but has zero interleaving support.
- **ZIP** (1989, ★★★★★) — cannot even be read in a forward-only pass (Central Directory at end), let alone interleave.
- **7-Zip** (1999, ★★★★☆) — requires patching the start header; no streaming at all.

### General-purpose (non-multimedia) candidates

The strongest requirement is a format that is **not multimedia-specific** — one designed for arbitrary data streams, not audio/video tracks. Sorted by suitability:

| Rank | Format | Year | Popularity | Wire format | General-purpose? | Notes |
|---|---|---|---|---|---|---|
| 1 | **Ogg** | 2003 | ★★★☆☆ | Binary | ✅ By spec (RFC 3533) | Spec says "general-purpose bitstream encapsulation" — but **all existing tooling is multimedia-only** (`7z x file.ogg` won't work; no archive utility recognizes it) |
| 2 | **HTTP/2 framing** | 2015 | ★★★★★ | Binary | ✅ By design | **Binary** stream multiplexer (9-byte binary frame header); but carries protocol complexity beyond just framing |
| 3 | **QUIC** | 2021 | ★★★★☆ | Binary | ✅ By design | **Binary** transport protocol; but requires full stack (TLS, congestion control, UDP) |
| 4 | **SCTP** | 2000 | ★★★☆☆ | Binary | ✅ By design | **Binary** transport protocol with native multi-streaming (16-bit stream IDs); but requires kernel stack |
| 5 | **HTTP/1.1 chunked** | 1997 | ★★★★★ | Text | ✅ By design | **Text-based** framing (hex-length + CRLF); chunk extensions (`;stream=N`) provide multiplexing over pipes — the only text-based multiplexing candidate |
| 6 | **SSH channels** | 1995 | ★★★★★ | Binary | ✅ By design | **Binary** protocol; proven channel multiplexing; but encryption/key-exchange overhead is unnecessary for local pipes |
| 7 | **WebSocket** | 2011 | ★★★★★ | Binary | ✅ By design | **Binary** framed messages; but **single channel only** — multiplexing extension was proposed but never standardized |
| 8 | **9P** | 1995 | ★★☆☆☆ | Binary | ✅ By design | **Binary** file access protocol with tag-based multiplexing; but requires full file-operations state machine |
| 9 | **Protobuf delimited** | 2008 | ★★★★☆ | Binary | ✅ By design | **Binary** varint-prefixed framing; but multi-stream requires custom envelope design |
| 10 | **CBOR sequences** | 2020 | ★★★☆☆ | Binary | ✅ By spec (RFC 8742) | **Binary** self-delimiting items; IETF standardized; but multi-stream requires custom convention |
| 11 | **Cap'n Proto RPC** | 2013 | ★★☆☆☆ | Binary | ✅ By design | **Binary** zero-copy framing with question-based multiplexing; but full RPC protocol overhead |
| 12 | **WARC** | 2009 | ★★★☆☆ | Text headers + binary payload | ✅ By spec (ISO 28500) | **Text headers** (HTTP-style) + binary payload; non-multimedia, ISO-standardized, streaming-capable; but sequential (no interleaving) |
| 13 | **Avro OCF** | 2009 | ★★★☆☆ | Binary | ✅ Data-oriented | **Binary** with JSON schema header; streaming with sync markers; but single-schema per file (no heterogeneous streams) |
| 14 | **gRPC** | 2015 | ★★★★★ | Binary | ✅ By design | **Binary** multiplexing RPC; inherits HTTP/2 framing + adds 5-byte message framing; extremely popular but requires full gRPC runtime (~1M LOC) |
| 15 | **AMQP 1.0** | 2012 | ★★★☆☆ | Binary | ✅ By spec (ISO 19464) | **Binary** connection/session/link multiplexing with named links; ISO standardized; but heavyweight protocol (SASL, flow control, delivery settlement) |
| 16 | **MQTT 5.0** | 2019 | ★★★★☆ | Binary | ✅ By design | **Binary** pub/sub with topic-string naming (closest to file names); but requires broker or protocol reimplementation for peer-to-peer |
| 17 | **NDJSON** | 2013 | ★★★★☆ | Text | ✅ Data-oriented | **Text-based** streaming; de-facto standard for CLI data pipelines; but no framing, no multiplexing, no tombstone |
| 18 | **D-Bus** | 2006 | ★★★★☆ | Binary | ⚠️ IPC-specific | **Binary** message protocol for desktop IPC; bus-level routing but not wire-level multiplexing; heavyweight type system |
| 19 | **Custom LTV** | — | N/A | Binary | ✅ By definition | **Binary** (designer's choice); zero legacy baggage; trivial to implement; but no established standard/tooling |

**Binary vs text**: Nearly all multiplexing formats are **binary** protocols — HTTP/2, QUIC, SSH, Ogg, Protobuf, gRPC, AMQP all use binary framing for efficiency. The notable exceptions are **HTTP/1.1 chunked encoding with chunk extensions** (`;stream=N`), which provides a **text-based multiplexing** option: `hex-length;stream=N\r\n...data...\r\n`. Over HTTP infrastructure (proxies, CDNs) the stream extensions would be stripped, but **over pipes** (direct producer→consumer), the extensions are preserved — making this the only text-based multiplexing approach with a standards basis. **NDJSON** is the most popular text-based streaming format but lacks any multiplexing mechanism. **WARC** uses text headers (HTTP-style `Key: Value\r\n`) with binary payloads, giving it readability for metadata while keeping payload efficiency.

**Ogg** (RFC 3533, 2003) is the **best general-purpose candidate among established formats by specification**. Despite its reputation as "the Vorbis/Opus container," RFC 3533 is explicitly a general-purpose bitstream encapsulation format — it defines pages, stream serial numbers, and granule positions with no multimedia-specific semantics. An Ogg stream carrying arbitrary tagged data chunks is fully spec-compliant. It has IETF standardization, clean implementations (`libogg` in C, crates in Rust, packages in Python/Go), and ~0.5–1% framing overhead with CRC-32 integrity.

**Critical caveat**: In practice, Ogg is entirely a multimedia format. No general-purpose archive tool can handle it — `7z x file.ogg` does not work, `file(1)` reports it as audio/video, and every existing library/tool assumes multimedia content. Using Ogg for arbitrary data would mean writing Ogg pages directly (the page format is simple enough for a clean-room implementation) while accepting that no user or tool in the ecosystem would recognize the result as anything but a broken media file. This gap between spec and ecosystem is the central tension.

**HTTP/2 framing** (RFC 7540, 2015) is the most widely deployed general-purpose multiplexing format in the world, with universal browser/server/CDN support. It is a **binary protocol** — the 9-byte frame header contains binary-encoded fields (3-byte length, 1-byte type, 1-byte flags, 31-bit stream ID), not human-readable text. This was a deliberate design choice: HTTP/1.1 was text-based (human-readable headers), and HTTP/2 switched to binary framing specifically for parsing efficiency and to eliminate text-parsing ambiguities. Its 31-bit stream ID is exactly the "stream N, chunk M" primitive needed. However, extracting just the framing layer from HTTP/2 means ignoring most of the spec (HPACK, flow control, SETTINGS, stream priorities) — it would be using ~5% of a complex protocol. HTTP/2 framing is best understood as **prior art / inspiration** for a new minimal multiplexing format, rather than a format to use directly.

**HTTP/1.1 chunked encoding with chunk extensions** (RFC 7230, 1997) deserves re-evaluation for the **pipe-specific** use case. The chunk extension mechanism (`;stream=N`) is fully specified in RFC 7230 §4.1.1 — adding a stream ID to each chunk is syntactically valid and unambiguous. Over HTTP infrastructure (proxies, CDNs), extensions would be stripped, but **over a pipe** (direct producer→consumer), there is no intermediate infrastructure to flatten the stream IDs. This makes HTTP/1.1 chunked the **only text-based multiplexing candidate**: human-readable with `cat`, debuggable with standard text tools, ~0.03% overhead at 64 KiB chunks, and a zero-length chunk provides a container-level tombstone. The limitation is that no existing tool supports the `;stream=N` convention — but the same is true for any custom binary format.

**SSH channels** (RFC 4254, since ~1995) are the oldest general-purpose multiplexing mechanism still in universal production use. However, the encryption and connection-setup overhead makes it impractical for local pipe multiplexing.

**QUIC** (RFC 9000, 2021) is the most advanced multiplexing transport protocol, fixing HTTP/2's head-of-line blocking problem with independent streams. Its STREAM frames with 62-bit stream IDs and FIN bits are the cleanest modern multiplexing primitive. However, QUIC is a **full transport protocol** — it requires TLS 1.3 encryption, congestion control, packet loss recovery, and runs over UDP. Using QUIC for local pipe multiplexing would be like using SSH channels: technically correct but absurdly over-engineered.

**SCTP** (RFC 4960, 2000) is historically significant as the **first IETF-standardized transport protocol with native multi-streaming** — predating QUIC by 21 years. Its 16-bit stream IDs and independent stream semantics map directly to the multi-stream pipe use case. However, like QUIC and SSH, SCTP is a kernel-level transport protocol requiring a full stack, making it impractical for local pipe use. Important as prior art for the independent-stream concept.

**WebSocket** (RFC 6455, 2011) is universally deployed for real-time web communication but is explicitly a **single-channel** protocol. The WebSocket community recognized the missing multiplexing capability: a multiplexing extension was drafted (draft-ietf-hybi-websocket-multiplexing) but expired in 2013 without standardization. The industry moved to HTTP/2 and WebTransport instead. WebSocket confirms that single-stream framing is well-solved, but multiplexing requires a fundamentally different design.

**9P** (Plan 9, 1995) is a clean multiplexed file access protocol with real modern adoption (Linux kernel v9fs, WSL2, QEMU). Its tag-based request/response multiplexing is elegant prior art, but the protocol requires implementing a full file-operations state machine (walk, open, read, write, close) — too heavyweight for pure data streaming.

**Protobuf delimited messages** deserve mention as one of the most widely deployed length-prefixed framing conventions. The varint-length + message pattern is used by gRPC, Google internal systems, and countless applications. However, multi-stream multiplexing requires a custom envelope message — protobuf is a serialization format, not a multiplexing container.

**CBOR sequences** (RFC 8742, 2020) provide IETF-standardized binary streaming via concatenated self-delimiting items. The self-delimiting encoding eliminates explicit length framing, but multi-stream use requires an application-level convention (e.g., each item wraps a stream ID + payload). More relevant as a serialization choice within a multiplexing container.

**gRPC** (2015) is the most widely deployed multiplexing RPC framework, used by Google Cloud, Kubernetes, and countless microservices. It runs on HTTP/2 and inherits its stream multiplexing, adding a 5-byte length-prefixed message framing (1 byte compressed flag + 4 bytes length). While gRPC confirms that HTTP/2-based multiplexing scales to millions of production systems, using gRPC for pipe multiplexing would require a full gRPC runtime (~1M lines of C++) plus protobuf schema compilation — even heavier than raw HTTP/2.

**AMQP 1.0** (ISO 19464, 2012) is the only ISO-standardized messaging protocol with hierarchical multiplexing (connection → session → link). Links carry named streams (UTF-8 link names), making it one of the few protocols with native string-based stream naming. However, the protocol includes SASL authentication, flow control with link credit, delivery settlement (at-least-once/at-most-once/exactly-once semantics), and a complex type system — extremely heavyweight for pipe use.

**MQTT 5.0** (OASIS, 2019) is notable for having **topic strings** as first-class wire-level concepts — the closest any protocol comes to "file names per stream." Topics like `data/output.csv` are human-readable UTF-8 hierarchies. However, MQTT is a broker-mediated pub/sub protocol; direct peer-to-peer pipe use would require either a broker process or protocol reimplementation.

**NDJSON / JSON Lines** (2013) deserves mention as the **de-facto standard for streaming structured data in CLI pipelines**. Tools like `jq`, Elasticsearch, and Spark treat NDJSON as the standard streaming data format. However, NDJSON has no framing (just newlines), no multiplexing, and no tombstone — it's a payload convention, not a container format.

**WARC** (ISO 28500, 2009) and **Avro OCF** (Apache, 2009) are notable as **non-multimedia** streaming formats with real adoption. WARC is used for web archiving (Internet Archive, Common Crawl) and Avro for big-data pipelines (Kafka, Hadoop). Both support streaming write (no patching) but are fundamentally **sequential** — WARC records and Avro blocks contain homogeneous data with no interleaving mechanism. They confirm that the data engineering and archiving communities have the same streaming needs, but neither format solves the interleaving problem.

**Apache Parquet** (2013) is included because its extreme popularity (★★★★★ — the dominant big-data format) means users will ask about it. Parquet is a **footer-based** columnar format where all metadata is stored at the end of the file. It cannot be streamed (neither writing nor reading) — it's the anti-streaming format. This illustrates that format popularity and streaming capability are largely orthogonal.

### Multimedia formats that also qualify

Among formats that support both streaming and interleaving but are multimedia-oriented:

| Rank | Format | Year | Popularity | All criteria met? |
|---|---|---|---|---|
| 1 | **MPEG-TS** | 1995 | ★★★★☆ | ✅ Yes — no patching, native interleaving, massive tooling |
| 2 | **NUT** | 2003 | ★☆☆☆☆ | ✅ Yes — streaming, interleaving, per-stream EOR tombstones; but negligible adoption |
| 3 | **FLV** | 2002 | ★★★☆☆ | ✅ Yes — streaming, interleaving; but deprecated (Flash EOL), single stream ID only |
| 4 | **MPEG-PS** | 1993 | ★★★☆☆ | ✅ Yes — but declining adoption |
| 5 | **MP4/fMP4** | 2001 | ★★★★★ | ⚠️ Fragmented MP4 only — regular MP4 requires patching; complex box structure |
| 6 | **Matroska** | 2002 | ★★★★☆ | ⚠️ Conditional — needs SeekHead omitted for no-patch write |

These are proven and well-tooled but carry multimedia-specific framing (PIDs, PAT/PMT, PES headers, track entries) that adds unnecessary conceptual and byte overhead for general-purpose data multiplexing.

### Best candidates for a multi-stream streaming pipe format

Evaluated on streaming (no patching), interleaving, general-purpose suitability, stream finalization (tombstones), and **7z extractability** — the ability to list/extract contents with standard archive tools.

**Core finding**: **TAR with interleaved chunk members** emerges as the strongest practical candidate when 7z extractability and file names are requirements. It is the only format that satisfies all of: streaming, file names, 7z extractability, and container-level tombstone. Native multiplexing formats (Ogg, HTTP/2) have better framing but fail 7z extractability. A container-level tombstone (e.g., TAR's two zero blocks) is sufficient for truncation detection — per-stream tombstones are a nice-to-have but not strictly required.

1. **TAR (interleaved chunk members)** — the best practical candidate when 7z extractability and file names are hard requirements. ★★★★★ popularity, streamable (no patching), file names native, `7z l file.tar` works, container-level tombstone (two zero blocks) for truncation detection. "Interleaving" is achieved via naming convention (each chunk is a separate member, e.g., `.streams/output.csv/chunk-000001`). Trade-offs: ~0.8–12.5% overhead depending on chunk size, and a reassembly tool is needed to concatenate chunks back into per-stream files. See the **TAR-based interleaving workaround** section above for details.

2. **Ogg** — the best fit **by specification** for general-purpose use. IETF standard (RFC 3533), explicitly general-purpose by spec, clean page-based multiplexing with CRC integrity, ~0.5–1% overhead, **per-stream EOS flag** for tombstones. **Binary** format (not human-readable). **Major practical limitation**: all existing tooling is multimedia-only — no archive utility recognizes Ogg for arbitrary data (`7z x file.ogg` won't work). Requires a clean-room page writer (~200 lines of C) and accepting that no existing ecosystem tool will help users inspect the result.

3. **HTTP/1.1 chunked with `;stream=N` extensions** — the **only text-based multiplexing candidate**. RFC 7230 §4.1.1 defines chunk extensions; adding `;stream=N` is syntactically valid and well-specified. Over pipes (direct producer→consumer), chunk extensions are preserved — no HTTP proxy stripping. ~0.03% overhead at 64 KiB chunks. Human-readable and debuggable with `cat`/`head`. Trailer headers (RFC 7230 §4.1.2) can carry per-stream metadata. Zero-length chunk provides a container-level tombstone. **Limitation**: no existing tooling supports the `;stream=N` convention (custom muxer/demuxer needed), not 7z-extractable, and over HTTP infrastructure (proxies/CDNs) the extensions would be flattened.

4. **Custom LTV framing** — minimal overhead (~0.01%), trivial to implement (8-byte header: stream_id + length), language-agnostic. **Binary** format (can be designed text-based if desired, e.g., using HTTP/1.1 chunked-style hex lengths). **Should include a container-level end marker** in the design for tombstone functionality. Best choice when no legacy format compatibility is needed and simplicity is paramount.

5. **MPEG-TS** — the most battle-tested streaming format (30 years, digital TV worldwide). **Binary** format. Best choice if multimedia tooling integration is desired or error-resilient sync recovery matters. **Lacks end markers entirely** (by design, for broadcast) — truncation detection requires application-level signaling.

6. **HTTP/2 framing (inspiration)** — a **binary** protocol (the 9-byte frame header with **END_STREAM flag** and **GOAWAY** connection shutdown is worth studying as prior art for any new "mux" format). HTTP/2 is explicitly **not** a text format — it was designed as a binary replacement for HTTP/1.1's text-based framing. Using the full HTTP/2 spec directly is overkill; the framing layer design is the useful takeaway.

7. **QUIC / SCTP (inspiration)** — **binary** transport protocols representing the state-of-the-art (QUIC, RFC 9000, 2021) and the original (SCTP, RFC 4960, 2000) in multiplexed transport design. Per-stream FIN bit (QUIC), independent streams without HOL blocking (both), and 62-bit (QUIC) / 16-bit (SCTP) stream IDs. SCTP predates QUIC by 21 years and proves that independent multi-streaming was recognized as a fundamental transport need. Both are useful as prior art rather than direct reuse — they require full transport stacks (TLS, congestion control, kernel sockets).

### Non-starters for multi-stream use

- **CPIO, ar, LHA/LZH, WARC** — sequential, no interleaving, not 7z-extractable (except CPIO via some builds).
- **TAR (plain sequential)** — native TAR is sequential; only the interleaved chunk member convention (see #1 above) enables multi-stream use.
- **ZIP** — Central Directory at end breaks streaming read.
- **7-Zip** — headers at end, no streaming.
- **RAR** — proprietary, no interleaving.
- **ISO 9660** — a filesystem, not an archive stream; requires pre-computed sector layout.
- **WIM** — deployment image format; must patch header with resource table offset.
- **CAB** — Windows installer archive; header requires pre-computed offsets and counts.
- **XAR** — XML TOC at beginning references heap by offset; requires pre-computation.
- **HTTP/1.1 chunked (over HTTP infra)** — over proxies/CDNs, chunk extensions are stripped, so stream IDs are lost. But **over pipes** (direct producer→consumer), chunk extensions with `;stream=N` ARE viable — see #3 in the best candidates ranking above.
- **AVI/RIFF** — native interleaving, but RIFF header requires total size (patching); multimedia-specific.
- **IFF** — historically important (1985, first general-purpose TLV container), but chunk sizes needed upfront.
- **ASF** — Microsoft proprietary, needs file-size in header.
- **CAF** — Apple-only, audio-specific.
- **Avro OCF** — streaming-capable, but single-schema per file (no heterogeneous multi-stream).
- **Protobuf delimited** — excellent framing primitive, but multi-stream requires custom envelope (not a standard).
- **MP4 (regular)** — moov atom requires pre-computation or patching (fMP4 streams but is complex).
- **FLV** — streaming and interleaving, but deprecated (Flash EOL 2020), single stream ID field unused.
- **WebSocket** — excellent single-stream framing with universal support, but explicitly single-channel; multiplexing extension was never standardized.
- **D-Bus** — heavyweight IPC protocol for desktop services; bus-level routing, not wire-level multiplexing; typed message overhead.
- **CBOR sequences** — clean IETF-standardized binary streaming, but no built-in multiplexing (requires application convention).
- **Cap'n Proto** — zero-copy RPC framework; question-based multiplexing but requires full RPC protocol implementation.
- **NDJSON** — excellent text-based streaming for single-stream structured data; no multiplexing, no tombstone, no framing — just JSON objects separated by newlines.
- **MessagePack** — popular binary serialization; self-delimiting values but no multiplexing container (same category as CBOR/Protobuf).
- **Apache Parquet** — columnar big-data format; footer-based metadata means it **cannot be streamed at all** — the entire file must exist before reading.
- **gRPC** — inherits HTTP/2 multiplexing with universal deployment; but requires full gRPC runtime (~1M lines of C++) plus protobuf schemas — even heavier than raw HTTP/2.
- **AMQP 1.0** — ISO-standardized messaging with named link multiplexing; but extremely heavyweight protocol (SASL, flow control, delivery settlement, type system).
- **MQTT 5.0** — topic-based naming is attractive, but broker-mediated pub/sub model is incompatible with direct pipe use.

### 7z extractability and stream naming

A practical requirement: can the output be listed and extracted using `7z` (p7zip / 7-Zip), the most versatile command-line archive tool? And can individual streams carry **file names** (not just numeric IDs) so extracted data is human-identifiable?

#### Formats supported by 7z for extraction/listing

`7z` (via p7zip) can list and extract the following formats from the 45 analyzed:

| Format | 7z support | Streaming (no patching) | Interleaving | File names |
|---|---|---|---|---|
| **TAR** | ✅ `7z l file.tar` | ✅ Yes | ❌ No | ✅ Full paths (up to 256 chars; PAX: unlimited) |
| **CPIO** | ✅ `7z l file.cpio` | ✅ Yes | ❌ No | ✅ Full paths |
| **ar** | ✅ `7z l file.a` | ✅ Yes | ❌ No | ✅ Short names (16 chars; extended: longer) |
| **ZIP** | ✅ `7z l file.zip` | ⚠️ Conditional | ❌ No | ✅ Full paths |
| **7-Zip** | ✅ `7z l file.7z` | ❌ No (patching) | ❌ No | ✅ Full paths |
| **RAR** | ✅ `7z l file.rar` | ⚠️ Partial | ❌ No | ✅ Full paths |
| **CAB** | ✅ `7z l file.cab` | ❌ No (patching) | ❌ No | ✅ Full paths |
| **WIM** | ✅ `7z l file.wim` | ❌ No (patching) | ❌ No | ✅ Full paths |
| **ISO 9660** | ✅ `7z l file.iso` | ❌ No (pre-computed) | ❌ No | ✅ Full paths |
| **XAR** | ✅ `7z l file.xar` | ❌ No (pre-computed) | ❌ No | ✅ Full paths |
| **LHA/LZH** | ✅ `7z l file.lzh` | ✅ Yes | ❌ No | ✅ Full paths |

**Not supported by 7z**: Ogg, MPEG-TS, MPEG-PS, Matroska/WebM, ASF, CAF, HTTP/2, SSH, HTTP/1.1, MIME, FLV, NUT, WARC, Avro, Protobuf, QUIC, SCTP, WebSocket, D-Bus, 9P, Cap'n Proto, CBOR, MP4, AVI/RIFF, IFF.

**Key finding**: Among all 7z-extractable formats, **none support interleaving**. Every 7z-compatible format is strictly sequential — files must be written one at a time, fully, before the next begins. This means:

- **No existing format simultaneously satisfies all three requirements**: 7z-extractable + streaming (no patching) + interleaving.
- The best 7z-compatible streaming formats (TAR, CPIO, LHA/LZH) support file names natively but lack interleaving entirely.
- ZIP is 7z-extractable and has file names, but streaming requires data descriptors (conditional), and interleaving is not supported.

#### Stream naming (file names per stream)

For multi-stream pipe use, each logical stream should be identifiable by a **name** (e.g., a file path like `output.csv` or `metadata.json`), not just a numeric index. Here's how the relevant formats handle naming:

| Format | Stream naming mechanism | Named? |
|---|---|---|
| **TAR** | File path in 512-byte header (ustar: 256 chars; PAX: unlimited) | ✅ Full paths |
| **CPIO** | File path in header (variable length) | ✅ Full paths |
| **ZIP** | File path in Local File Header + Central Directory | ✅ Full paths |
| **Ogg** | 32-bit serial number (integer only) | ❌ Numeric ID only — name requires application-level convention |
| **MPEG-TS** | 13-bit PID number | ❌ Numeric ID only — PAT/PMT carry service names, not file names |
| **Matroska** | Track Name element (UTF-8 string) in TrackEntry | ✅ Track names |
| **HTTP/2** | 31-bit stream ID (integer) + HEADERS frame can carry `:path` | ⚠️ Possible via HTTP headers, but heavyweight |
| **SSH** | Channel ID (integer) + channel type string at open | ⚠️ Channel type only (e.g., "session"), not a filename |
| **NUT** | Stream ID (integer) + stream header metadata | ⚠️ Metadata possible but no standard filename field |
| **QUIC** | 62-bit stream ID (integer) | ❌ Numeric ID only |
| **FLV** | Tag type byte (audio/video/script) | ❌ Fixed types, no naming |
| **MP4/fMP4** | Track Name box (udta/name) or handler name | ⚠️ Possible but non-standard |
| **Protobuf** | Field tags (integers) — names in .proto schema only | ❌ Numeric tags only at wire level |
| **SCTP** | 16-bit stream ID (integer) | ❌ Numeric ID only |
| **WebSocket** | N/A (single channel) | ❌ No stream concept |
| **D-Bus** | Object path + interface + member name | ✅ Path-based naming |
| **9P** | File path (walk + fid) | ✅ Full file paths |
| **Cap'n Proto** | Question ID (integer) | ❌ Numeric IDs only |
| **CBOR sequences** | Application-defined per item | ❌ No standard naming |
| **NDJSON** | Application-defined per line | ❌ No standard naming |
| **MessagePack** | Application-defined per value | ❌ No standard naming |
| **gRPC** | Method name (service/method path) | ⚠️ Method paths, not file names |
| **AMQP 1.0** | Link name (UTF-8 string) | ✅ Link names |
| **MQTT 5.0** | Topic string (UTF-8 hierarchy) | ✅ Topic strings (e.g., `data/output.csv`) |
| **Custom LTV** | Designer's choice — can include name field | ✅ If designed with name support |

**Finding**: Only archive formats (TAR, CPIO, ZIP), Matroska, D-Bus (object paths), 9P (file paths), AMQP 1.0 (link names), and MQTT (topic strings) natively support human-readable names per stream. All other protocol-style multiplexers (HTTP/2, SSH, QUIC, SCTP, Ogg) use numeric stream IDs — file name mapping must happen at the application level (e.g., a manifest message at the start of the stream that maps stream ID → file name). MQTT is notable for having the closest thing to "file names" as a first-class protocol concept via topic strings.

#### TAR-based interleaving workaround

Given the requirement for 7z extractability + file names, a **TAR-based approach** deserves reconsideration despite its sequential nature.

**The core idea**: TAR is a sequential format — normally you write file A completely, then file B completely. But nothing stops you from writing _many small files_ instead, where each small file is a _chunk_ of a logical stream. By using a naming convention like `.streams/<stream-name>/chunk-NNNNNN`, a writer can alternate between streams at chunk boundaries, and a reader (demuxer) can reconstruct the original streams by concatenating chunks with the same stream name.

**Worked example** — suppose a CLI tool produces two outputs simultaneously: `output.csv` (large, generated incrementally) and `metadata.json` (small, generated alongside). Instead of writing one complete file after another, the writer emits them as interleaved TAR members:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ TAR member 1:  .streams/output.csv/chunk-000001     [512-byte header]  │
│                  ← 4096 bytes of output.csv data (rows 1–100) →        │
├─────────────────────────────────────────────────────────────────────────┤
│ TAR member 2:  .streams/metadata.json/chunk-000001  [512-byte header]  │
│                  ← 128 bytes of metadata.json data →                   │
├─────────────────────────────────────────────────────────────────────────┤
│ TAR member 3:  .streams/output.csv/chunk-000002     [512-byte header]  │
│                  ← 4096 bytes of output.csv data (rows 101–200) →      │
├─────────────────────────────────────────────────────────────────────────┤
│ TAR member 4:  .streams/metadata.json/chunk-000002  [512-byte header]  │
│                  ← 256 bytes of metadata.json data →                   │
├─────────────────────────────────────────────────────────────────────────┤
│ ... more interleaved chunks ...                                        │
├─────────────────────────────────────────────────────────────────────────┤
│ TAR end-of-archive: two 512-byte zero blocks (tombstone)               │
└─────────────────────────────────────────────────────────────────────────┘
```

**What the reader/demuxer does**: read TAR members one by one, group chunks by stream name (`.streams/output.csv/*` vs `.streams/metadata.json/*`), concatenate each group's payloads in order → reconstructed `output.csv` and `metadata.json`. Both streams are available incrementally as data arrives — the reader doesn't need to wait for all of `output.csv` before seeing `metadata.json` data.

**What existing tools see**:
- `tar -t` lists all chunks as individual files — not pretty, but works
- `7z l file.tar` lists them too — 7z extractability preserved
- `tar -x` extracts thousands of small chunk files — a reassembly step is needed afterward (e.g., `cat .streams/output.csv/chunk-* > output.csv`)

**Why it works for streaming**: each TAR member header contains the chunk's size upfront (known at write time since the writer chooses the chunk size), so no seeking/patching is needed. The writer appends header+data for each chunk in a single forward pass. The two zero blocks at the end serve as a tombstone — if they're missing, the stream was truncated.

**Pros**: `7z l file.tar` works, file names are visible, any TAR tool can extract, streaming write (no patching), container-level tombstone (two zero blocks).
**Cons**: ~512 bytes overhead per chunk (TAR header per member), reassembly required (chunks must be concatenated by stream name), not true multiplexing — it's a convention on top of a sequential format.

At 4 KiB payload chunks, TAR member overhead is ~12.5% (512-byte header per 4096-byte payload). At 64 KiB chunks, overhead drops to ~0.8% — still 80× higher than custom LTV framing.

#### Intersection analysis

Placing all requirements together:

| Requirement | TAR (interleaved hack) | Ogg | HTTP/2 framing | SCTP | 9P | gRPC | AMQP 1.0 | Custom LTV | Matroska |
|---|---|---|---|---|---|---|---|---|---|
| Streaming (no patching) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ Conditional |
| Interleaving | ✅ (via naming convention) | ✅ Native | ✅ Native | ✅ Native | ✅ Tag-based | ✅ Via HTTP/2 | ✅ Native | ✅ By design | ✅ Native |
| File names | ✅ Native (paths in headers) | ❌ Numeric IDs | ⚠️ Via HTTP headers | ❌ Numeric IDs | ✅ File paths | ⚠️ Method paths | ✅ Link names | ✅ If designed in | ✅ Track names |
| 7z extractable | ✅ `7z l file.tar` | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Tombstone (end marker) | ✅ Two zero blocks | ✅ EOS flag (per-stream) | ✅ END_STREAM (per-stream) | ✅ SHUTDOWN | ⚠️ Tclunk per fid | ✅ Trailers (per-RPC) | ✅ Detach (per-link) | ✅ If designed in | ❌ None |
| Low overhead | ❌ (~0.8–12.5%) | ✅ (~0.5–1%) | ✅ (~0.05%) | ✅ (~0.01%) | ⚠️ Request/response | ❌ (~0.1%+) | ❌ (~0.5%+) | ✅ (~0.01%) | ✅ (~0.1–1%) |
| No full protocol stack required | ✅ | ✅ | ❌ (HPACK, flow ctrl) | ❌ (kernel stack) | ❌ (file ops FSM) | ❌ (HTTP/2 + gRPC runtime) | ❌ (SASL, flow ctrl, settlement) | ✅ | ✅ |

**Core trade-off**: TAR is the **only** format that is simultaneously 7z-extractable, streaming-writable, can carry file names, has a container-level tombstone, and requires no protocol stack beyond simple file I/O. It achieves "interleaving" through a naming convention (each chunk is a separate TAR member), with significant overhead vs custom LTV. Every native multiplexing format (Ogg, HTTP/2, QUIC, SCTP, NUT, 9P, gRPC, AMQP) fails the 7z extractability test. Transport protocols and RPC frameworks (SCTP, QUIC, HTTP/2, SSH, gRPC, AMQP) additionally require protocol stacks inappropriate for local pipe use.

**Practical conclusion**: **TAR with interleaved chunk members is the strongest candidate** when 7z extractability and file names are hard requirements. It satisfies streaming (no patching), interleaving (via naming convention), file names (native), 7z extractability, and truncation detection (two zero blocks as tombstone). The trade-offs are overhead (~0.8–12.5% depending on chunk size) and the need for a reassembly tool to reconstruct per-stream files from chunks. If 7z extractability can be relaxed (e.g., a dedicated `mux` tool is acceptable), then a custom LTV format remains the lowest-overhead option.

### Current status and practical recommendation

After evaluating 45 formats across archives, multimedia containers, transport protocols, messaging systems, data formats, and custom framing options, the research concludes that **no existing format fully satisfies all requirements** (streaming without patching + native interleaving + file names + 7z extractability + tombstone). The gap between what exists and what is needed is the core finding.

**For immediate practical use**, the **higher-FD (3+) approach** is the most viable path: tools like GPG (`--status-fd`), bubblewrap (`--json-status-fd`), and apt (`APT::Status-Fd`) demonstrate that multi-FD output works reliably for single-hop producer→consumer scenarios. FDs 3–9 are portable across all POSIX shells (bash, zsh, ksh, dash); FDs ≥10 work in bash/zsh/ksh but are silently misparsed by dash. This approach has zero framing overhead, zero latency, and requires no format parsing — but it does not compose across pipeline stages and is not portable to Windows or Nushell.

**For composable multi-stream pipelines** (multiple stages, arbitrary stream counts, cross-platform), the best existing options are:
1. **TAR with interleaved chunk members** — the only format satisfying 7z extractability + file names + streaming + tombstone, at the cost of ~0.8–12.5% overhead and requiring a reassembly tool.
2. **HTTP/1.1 chunked encoding with `;stream=N` extensions** — the only text-based multiplexing option, human-debuggable, ~0.03% overhead, but requires custom tooling.
3. **Custom LTV framing** — lowest overhead (~0.01%), simplest to implement, but no established standard.

### Future work

- Evaluate additional format candidates not yet covered (the space of wire protocols and container formats is vast).
- Investigate feasibility of a new minimal open specification designed specifically for general-purpose multi-stream CLI piping (working name: "mux"). HTTP/2's 9-byte frame header, QUIC's per-stream FIN bit, SCTP's independent stream model, Ogg's page structure, NUT's startcode sync, and HTTP/1.1's chunked encoding (hex-length + extensions) are the strongest prior art to draw from.
- Prototype and benchmark TAR-based interleaved chunking vs custom LTV framing for real-world CLI workloads.
- Explore whether an RFC or community specification process could establish a standard for multi-stream CLI piping.

---

## Appendix A: Framing Overhead Comparison

A practical concern when choosing a multi-stream container for CLI pipes is **framing overhead** — how many extra bytes per payload chunk does the format add?

| Format | Header per chunk | Payload per chunk | Overhead % | Notes |
|---|---|---|---|---|
| **LTV (custom)** | 8 bytes (4 stream_id + 4 length) | Variable (any size) | < 0.01% at 64 KiB chunks | Minimal; no checksum, no sync |
| **Ogg** | 27–282 bytes (27 fixed + 0–255 segment table) | Up to 65,025 bytes (255 × 255) | ~0.5–1% typical | Includes CRC-32 checksum; variable page size; segment table adds 1 byte per 255-byte segment |
| **MPEG-TS** | 4 bytes | 184 bytes (fixed) | ~2.13% | Fixed 188-byte packets; includes sync byte for error recovery; adaptation field may reduce payload further |
| **HTTP/2 framing** | 9 bytes | Up to 16,384 bytes (default) | ~0.05% at max frame | Well-defined; stream ID native; but designed for TCP, not pipes |
| **HTTP/1.1 chunked** | ~15–22 bytes (hex-len + `;stream=N` + CRLFs) | Variable (any size) | ~0.03% at 64 KiB chunks | Stream tagging via chunk extensions adds ~10 bytes; universally parseable |
| **Netstring** | ~5–10 bytes (`len:...,`) | Variable | < 0.01% at large sizes | ASCII length prefix; simple but no stream ID built in |
| **FLV** | 11 bytes (type + size + timestamp + stream_id) + 4 PreviousTagSize | Variable | ~0.02% at 64 KiB tags | Simple tag structure; PreviousTagSize adds 4 bytes per tag |
| **Protobuf delimited** | 1–10 bytes (varint length prefix) | Variable | < 0.01% at large messages | Minimal; varint encoding; no stream ID built in |
| **QUIC STREAM frame** | 1–17 bytes (type + stream_id + offset + length) | Variable | < 0.03% at 64 KiB frames | Variable-length integer encoding; stream ID native |
| **NUT** | 1–13 bytes (startcode + stream_id + pts + size) | Variable | ~0.02% at 64 KiB frames | Variable-length coding; startcodes for sync recovery |
| **MP4/fMP4** | ~100+ bytes (moof box per fragment) | Variable | ~0.1–0.5% | Box structure adds significant per-fragment overhead; includes track/sample metadata |
| **SCTP** | 16+ bytes (DATA chunk header) | Variable | ~0.02% at 64 KiB | Transport-level; includes TSN, stream ID, SSN |
| **WebSocket** | 2–14 bytes (opcode + length + optional mask) | Variable | ~0.02% at 64 KiB | Simple framing; but single-stream only |
| **CBOR sequences** | 1–9 bytes (type + length encoding) | Variable | < 0.01% at large items | Self-delimiting; no explicit length prefix for known types |
| **Cap'n Proto** | 8+ bytes (segment table) | Variable | ~0.01% at large segments | Zero-copy design; segment-based framing |
| **TAR (chunk hack)** | 512 bytes (member header) | Variable | ~0.8% at 64 KiB, ~12.5% at 4 KiB | Fixed 512-byte header per chunk; 512-byte alignment padding |
| **gRPC** | 5 bytes (1 compress flag + 4 length) + HTTP/2 9 bytes | Variable | ~0.02% at 64 KiB (gRPC framing only) | gRPC adds 5 bytes per message on top of HTTP/2 framing; total overhead depends on HTTP/2 frame size |
| **AMQP 1.0** | ~50+ bytes (transfer performative) | Variable | ~0.08% at 64 KiB | Transfer frame includes delivery ID, message format, settled flag; significant per-message overhead |
| **MQTT 5.0** | 2–5 bytes (fixed header) + topic length | Variable | ~0.05% at 64 KiB | Compact variable-byte-integer length; topic string adds per-message overhead |
| **NDJSON** | 0 bytes (no framing) | Variable | ~0% (pure payload) | No framing overhead; but no stream ID, no length prefix — newline-delimited only |

**Key takeaway**: Custom LTV framing has the lowest overhead for high-throughput CLI pipes. Ogg and MPEG-TS add meaningful overhead but provide checksums (Ogg) or sync recovery (MPEG-TS) which matter for unreliable channels. For reliable UNIX pipes, the extra error-resilience features are less valuable, making LTV or Ogg the pragmatic choices.

---

## Appendix B: Survey of Existing CLI Multi-Stream Tools

Several existing UNIX tools interact with multi-stream patterns. None of them solve the core multiplexing problem, but they illustrate the design space:

#### `tee`

Duplicates stdin to one or more files while also writing to stdout. This is a **fan-out** (1-to-N) tool, not a multiplexer. Combined with process substitution, it can feed multiple commands:
```bash
cmd | tee >(filter1 > out1) >(filter2 > out2) > /dev/null
```
Limitation: all consumers see the same data; there is no per-stream differentiation.

#### `pv` (Pipe Viewer)

Monitors data throughput on a single pipe. Writes progress information to stderr while passing stdin through to stdout unchanged. This is a single-stream tool that uses stderr as a side channel — exactly the hack the issue describes as inadequate.

#### GNU Parallel

Distributes work across multiple processes. With `--pipe`, it splits stdin into chunks and fans them out to parallel instances of a command. Output is collected and serialized back to stdout (in order by default). This is a **parallelization** tool, not a multiplexer: the output is a single merged stream, not interleaved tagged sub-streams.

```bash
A | parallel --pipe B | C
```

GNU Parallel explicitly avoids interleaving output from different jobs — each job's output is buffered and printed atomically. This is useful but orthogonal to the multi-stream multiplexing problem.

#### `socat`

A powerful relay tool that can connect diverse I/O channels (pipes, sockets, FDs, PTYs). It can bridge higher FDs to sockets or files, but operates on exactly two endpoints (unidirectional or bidirectional). It does not multiplex multiple streams into one.

#### `multiplex` / `tmux` / `screen`

Terminal multiplexers that manage multiple virtual terminals over a single connection. They solve a UI problem (multiple interactive sessions), not a data-pipeline multiplexing problem. Their wire protocols (e.g., tmux's control mode) are not designed for general-purpose stream interleaving.

**Conclusion**: No existing general-purpose UNIX CLI tool provides transparent multi-stream multiplexing over a single pipe. The gap identified in the issue is real and unfilled by current tooling.

---

## Appendix C: TAR/CPIO Interleaving Extension Feasibility

Could TAR or CPIO be extended to support chunk interleaving without breaking existing readers?

#### TAR with PAX Extended Headers

The POSIX **pax** interchange format allows arbitrary key-value extended header records before each file entry. In theory, a convention could be designed:

1. Each "chunk" is written as a small TAR member with a special filename convention (e.g., `.streams/stream-0/chunk-0042`).
2. Chunks from different streams are interleaved as separate TAR members.
3. A demultiplexer reads the stream, groups chunks by stream name, and reassembles each stream.

**Pros**:
- Backwards-compatible: standard `tar -t` would list all chunks; `tar -x` would extract them as files.
- No format changes needed; pure convention.
- `libarchive` streaming API could produce and consume this.

**Cons**:
- Massive overhead: each chunk requires a 512-byte TAR header (minimum), so a 4 KiB payload chunk has ≥12.5% framing overhead. A 64 KiB chunk still has ~0.8% overhead, but the 512-byte alignment padding adds more waste for non-aligned sizes.
- No standard demultiplexer exists; every consumer would need custom logic.
- TAR's member-at-a-time model means existing tools (`tar -x`) would create thousands of small files rather than reassembling streams.
- Granularity is limited by TAR header size: sub-512-byte chunks are wasteful.

#### CPIO Interleaving

The same approach could work with CPIO's smaller headers (76 or 110 bytes for `newc`), reducing per-chunk overhead slightly. The same fundamental limitations apply: no existing tool would understand the convention, and the overhead is still far higher than a purpose-built framing format.

**Verdict**: While technically possible, extending TAR or CPIO for interleaving produces a worse result than using Ogg or custom LTV framing in every measurable dimension (overhead, tooling, simplicity). The only advantage would be superficial compatibility with `tar`/`cpio` commands, which would not actually be useful since those tools would not reassemble the streams.

---

## Appendix D: Zero-Copy Pipe Performance (`splice(2)` and `vmsplice(2)`)

For high-throughput CLI pipelines, the cost of copying data through userspace is significant. Linux provides kernel-level zero-copy mechanisms:

#### `splice(2)`

Moves data between two file descriptors **without copying through userspace**, as long as at least one FD is a pipe. Internally, Linux pipes are implemented as ring buffers of `struct pipe_buffer` entries, each pointing to a kernel memory page. `splice()` transfers page references (pointer + refcount) rather than copying data, achieving true zero-copy.

- **Default pipe buffer**: 16 slots × 4 KiB pages = 64 KiB. Can be increased via `fcntl(F_SETPIPE_SZ)` up to `/proc/sys/fs/pipe-max-size` (typically 1 MiB).
- **Atomicity**: Writes ≤ `PIPE_BUF` (4 KiB on Linux) are atomic. Larger writes may be split across multiple pipe buffer slots.
- **Use case**: A multiplexer could `splice()` data from input FDs into the pipe without ever touching the payload in userspace — only the framing headers need to be constructed in userspace.

#### `vmsplice(2)`

Maps **userspace memory pages** into a pipe's ring buffer without copying. This allows a writer to construct data in userspace and then zero-copy-transfer it into a pipe. Combined with `splice()` on the reader side, an entire pipeline can avoid memcpy for payload data.

#### `tee(2)` (kernel)

Duplicates data from one pipe to another without consuming it (copy-on-write semantics). Useful for fan-out patterns where multiple consumers need the same data.

#### Implications for Multi-Stream Pipe Format

A well-designed multiplexer could:

1. Use `splice(2)` to move payload data from source FDs into the output pipe without userspace copies.
2. Use `vmsplice(2)` to inject framing headers (stream ID + length) constructed in userspace.
3. Achieve near-wire-speed throughput limited only by pipe buffer size and scheduling overhead.

This makes the **LTV custom framing** approach even more attractive: its 8-byte headers are trivially constructed in userspace and can be vmspliced, while payload data can be spliced directly from source FDs, achieving zero-copy for the bulk of the data.

**Note**: `splice(2)` and `vmsplice(2)` are Linux-specific. macOS provides no equivalent (the `splice` name is used for a different purpose). FreeBSD has `sendfile(2)` but not `splice`. Portable code must fall back to `read(2)`/`write(2)` on non-Linux systems.

---

## References

- [RFC 3533 — The Ogg Encapsulation Format Version 0](https://www.rfc-editor.org/rfc/rfc3533)
- [Ogg Framing Specification (Xiph.Org)](https://xiph.org/ogg/doc/framing.html)
- [Matroska Specification](https://www.matroska.org/technical/specs/index.html)
- [EBML Specification (RFC 8794)](https://www.rfc-editor.org/rfc/rfc8794)
- [ISO 13818-1 — MPEG-2 Systems (Transport Stream)](https://www.iso.org/standard/74427.html)
- [MPEG-TS Introduction (TSDuck)](https://tsduck.io/docs/mpegts-introduction.pdf)
- [ZIP Application Note (PKWARE)](https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT)
- [GNU tar manual](https://www.gnu.org/software/tar/manual/)
- [libarchive — Multi-format streaming archive library](http://libarchive.org/)
- [Bash Reference Manual — Redirections](https://www.gnu.org/software/bash/manual/bash.html#Redirections)
- [POSIX Shell Command Language — Redirection](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/V3_chap02.html#tag_18_07)
- [nushell FD support issue (nushell/nushell#15650)](https://github.com/nushell/nushell/issues/15650)
- [ASF Specification (Microsoft)](https://learn.microsoft.com/en-us/windows/win32/wmformat/asf-specification)
- [splice(2) — Linux man page](https://www.man7.org/linux/man-pages/man2/splice.2.html)
- [vmsplice(2) — Linux man page](https://www.man7.org/linux/man-pages/man2/vmsplice.2.html)
- [GNU Parallel Tutorial](https://www.gnu.org/software/parallel/parallel_tutorial.html)
- [RFC 7230 — HTTP/1.1 Message Syntax and Routing (Chunked Transfer Coding)](https://www.rfc-editor.org/rfc/rfc7230#section-4.1)
- [RFC 7540 — Hypertext Transfer Protocol Version 2 (HTTP/2)](https://www.rfc-editor.org/rfc/rfc7540)
- [RFC 4254 — The Secure Shell (SSH) Connection Protocol](https://www.rfc-editor.org/rfc/rfc4254)
- [RFC 2046 — MIME Part Two: Media Types (Multipart)](https://www.rfc-editor.org/rfc/rfc2046#section-5.1)
- [ECMA-119 / ISO 9660 — Volume and File Structure of CD-ROM](https://www.ecma-international.org/publications-and-standards/standards/ecma-119/)
- [WIM File Format (Microsoft)](https://learn.microsoft.com/en-us/previous-versions/windows/desktop/xpewim/wim-file-format)
- [Microsoft Cabinet File Format](https://learn.microsoft.com/en-us/previous-versions/bb267310(v=msdn.10))
- [RFC 4960 — Stream Control Transmission Protocol (SCTP)](https://www.rfc-editor.org/rfc/rfc4960)
- [RFC 6455 — The WebSocket Protocol](https://www.rfc-editor.org/rfc/rfc6455)
- [RFC 9000 — QUIC: A UDP-Based Multiplexed and Secure Transport](https://www.rfc-editor.org/rfc/rfc9000)
- [RFC 8949 — Concise Binary Object Representation (CBOR)](https://www.rfc-editor.org/rfc/rfc8949)
- [RFC 8742 — Concise Binary Object Representation (CBOR) Sequences](https://www.rfc-editor.org/rfc/rfc8742)
- [D-Bus Specification](https://dbus.freedesktop.org/doc/dbus-specification.html)
- [9P Protocol — Plan 9 Manual](http://man.cat-v.org/plan_9/5/intro)
- [Cap'n Proto RPC Protocol](https://capnproto.org/rpc.html)
- [JSON Lines / NDJSON specification](https://jsonlines.org/)
- [RFC 7464 — JavaScript Object Notation (JSON) Text Sequences](https://www.rfc-editor.org/rfc/rfc7464)
- [MessagePack specification](https://msgpack.org/)
- [Apache Parquet Format Specification](https://parquet.apache.org/documentation/latest/)
- [gRPC Core Concepts](https://grpc.io/docs/what-is-grpc/core-concepts/)
- [gRPC over HTTP/2 (gRPC wire format)](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
- [AMQP 1.0 Specification (OASIS / ISO 19464)](https://www.amqp.org/specification/1.0/amqp-org-download)
- [MQTT 5.0 Specification (OASIS)](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
