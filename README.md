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

Using `stderr` as a second output channel is possible but semantically wrong and fragile. Using higher-numbered file descriptors (3, 4, 5, …) is technically possible at the OS level, but shell and tooling support varies widely (see [Higher Pipes Compatibility](#higher-pipes-fd-3-4-5--compatibility-with-shells)).

The most portable and interoperable alternative is to **multiplex several logical streams into a single byte stream** — i.e., to send a container or archive format through stdout, where the container supports genuine chunk-level interleaving of multiple member files/streams.

### Research Goals

Evaluate existing archive and container formats against the following criteria:

1. **Popularity** — how widely deployed the format is; availability of tooling.
2. **Streaming compatibility** — can the format be **written in a single forward pass** without seeking back to patch previously-written bytes? A format "supports streaming" if a writer can produce a complete, valid output by only appending data — never modifying bytes that have already been emitted. (Correspondingly, can a reader consume the format in a single forward pass?)
3. **Existing implementations that support chunk interleaving** — tools or libraries that actively interleave chunks from multiple members rather than writing each member end-to-end before starting the next.
4. **Theoretical support of interleaving on the wire** — does the format specification allow or describe interleaving, even if no common implementation exploits it?

---

## Archive / Container Format Comparison

### Summary Table

| Format | Popularity | Write-streaming (no patching) | Read-streaming (forward-only) | Interleaving: implementations | Interleaving: theoretical |
|---|---|---|---|---|---|
| TAR | ★★★★★ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| CPIO | ★★★☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| ar | ★★☆☆☆ | ✅ Yes | ✅ Yes | ❌ None known | ❌ Not in spec |
| ZIP | ★★★★★ | ⚠️ Yes with data descriptors | ❌ Requires seek for central dir | ❌ None known | ❌ Not in spec |
| 7-Zip | ★★★★☆ | ❌ No (must patch start header) | ❌ No | ❌ None known | ❌ Not in spec |
| RAR | ★★★☆☆ | ⚠️ Partial (may need patching) | ⚠️ Partial | ❌ None known | ❌ Not in spec |
| Ogg | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| Matroska / MKV | ★★★★☆ | ⚠️ Yes if SeekHead omitted | ✅ Yes (without SeekHead) | ✅ Native interleaving | ✅ Yes — in spec |
| WebM | ★★★☆☆ | ⚠️ Same as Matroska | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| MPEG-TS | ★★★★☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| MPEG-PS | ★★★☆☆ | ✅ Yes | ✅ Yes | ✅ Native multiplexing | ✅ Yes — in spec |
| ASF / WMV / WMA | ★★★☆☆ | ⚠️ Header needs file size | ✅ Yes | ✅ Native interleaving | ✅ Yes — in spec |
| CAF | ★★☆☆☆ | ✅ Yes (size -1 = unknown) | ✅ Yes | ✅ Audio tracks | ✅ Yes — in spec |
| Framing (custom) | N/A | ✅ Yes | ✅ Yes | ✅ By design | ✅ By design |

---

### Detailed Analysis

#### TAR (Tape Archive)

**Popularity**: ★★★★★ — ubiquitous on all UNIX-like systems; the de-facto standard for UNIX archiving.

**Format overview**: TAR is a purely sequential format. Each member consists of a 512-byte header block immediately followed by the file's data padded to a multiple of 512 bytes. A two-block all-zero trailer marks end-of-archive. Extensions (ustar / POSIX.1-2001 / GNU tar / pax) add long filenames, extended metadata, and sparse file support via additional header types, but the fundamental sequential layout is unchanged.

**Sequential-write streaming**: ✅ Fully supported — no patching needed. A TAR stream can be produced in a single forward pass; each header's size field is filled before its data is written, and no previously-written bytes are ever modified. This is why `tar -c | gzip | ssh host tar -xz` works reliably.

**Sequential-read streaming**: ✅ Fully supported. A TAR reader needs only a forward scan.

**Chunk interleaving (implementations)**: ❌ None known. All standard TAR implementations (`GNU tar`, `bsdtar`, `libarchive`) write each member file completely (header + all data blocks) before starting the next member. There is no mechanism to pause in the middle of a member's data and insert data for another member.

**Chunk interleaving (theoretical)**: ❌ The TAR specification provides no mechanism for interleaving. A non-standard extension could be imagined using PAX extended header records as framing tokens, but this would be entirely non-standard.

**Conclusion for multi-stream use**: TAR is excellent for single-pass streaming of a flat sequence of files, but fundamentally cannot interleave chunks from different members. It is not suitable as a multi-stream wire format without a new wrapping layer.

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

**Format overview**: The Ogg container format (RFC 3533) is built on **pages**. Each page belongs to exactly one **logical bitstream**, identified by a 32-bit serial number in the page header. Pages from different logical bitstreams are freely interleaved in the physical bitstream. A physical bitstream may contain:

- A single logical bitstream (simple file).
- Multiple **sequential** logical bitstreams (chained streams, one ends before the next begins).
- Multiple **concurrent** logical bitstreams (grouped streams, beginning of all streams before any data pages — used for audio+video multiplexing).

**Sequential-write streaming**: ✅ Fully supported — no patching needed. Each Ogg page is self-contained with its own header, CRC, and segment table; pages are written sequentially and no previously-written bytes are ever modified.

**Sequential-read streaming**: ✅ Fully supported. Pages can be demultiplexed by serial number in a single forward pass.

**Chunk interleaving (implementations)**: ✅ Yes — native to the format. `oggenc` (encoding), `ffmpeg` (via libavformat), and `libogg` all handle multi-stream interleaved Ogg files. For example, an Ogg file with concurrent Vorbis (audio) and Theora (video) streams is the standard encoding.

**Chunk interleaving (theoretical)**: ✅ Fully specified in RFC 3533. The Ogg page structure explicitly carries a stream serial number and granule position to support arbitrary interleaving.

**Conclusion for multi-stream use**: Ogg is well-suited for multi-stream interleaved streaming. The major limitation is that it is associated with multimedia codecs; using it as a general-purpose multi-stream pipe format is unusual and tooling outside the multimedia domain is sparse.

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

#### Custom / Generic Framing Formats

When no existing format is suitable, a lightweight framing protocol can be designed. Several well-known examples exist:

| Framing | Description | Streaming | Interleaving |
|---|---|---|---|
| **Netstring** | `len:data,` — trivially parseable | ✅ | ✅ (with stream tag in data) |
| **MIME multipart** | `--boundary\r\nContent-*\r\n\r\ndata` | ✅ | ❌ (boundaries require full parts) |
| **HTTP/2 framing** | 9-byte frame header with stream ID | ✅ | ✅ Native |
| **MessagePack** | Self-delimiting binary encoding | ✅ | ✅ (with envelope) |
| **CBOR sequences** (RFC 7049 / RFC 8742) | Self-delimiting binary encoding | ✅ | ✅ (with envelope) |
| **Protocol Buffers** delimited | Length-prefixed PB messages | ✅ | ✅ (with stream field) |
| **LTV (Length-Tag-Value)** | Minimal custom framing | ✅ | ✅ Native |
| **NDJSON** | Newline-delimited JSON | ✅ | ✅ (with stream field) |

A custom LTV-style framing is the simplest possible approach:
```
[4-byte stream_id][4-byte payload_len][payload_len bytes of payload]
```
This allows a reader to demultiplex any number of streams in a single forward pass with O(1) memory overhead.

---

## Higher Pipes (FD 3, 4, 5 …) Compatibility with Shells

UNIX processes inherit file descriptors from their parent. In principle, any number of FDs can be used for communication between processes connected via `pipe(2)` system calls. The question is how well **shells** support setting up such pipes for arbitrary commands.

### OS / Kernel Level

At the OS level, higher file descriptors are fully supported by all POSIX systems. There is no inherent limit preventing FD 3, 4, 5, … from being used as pipes. The `pipe(2)` system call returns two FDs (read end and write end) that can be any available integer. The `RLIMIT_NOFILE` resource limit controls the maximum number of open FDs per process (typically 1024 soft, 65536 hard on Linux), not the specific numbers.

### Shell Compatibility

#### Bash (GNU Bourne Again Shell)

**Version**: 5.x (most Linux systems, macOS via Homebrew)

**FD redirection**: ✅ Full support. `n>file`, `n<file`, `n>&m`, `n<&m` for any `n`.

**Higher-FD piping between commands**:
```bash
# Redirect FD 3 of cmd1 to FD 3 of cmd2 (not directly pipe-able with | )
# Named pipe workaround:
fd3_pipe=$(mktemp -u) && mkfifo "$fd3_pipe"
cmd1 3>"$fd3_pipe" & cmd2 3<"$fd3_pipe"
# Process substitution (creates unnamed pipes):
cmd1 > >(cmd2) 2> >(cmd3)  # redirects stdout and stderr to separate commands
# Explicit FD passing (process substitution assigns FD):
exec 3> >(cmd2)  # opens FD 3 as write end of a pipe to cmd2
cmd1 3>&3
```

**Direct `cmd1 3| cmd2` syntax**: ❌ Not supported. The `|` operator in bash only connects stdout (FD 1) of the left command to stdin (FD 0) of the right command. There is no `n|` syntax.

**coproc**: ✅ Bash 4.0+ supports `coproc name { cmd; }` which provides bidirectional pipes to a background process via `$name[0]` (read FD) and `$name[1]` (write FD). These are assigned to the first available FD numbers.

#### Zsh

**FD redirection**: ✅ Full support (same syntax as bash).

**Named pipe / process substitution**: ✅ Full support; zsh process substitution is more powerful and flexible than bash.

**Multios**: ✅ Zsh supports the `MULTIOS` option, which allows `cmd > file1 > file2` to tee output to multiple files.

**Direct higher-FD piping**: ❌ Same limitation as bash — `|` is stdout-only.

#### Fish Shell

**FD redirection**: ✅ Partial. Fish supports `cmd 2>&1` and `cmd 2>file` but uses a different syntax for some operations.

**Higher FD**: ⚠️ Limited. Fish has historically had limited support for arbitrary FD redirection. Fish 3.x added `cmd n> file` and `cmd n>&m` support but the surface area is smaller than bash/zsh.

**Named pipes / process substitution**: ⚠️ Fish does not support bash-style process substitution `<(cmd)`. Workarounds using temporary named pipes are needed.

#### ksh (Korn Shell) / mksh

**FD redirection**: ✅ Full support (bash inherited most syntax from ksh).

**coproc**: ✅ ksh introduced `|&` for bidirectional pipes to a co-process and `print -p` / `read -p` to communicate with it via FDs 3 and 4 by convention.

**Direct higher-FD piping**: ❌ Same limitation — `|` is stdout-only.

#### dash / POSIX sh

**FD redirection**: ✅ POSIX specifies `n>file`, `n<file`, `n>&m`, `n<&m`.

**Process substitution**: ❌ Not in POSIX sh; bash/zsh extension only.

**coproc**: ❌ Not in POSIX sh.

**Direct higher-FD piping**: ❌ Not in POSIX sh.

#### Nushell

**FD redirection**: ⚠️ Limited. Nushell supports `o>` (stdout to file), `e>` (stderr to file), and `o+e>` (both to file), plus `e>|` and `o+e>|` for piping stderr. However, Nushell **does not** support arbitrary higher FD manipulation — there is no `3>file`, `exec 3>file`, or `n>&m` syntax. This is a conscious design choice: Nushell focuses on structured data pipelines and cross-platform compatibility (Windows uses HANDLEs, not integer FDs). See [nushell/nushell#15650](https://github.com/nushell/nushell/issues/15650) for the open feature request.

**Higher-FD piping**: ❌ Not supported. Tools that require higher FD passing (e.g. bubblewrap `--json-status-fd`) cannot be used directly from Nushell; a bash wrapper is needed.

**Workaround**: Wrap the external command in a bash one-liner called from Nushell, or use `save` with `--stderr` for two-stream capture.

### Summary Table: Shell Higher-FD Support

| Feature | bash | zsh | fish | ksh | dash/sh | nushell |
|---|---|---|---|---|---|---|
| `n>file`, `n<file` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ (stdout/stderr only) |
| `n>&m` (FD duplication) | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `n| cmd` (pipe on FD n) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Process substitution `<(cmd)` | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| `coproc` | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| Named pipe workaround | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ Via bash wrapper |

**Key finding**: No common POSIX shell provides a `cmd1 n| cmd2` syntax to pipe FD *n* of one command directly into FD *m* of another. All higher-FD inter-process communication requires either named pipes (FIFOs), process substitution, or explicit FD manipulation combined with subshells — all of which are more complex and less portable than the simple `|` operator.

This confirms that **embedding multiple streams inside a single archive/container format piped over stdout remains the most portable and practical approach** for multi-stream CLI pipelines.

---

## Conclusions and Recommendations

### Best candidates for a multi-stream streaming pipe format

1. **MPEG-TS** — the most mature and proven format for streaming interleaved data over unreliable channels. Excellent tooling (ffmpeg, GStreamer, VLC). Fixed-size 188-byte packets simplify synchronization and recovery. Best choice if multimedia tooling integration is desired.

2. **Ogg** — well-specified open standard (RFC 3533), explicit multi-stream support, lighter weight than MPEG-TS. Good choice for non-multimedia applications due to clear, simple page structure and open specification.

3. **Custom LTV framing** — minimal overhead, simple to implement correctly, language-agnostic. Best choice for new tooling where no legacy format compatibility is required.

4. **Matroska/EBML** — richest metadata support, good tooling, but more complex than Ogg or MPEG-TS. Worth considering if multimedia integration is important.

### Non-starters for multi-stream use

- **TAR, CPIO, ar** — sequential, no interleaving.
- **ZIP** — Central Directory at end breaks streaming read.
- **7-Zip** — headers at end, no streaming.
- **RAR** — proprietary, no interleaving.

### Open questions / TBD

- TBD: Feasibility of a new minimal open spec designed specifically for general-purpose multi-stream CLI piping (working name: "mux").

---

## Appendix A: Framing Overhead Comparison

A practical concern when choosing a multi-stream container for CLI pipes is **framing overhead** — how many extra bytes per payload chunk does the format add?

| Format | Header per chunk | Payload per chunk | Overhead % | Notes |
|---|---|---|---|---|
| **LTV (custom)** | 8 bytes (4 stream_id + 4 length) | Variable (any size) | < 0.01% at 64 KiB chunks | Minimal; no checksum, no sync |
| **Ogg** | 27–282 bytes (27 fixed + 0–255 segment table) | Up to 65,025 bytes (255 × 255) | ~0.5–1% typical | Includes CRC-32 checksum; variable page size; segment table adds 1 byte per 255-byte segment |
| **MPEG-TS** | 4 bytes | 184 bytes (fixed) | ~2.13% | Fixed 188-byte packets; includes sync byte for error recovery; adaptation field may reduce payload further |
| **HTTP/2 framing** | 9 bytes | Up to 16,384 bytes (default) | ~0.05% at max frame | Well-defined; stream ID native; but designed for TCP, not pipes |
| **Netstring** | ~5–10 bytes (`len:...,`) | Variable | < 0.01% at large sizes | ASCII length prefix; simple but no stream ID built in |

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
