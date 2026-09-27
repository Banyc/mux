# The mux validation gate

This file is the authoritative scope of the mux scenario gate. `cargo test
-p mux` silently skips every `#[ignore]`d scenario, so the gate is defined in
tiers and the `gate-manifest` block below names every opt-in scenario and its
tier. The manifest is machine-checked by the shared checker
(`netem_test/tools/check-gate.py`, parameterized per crate), which fails if a
scenario is added or removed without the manifest being updated, making an
unnoticed `#[ignore]` skip impossible.

Run the checker after adding, removing, or re-tiering any scenario (from the
`netem_test` checkout, so the checker finds the sibling kit sources):

```sh
python3 ../netem_test/tools/check-gate.py \
  --crate . mux tests GATE.md
```

Every `mux`-over-`rtp` scenario
is asserted by the cooperation crate (`rtp_mux`) that owns the dual-lane
topology, so `rtp_mux/GATE.md` holds those tiers, manifests and gates. What
mux retains is the transport-free kit (`mux::testkit`, behind `testing`), the
mux-only test targets, the `standard`-tier long-lived-session soaks below, and
the lib unit tests that always run — in particular the memory floor below.

## Performance

The operator's product constitution is three mandates: low latency of the
interactive lane, reasonable goodput of the interactive lane without inflating
its own wire, and high goodput of the bulk lane. Each is asserted by the crate
that owns the lane it constrains. The production dual-lane topology is owned
by `rtp_mux`, so the topology-level gates — the interactive-lane tail latency
and own-wire budget, the small-stream-never-behind-bulk ordering, the loopback
bulk ceiling, and the per-stream fairness sweep — are stated and asserted in
`rtp_mux/GATE.md`. The scenarios that used to run here (`mux_over_rtp`,
`mux_over_rtp_perf`, `rtp_and_mux`, `perf_probe`, `mux_stream_fairness`,
`mux_bulk_clean_stall`, `hol_verify4`) moved to `rtp_mux/tests` with their
tiers, assertions and floors unchanged; moving them is what lets `mux` stop
knowing `rtp` at all. The kit mux exposes downward is transport-free: it wraps
an already-connected reliable byte-stream pair in a `mux` client and supervises
the session, and the cooperation crate composes it with its own transport.

### Memory floor (structural, default tier)

The idle per-stream footprint on one mux connection is gated by mux's own
lib unit test `session::idle_stream_memory_is_amortized_constant_and_budget_bounded`
(a plain `cargo test -p mux` runs it — the measured quantity is a
deterministic allocation count, not wall-clock, so it belongs in the gate
that always runs): the live bytes held while `S` idle streams are open on a
fresh warm session (both endpoints of each stream), measured with the lib's
`test_alloc` live-byte counter on a `current_thread` runtime so every
allocation lands on the driving thread. Measured band **10.7-11.6 KiB per
stream** at S = 8/32/128 with <= 1.5 % run-to-run variance; the assertions
are:

- per-stream >= **1 KiB** (floor — an open path holding no per-stream state
  would read ~0 and the ratios below would pass vacuously);
- per-stream <= **24 KiB** at 128 streams (2x the measured band; a change
  adding >= ~13 KiB of held per-stream state fails — vacuity-checked by
  holding 16 KiB per open);
- `per_stream(128) <= 3.0 x per_stream(8)` (amortized-constant: the
  measured ratio is ~0.99; a per-stream cost growing with the number of
  open streams reads >= 8 and fails the 3.0 ceiling — vacuity-checked by a
  per-open leak proportional to the prior open count).

Both vacuity mutations fail the gate naming the violated arm; the gate
guards hole 9 of `crates/AUDIT_COVERAGE.md` (memory per stream: never
measured) for the mux session table and its per-stream channels.

## Tiers

- **default** — not `#[ignore]`d, so a plain `cargo test -p mux` runs it.
  Every scenario here is seeded (deterministic impairment) and finishes in a
  few seconds. This is the gate that runs on every `cargo test`.
- **standard** — `#[ignore]`d, runs in well under a minute per target and
  asserts a correctness property (not just a measurement). Run with
  `cargo test -p mux -- --ignored --test-threads=1`.
- **full** — `#[ignore]`d, minutes per target; still asserts a property, but
  too slow for the default gate. Run the target explicitly.
- **perf** — `#[ignore]`d, report-only measurement or long-run tooling; these
  produce numbers (or feed the harness perf-loop), they do not assert a gate
  floor. A `perf` scenario must not contain an assertion in its own body;
  `check-gate.py` fails with the scenario name, its file, and the token if
  one does. It must also not reach an assertion through a helper: the checker
  derives the crate-local call-graph closure of every `perf` scenario and
  requires every asserting helper it reaches to be declared report-only in the
  `gate-perf-guard-helpers` block.

## The gate that always runs

`cargo test -p mux` runs the crate's lib unit tests (including the memory
floor above) and the mux-only non-scenario targets `stream_writer` (a mux
stream's write/close ordering and EOF semantics) and
`reassembly_stream_release` (a finished stream's table entry is released in
both wire modes; see the `reassembly_gap_family` section below). There is no
`#[ignore]`d scenario left in this crate, so the `gate-default-required` block
names the non-scenario target that must keep running in the default tier.

### Stream-read ordering integrity under out-of-order frame delivery (structural, default tier)

A transport that hands complete frames up in arrival order rather than sent
order — the receiver-side fast-forward the deployment's interactive lane runs —
is only sound while the consumer above it restores per-stream order. This crate
is that consumer, so the restoration is gated in the tier that always runs.

The lib unit test
`control::reassembly_tests::out_of_order_frame_delivery_reaches_the_reader_in_sent_order`
drives the real delivery decision (`handle_central_read`, the same function
`run_control` calls) with one stream's frames scrambled across arrival, reads
the bytes back through the real `StreamReader` the control loop hands the
application, and asserts they are exactly the bytes the sender wrote, in the
sent order. It guards the mux end of the mux↔transport frame fast-forward
coupling (in the deployment, mux over `rtp`). Vacuity: with `frame_reassembly`
off the same frames route through `MuxControl::dispatch_data`, which forwards
each body in arrival order, and the assertion fails naming the property
(arrival order `[3, 0, 7, 1, 6, 2, 5, 4]` reads back as `D, A, H, B, G, C, F,
E`). The test is a lib unit test, so the opt-in manifest above is unaffected.

```gate-default-required
reassembly_stream_release::finished_streams_leave_the_peer_table_in_both_wire_modes
session_growth_soak::the_growth_assertion_rejects_a_grown_census
session_growth_soak::the_release_assertion_rejects_a_retained_structure
```

### Interactive-path liveness under sustained concurrency (standard tier)

The audits here close *decision* defects (threshold inclusivity, equivalence,
boundary exactness); mutation sweeps cannot reach a *liveness* defect — a lost
wakeup, a staged reply cancelled by a teardown guard, a cursor blocked on
something a refusal path silently discarded. Those turn into the multi-second
interactive stall the product's M1 mandate is about, so the interactive path
has its own opt-in soak:
`interactive_liveness_soak::interactive_path_liveness_soak` drives real mux
sessions over an in-memory transport pair, many concurrent streams per cycle,
with small request/response messages interleaved with bulk transfers, writers
parked on the fair-queue reserve path, a reader or writer dropped mid-flight,
`Fin` racing pending data, and thousands of open/close cycles. It asserts byte
conservation (per-stream deterministic payload check), per-stream completion
(no stream starved) and a 2 s per-cycle bound; a cycle that never completes is
reported as a **hang** distinctly from a cycle that completes late or one that
moved but left a stream stuck. A stall verdict prints the in-flight jobs with
their staged/received byte counts, a post-stall session probe (stream-local
versus session-wide), and a 10 ms runtime heartbeat (a genuine park versus host
starvation).

Tier: **standard** (`#[ignore]`d, asserting). Measured cost on the release gate
build: **~4 s** for the default 1500 cycles (~180 MiB, ~9 000 streams opened).
`MUX_SOAK_CYCLES` and `MUX_SOAK_SEED` widen the run; `MUX_SOAK_REPLAY_TO` and
`MUX_SOAK_REPEAT` replay one cycle's schedule for reproduction.

Detection limit, stated rather than implied: a zero-hit run of N cycles
excludes a per-cycle defect rate above ~3/N at 95 % — 0.2 % per cycle at the
default 1500 cycles — for **this schedule family only**. Cycles share one
build, one host and one in-memory transport and are seeded replications of the
same schedule, not independent draws, so zero hits support an order-of-
magnitude exclusion, not a rate. Host-capacity failures are not catches: this
soak binds no sockets (no port exhaustion), and the heartbeat separates a
starved runtime from a parked task.

Coverage cells provided: liveness under sustained interactive+bulk concurrency;
the `fair_queue` scan/ready discipline and the reserve/`Pending` path; egress
rotation under a full queue; teardown (`Fin`/close/drop) racing pending data;
and stream open/close churn. Cells deliberately **not** covered: network
impairment (delay/loss/reordering live in the harness scenarios and
`rtp_mux/GATE.md`, not in this transport-free crate); `frame_reassembly = true`
(the soak uses the stock wire — the reassembly dimension is covered by the
opt-in `reassembly_gap_family` below); the real transport (mux does not depend
on it).

### Liveness in the schedule families the soak holds fixed (standard tier)

The soak's *injection schedule* is fixed by the cycle index (`cycle % 7` for
the `Fin` race, `% 11` for the dropped reader, `% 13` for the flood), so the
timing of every injection relative to a message boundary, the number of
concurrent sessions, and the control-frame races are identical across its
seeds; it varies stream counts, sizes and volume only. A green soak therefore
excludes liveness defects in one schedule family. `tests/
interactive_liveness_families.rs` adds the families it holds fixed, one
opt-in test each, asserting the same three properties (byte conservation,
per-job completion, a 2 s cycle bound with the stall reported separately from
a late cycle) and reporting a stall with the in-flight jobs and their
staged/received byte counts:

- `quiet_egress_tail_family` — one stream at a time with a cooperative quiet
gap (yields plus a 200 us sleep) between every message, so each publish is
made against a scheduler that has been given the chance to park; the same gap
precedes the tail message-plus-`shutdown` and a dropped reader. This is the
*timing* axis: no publication in the soak is phase-locked to a quiet message
boundary, and only one token is live, so no other token's push can substitute
for a lost wake. Measured cost: **5.0 s per 3 000 cycles** (1.7 ms/cycle,
dominated by the deliberate gaps); default `MUX_FAMILY_CYCLES=400` is ~0.7 s.
- `concurrent_sessions_family` — three independent mux pairs over three
duplexes in one process, so three central-I/O writers each park and must be
woken on their own; the third pair is idle on odd cycles, so a wake lost in an
idle session cannot be covered by another session's activity. Measured cost:
**2.4 s per 20 000 cycles** (0.12 ms/cycle).
- `control_race_family` — `Open`/`CloseRead`/`CloseWrite` churn racing
in-flight data: open-then-immediately-abandon (no data at all), a reader
dropped while the peer is still staging (CloseRead against data), and a writer
dropped on its own staged tail (the `Fin` against pending data, verified
byte-for-byte with a clean EOF). Measured cost: **4.1 s per 20 000 cycles**
(0.21 ms/cycle).
- `reassembly_gap_family` — the reassembly dimension, which no other family
reaches: both sessions run `frame_reassembly` on over a transport whose two
directions deliver frames out of sent order, so the reorder buffer's
gap-filling wakeup is observed at all. Shapes: interactive ping-pong
interleaved with a multi-frame bulk message on one session; a message whose
frames arrive out of order and across multi-frame gaps (up to three frames
overtaking one held frame); a `Fin` racing an in-flight message (the extended
`CloseWrite` final offset arriving while the last data frame is still
buffered); and a reader dropped mid-reassembly. Each cycle counts the
out-of-order deliveries it produced (`reorders`, `max_gap`, `close_overtakes`)
and the run fails if it produced none, so a green run cannot be vacuous.
Stream setup is inside the cycle bound as well, so an `open`/`accept` that
never completes is reported as a stall instead of hanging the runner.
Measured cost: **0.25 ms per cycle** on an idle host (400 cycles in 0.10 s;
4 000 in 0.81-1.07 s); the default `MUX_FAMILY_CYCLES=400` is ~0.1 s. On the
loaded host the release fix was verified on (load average ~40) the same family
measures ~2.0 ms per cycle (14 000 cycles in 28-30 s, 60 000 in ~123 s); the
admission ledger's two relaxed stores per open and retire are part of that, and
the two figures are not a controlled A/B.
`MUX_FAMILY_STRAND=n` is a red-proof mode, never a normal one: the n-th data
frame in each direction is never released.

All four are `#[ignore]`d under `standard`; `MUX_FAMILY_CYCLES` and
`MUX_FAMILY_SEED` widen the run. Detection limit, stated rather than implied:
a zero-hit run of N cycles excludes a per-cycle defect rate above ~3/N at
95 % — 0.75 % at this file's default 400 cycles per family, 0.015 % at a
20 000-cycle family run, and 0.002 % for `reassembly_gap_family`'s 148 000
cycles at 14 000 and 60 000 (the 32 000 green cycles at its default 400 cycles
are reported above) — and, as for the soak, the cycles share one build, one
host and one in-memory transport and are seeded replications, so the exclusion
is order-of-magnitude, not a rate.

#### `reassembly_gap_family`: stream-table release under `frame_reassembly`

At its default 400 cycles the family is green: 32 000 cycles across eight seeds
(four shapes, ~118 000 job completions). Every run of 14 000 cycles — ten of
ten, across seeds, over both the reordering transport and a plain duplex — used
to wedge the session between cycle 11 390 and 12 528: the client's `open`
succeeds but the peer's `accept` never completes, the post-stall probe hangs,
and the server's stream table has grown to `MAX_CONCURRENT_STREAMS` (8 192
entries, all peer-materialised, `local_opened == 0`), after which
`accept_peer_stream` tolerates `TooManyOpenStreams` and every later stream is
refused for the life of the session. The same shapes over the same transport
with `frame_reassembly = false` keep the table flat (it never reaches 64
entries) over 14 000 cycles, so the retention was mode-on-specific rather than a
property of the shapes.

What was retained, and why mode-on: `is_peer_write_closed` is set by the
*reassembly* paths (`MuxControl::ingest_reassembly` and
`peer_close_write_with_offset`, from the peer's final offset), not by
`MuxControl::peer_close`, and `MuxControl::local_close` / `peer_close` were the
only transitions that ran the `is_closed()` → `retire_stream()` check. A stream
whose last outstanding frame was its `CloseWrite` had no later call through
either of them, so its entry was retained for the life of the session. The
admission census the ledger samples at a refusal shows the shape of it: at the
wedge, 8 190 of the receiver's 8 192 entries were already `is_closed()`. Both
sides retained entries (the client's own table reached 3 455 entries, all
`local_opened`), so the defect is "the frame that closes the peer's write half
does not release the entry", not a property of either id space.

The release: `MuxControl::retire_if_closed` is now the one release check, and
the reassembly paths that complete the close state run it — including the
`CloseWrite` that arrives when the read side is already torn down, where no
later `local_close` can run either. The `CloseWrite` is the stream's last
frame, so this releases exactly the entries that are finished; an entry with a
live side is untouched. The cap was not changed: a table that reaches it is the
symptom of a missing release, not a capacity setting.

Measured rate: the receiver retained 0.677 peer-materialised entries per cycle
(8 192 retained at cycle 12 105, read from the admission ledger's insert/retire
gap) and 0.285 per cycle on its own opens; after the fix the same command is
green at **14 000** cycles and at **60 000** cycles on seeds 1 and 555 (148 000
cycles, ~9.5 M frames, ~1.4 M job completions), same four shapes, same
transport. Regression tests,
both failing before the fix and passing after: the lib test
`control::reassembly_tests::peer_close_write_arriving_last_releases_the_finished_entry`
drives the order the defect needs through the real close paths and asserts the
entry is released, with
`control::reassembly_tests::peer_close_write_does_not_release_a_stream_with_an_open_side`
holding the converse; `tests/reassembly_stream_release.rs` drives the same
order through two real sessions and asserts both admission ledgers drain to
their baseline, with the stock wire as the in-test control. Vacuity: with
`retire_if_closed` reduced to a no-op the lib test fails naming the retained
entry and the scenario fails at iteration 0 with
`server[table=1 local_opened=0 … inserted=1 retired=0]`.

The reproduction command above is unchanged, and is now the regression run:

```sh
MUX_FAMILY_CYCLES=14000 MUX_FAMILY_SEED=555 cargo test --release -p mux \
  --test interactive_liveness_families -- --ignored --nocapture --exact \
  reassembly_gap_family
```

A green 400-cycle run still clears the covered shapes at that length, not the
reassembly path, so the long run remains the end-to-end proof.

Red proof of the family's detector power: `MUX_FAMILY_STRAND=1` (an injected
hold that never releases a data frame) turns the family red inside the first
cycle, with the stalled jobs named by their staged/received byte counts
(`interleaved ping staged=16 received=0`). With the landed
`ReadyCounts::add` wake suppressed, the family stays **green** — 8 runs,
17 600 cycles, four seeds — so it is not a detector of that defect, and its
green runs do not certify the egress ready-mark wake.

Coverage cells provided: publish-after-park timing at message boundaries
(`Fin` and CloseRead included); one-token-at-a-time egress with no cross-token
wake substitution; several independent sessions with one idle; control frames
racing in-flight data; and the reassembly dimension — out-of-order arrival,
multi-frame gaps in the reorder buffer, the extended `CloseWrite` final offset
racing an in-flight message, a reader dropped mid-reassembly, and stream setup
liveness inside the cycle bound. Cells deliberately **not** covered, with the
reason for each empty cell: network impairment (mux is transport-free here;
delay/loss/reordering belong to the harness scenarios and `rtp_mux/GATE.md`,
and a duplex cannot produce them); byte-for-byte wire shape (a liveness
family asserts delivery, not framing); and a duplicated frame (the reorderer
reorders, it never delivers a frame twice, so the released-id window is pinned
by the default-tier lib tests in the admission section above instead). The
reassembly cursor's *framing* stays
covered by the default-tier lib test
`control::reassembly_tests::out_of_order_frame_delivery_reaches_the_reader_in_sent_order`,
which injects the out-of-order arrival directly.

### The egress ready mark carries its wake (structural, default tier)

A bounded channel's receiver waker is consumed by each delivery, so a
channel whose last poll returned a message has no waker armed until it is
polled again; the per-token channels alone therefore cannot guarantee that a
mark published after the consumer's last scan wakes it. The default-tier lib
test
`fair_queue::tests::a_ready_mark_wakes_a_parked_consumer_whose_channel_waker_is_gone`
parks a consumer on the ready set, installs a token channel with no waker
armed, publishes a mark exactly as a sender does, and asserts the parked
consumer is woken and the marked token is still deliverable. The state is
constructed white-box (the runtime interleaving that reaches it is rare
enough that only a construction pins it); vacuity: removing the `wake()`
from `ReadyCounts::add` fails the test naming the lost mark.

### Stall localisation probe (report-only, inside the soak)

`mux::live_probe` publishes the egress and ingress stage counters the
interactive-path soak prints at a stall verdict (fair-queue park census,
frames emitted and decoded, frames handled, bytes pushed into and dequeued
from a receiving stream's read queue) plus a per-stream, per-end byte ledger
(`mux::live_probe::stream_trace_report`, enabled by the soak and off by
default). It also publishes, per session role, the stream-table admission
ledger — table length, the count of streams this session opened itself (the
rest were materialised from the peer's frames), the insert/retire totals, and
a census sampled at the instant admission refuses, which separates entries
that are already fully closed from entries waiting on the peer's read close.
It asserts nothing, gates nothing and is not a scenario: it exists so that a
stall names the stage it parked in. The soak's own assertions,
bound and verdict kinds are unchanged; the counters are inside its existing
failure report, next to the heartbeat. Cost of the added instrumentation on
the measured soak: 1500 cycles still run in **4.1 s** (two runs measured,
4.08 s and 4.12 s, against the 4 s recorded above).

Two default-tier lib tests pin the same components at unit scale:
`central_io::scheduler::tests::concurrent_streams_stage_and_close_without_losing_a_byte`
(48 concurrent streams stage on the production reserve path and close while
one consumer drains; byte conservation, every `Fin`, bounded run) and
`fair_queue::tests::a_spurious_ready_mark_does_not_strand_a_later_ready_token`
(a deterministic pin of the scan-continuation: reverting the `continue` on a
spurious ready mark to `return Poll::Pending` strands the later token's message
and fails the test). Neither needs a manifest entry (lib tests are outside the
scenario targets).

### Stream-table admission (structural, default tier)

The stream table is mux's per-session admission resource: it holds one entry
per open stream, in both wire modes, and `MuxControl::open` refuses a new
stream once it holds `max_concurrent_streams` entries. The refusal is bound to
the table, not to either id space's own count, because a peer under
`frame_reassembly` materialises entries the local side never opened — the
table can be full while `local_opened_streams == 0` (measured at the wedge:
8 192 entries, `local_opened == 0`).

Two properties are gated in the tier that always runs:

- `control::reassembly_tests::a_table_full_of_peer_streams_refuses_further_opens`
  fills the table with peer-materialised streams and asserts the next local
  open *and* the next peer open are both refused while the local-id space
  guard is nowhere near firing. A change that guarded only the local count
  would admit the local open and let the table and its memory grow without
  bound.
- `control::reassembly_tests::a_fully_closed_stream_releases_its_table_entry_and_slot`
  and `control::reassembly_tests::a_peer_stream_takes_no_local_id_slot` pin
  the two halves of the counter's authority: the slot is released with the
  entry, and a peer-materialised stream never takes one. `retire_stream`
  releases the slot only for an entry the table actually held and classified
  as local, so a double retire or a retire for an unknown id cannot move the
  count that the id-space guard in `next_stream_id` uses to prove its ring
  search terminates.

- `control::reassembly_tests::a_duplicate_frame_after_a_finished_peer_stream_is_not_materialised`
  releases a peer stream (`Open`, data, both local closes, `CloseRead`,
  `CloseWrite`) and then re-delivers a data frame, a `CloseWrite`, and an
  `Open` for it, asserting none of them re-materialises the stream or hands
  the application a phantom accepted stream. The reassembly admission path
  materialises from any peer frame it does not hold — that is what lets a
  frame reordered ahead of its `Open` still arrive — so a released id needs a
  duplicate-suppression window. The window is bounded
  (`RETIRED_FINISHED_PEER_STREAM_WINDOW`, oldest-first eviction), and
  `control::reassembly_tests::the_released_finished_peer_stream_window_is_bounded`
  pins the bound and the eviction order. It is not an admission bound: a peer
  mints ids monotonically over a 2^31 ring, so a remembered id cannot be a new
  stream until the peer has minted on the order of 2^31 streams after that
  retirement, and exceeding the window only means a duplicate arriving more
  than this many finished streams later can still materialise. Vacuity:
  dropping the released-id check makes the duplicate test fail naming the
  re-materialised frame.

A refused peer admission has no caller to return an error to: the caller is
the peer's frame, so the stream cannot be materialised, its data is dropped,
and `accept_peer_stream` used to return `Ok(())` indistinguishable from an
accepted stream. The refusal is now counted in the admission ledger at the
point it is decided (`MuxControl::open` records it as a peer refusal, next to
the census of what was retained) and the first one per session is logged with
the stream id and the occupancy, so the symptom a peer sees as a stall leaves
a line naming the resource. The frame is still dropped rather than answered
with a synthetic close: the local application never asked for that stream, and
a close the peer did not open would be indistinguishable from a duplicate of
its own.

The release side of the same resource — a stream whose last outstanding frame
is the peer's `CloseWrite` — is described in the `reassembly_gap_family`
section below.

### Long-lived-session churn, retention and growth (standard tier)

`tests/session_growth_soak.rs` asks the question the two worst defects in this
workspace were found by answering — do the structures a session holds *per
stream* grow, or wedge, over a long session with heavy churn and adversarial
traffic? — on the `mux::live_probe` instrument. `frame_reassembly = true`
throughout, i.e. the mode the frame-reassembly retention defect was specific
to, over a plain in-memory duplex; the reorder buffer's *gap* dimension is not
this arm's (it is `reassembly_gap_family`'s), but every frame is ingested
into and drained out of a reorder buffer and the census must return to zero.

`a_long_lived_session_releases_every_per_stream_structure` runs 667 rounds of
a fixed eight-round phase schedule (one varying dimension per arm: a
concurrent echo round, a peer that accepts and stops reading against a bounded
read queue, streams dropped mid-transfer with the client's halves closed
first, a burst of streams written and closed but never read with the closing
order alternating by index, and a 1063 ms delivery stall), each round followed
by a full recovery probe that must complete. It samples at two matched points
(203 and 2 003 completed streams), asserts every per-stream structure reads
zero at each and does not grow between them, and asserts each session's
egress token table is empty at the end. `MUX_GROWTH_ROUNDS` and
`MUX_GROWTH_CHECKPOINT` size it; `MUX_GROWTH_FAULT=leak_stream|no_stall` are
red-proof modes.

The census (`mux::live_probe::structure_censuses`) reports, per session role,
the live count of the stream table, the reorder buffers, their pending frame
and byte totals, the open read sinks, entries `is_stream_closed` already
reports finished, and the retired-peer-id window; the egress side
(`mux::live_probe::egress_token_censuses`) reports the fair queue's per-token
queue table and ready map plus the scheduler's cached-head, token→stream and
deficit maps. The census is a table walk, so it is off unless a soak enables
it: with it off the cost is one relaxed load on the stream insert and retire
paths and on the egress consumer's poll.

Tier: **standard** (`#[ignore]`d, asserting). Measured cost on the release gate
build: **3.39 s** for the default 667 rounds (1 752 streams opened, 2 003
completed, 83 stalls, 4 580 s of simulated session time; wall cost measured at
3.32 s and 3.47 s on two more runs). Detection limit: the per-checkpoint
assertion is an exact zero, not a rate, so a single retained entry at either
matched point fails the run; the matched-point comparison additionally catches
any structure that is non-zero at both points with a larger value later.
Because the schedule is fixed by the round index, the cycles are seeded
replications of one schedule rather than independent draws.

Coverage cells provided: per-stream structure release and non-growth over a
long session (`stream_table`, reorder buffers and their pending maps, open read
sinks, retired-id window); egress token-table reaping at session scale;
recovery after a slow consumer, after streams dropped mid-transfer, after a
closed-without-reading burst, and after a delivery stall; per-round progress
rather than end-of-run totals;. Cells deliberately **not** covered: the real transport and
its impairment models (mux is transport-free); frame *reordering* (a duplex
delivers in order, so the reorder buffer holds for one poll only — the gap
dimension belongs to `reassembly_gap_family`); and per-stream task counts
(mux spawns no per-stream task: a session owns three tasks, the per-stream
resource is the token queue, which the egress census does measure).

Vacuity. Three probes, each pasted in the landing report: `retire_if_closed`
reduced to a no-op (the retention defect reintroduced) fails the first
checkpoint naming the structure and both counts (`client retains 5 reorder
buffer(s) … stream_table=5 reassembly_buffers=5 … closed_but_retained=5`);
`MUX_GROWTH_FAULT=leak_stream` fails naming 8 retained buffers;
and `MUX_GROWTH_FAULT=no_stall` fails the stall-applied check (1 645 bytes
delivered while stalled). Two default-tier tests
kept in the suite pin the assertions themselves:
`the_growth_assertion_rejects_a_grown_census` and
`the_release_assertion_rejects_a_retained_structure`.

### Session survival under latency spikes (standard tier)

The operator's client multiplexes everything over one long-lived mux session on
a path whose measured floor is ~190 ms and whose measured worst spikes are
1063 ms and 3205 ms; a spike must cost *time*, not the session, because a
teardown during a finite stall pays a cold re-establishment worse than the
spike. mux's only session liveness timer is the central reader's sliding
receive deadline (`central_io::reader::RECEIVE_DEADLINE_INTERVALS` = 4 x the
heartbeat, so 20 s at the production 5 s heartbeat).
`tests/spike_survival_soak.rs` holds one session open across 120 rounds and
stalls *delivery* in both directions — a gate that withholds reads and is
released by the test, so the advance is the only clock movement — for a fixed
schedule of 190 ms / 1063 ms / 3205 ms / 19.9 s, i.e. ~753 s of simulated
session time. It asserts per-round payload integrity, that every round
completes, that no session tore down, and that `receive_deadline_expiries` is
zero, with three non-vacuity checks on its own instrument: the gate held at
least one read and let no byte through while stalled (`held_polls`,
`delivered_while_stalled`), a heartbeat due inside each long stall was
withheld (`heartbeats_received` unchanged across the stall), and the deadline
was actually armed (`receive_deadline_sleeps_armed`, `receive_deadline_arms`,
`heartbeats_sent`/`received`). The same target's default-tier
`a_stall_past_the_deadline_still_trips_the_detector` is the positive half:
a stall past the deadline must expire and tear the session down, so the
soak's zero-expiry result cannot mean "the deadline can never fire". The
advance there is a full second past the deadline, not a millisecond:
`tokio::time::advance` does not reliably cascade a coarse timer-wheel slot on
a 1 ms step at 20 s (measured: 20.001 s -> `expiries=0`, 21 s -> `expiries=2`),
so a millisecond-exact crossing would assert the harness's granularity; the
exact boundary is pinned at a fine scale by the in-crate
`the_steady_receive_deadline_is_four_heartbeat_intervals`.

Tier: **standard** (`#[ignore]`d, asserting). Measured cost on the release
gate build: **1.12-1.20 s** for the default 120 rounds. `MUX_SPIKE_ROUNDS`
sizes the soak; `MUX_SPIKE_FAULT=no_stall` is the red-proof mode (it disables
the gate's hold and must fail the "spike was applied" checks). The schedule
is fixed, so the cycles are seeded replications of one schedule, not
independent draws: a zero-hit run of N cycles excludes a per-cycle defect rate
above ~3/N at 95 % (0.025 per cycle at 120).

Coverage cells provided: long-lived-session liveness across transport silence
at the field's own magnitudes; the receive-deadline arm/expiry ledger; and
the detector's positive control. Cells deliberately **not** covered: the real
transport and its impairment models (mux is transport-free; delay/loss belong
to the harness scenarios in `rtp_mux/GATE.md`), and the birth window
(`first_receive_deadline`), whose production value and red-proof arm live in
`rtp_mux`.

## Opt-in manifest

Each line is `target::test_name = tier`. The set must equal the set of
non-`support` tests reported by `cargo test -p mux --test <target> -- --list
--ignored`.

```gate-manifest
interactive_liveness_soak::interactive_path_liveness_soak = standard
interactive_liveness_families::quiet_egress_tail_family = standard
interactive_liveness_families::concurrent_sessions_family = standard
interactive_liveness_families::control_race_family = standard
interactive_liveness_families::reassembly_gap_family = standard
spike_survival_soak::a_live_session_survives_the_fields_spike_schedule = standard
session_growth_soak::a_long_lived_session_releases_every_per_stream_structure = standard
```

The `gate-asserting` block records the report-only/asserting split: every
`standard`/`full` scenario plus every default-tier assertion.

```gate-asserting
interactive_liveness_soak::interactive_path_liveness_soak
interactive_liveness_families::quiet_egress_tail_family
interactive_liveness_families::concurrent_sessions_family
interactive_liveness_families::control_race_family
interactive_liveness_families::reassembly_gap_family
spike_survival_soak::a_live_session_survives_the_fields_spike_schedule
session_growth_soak::a_long_lived_session_releases_every_per_stream_structure
session_growth_soak::the_growth_assertion_rejects_a_grown_census
session_growth_soak::the_release_assertion_rejects_a_retained_structure
reassembly_stream_release::finished_streams_leave_the_peer_table_in_both_wire_modes
```

## Perf-tier reach into asserting helpers

The direct-body scan only sees assertions in a `perf` scenario's own body, so
it would miss an assertion moved one call away into a helper. The
`gate-perf-guard-helpers` block records every asserting crate-local helper the
perf tier reaches. mux has no `perf` scenario, so this block is empty.

```gate-perf-guard-helpers
```

## The env-scaled opt-in surface: `MUX_FAMILY_*`, `MUX_SOAK_*`, `MUX_EGRESS_SOAK_ROUNDS`, `MUX_SPIKE_*` and `MUX_GROWTH_*`

The five `standard`-tier liveness arms above are `#[ignore]`d, but their load
shape is not fixed by the ignore set: `tests/interactive_liveness_families.rs`
and `tests/interactive_liveness_soak.rs` read it from the process environment,
so the same green arm judges a few hundred cycles or a few hundred thousand.
A default-tier lib-target component soak in `src` scales its round count the
same way. All of it is declared in the `gate-env-tier` block below; all three
rows are scriptless (`-`), because no script of this crate sets any of the
names.

### The schedule families: `MUX_FAMILY_*`

`tests/interactive_liveness_families.rs` runs four families, each a different
injection schedule over a real mux session pair on an in-memory duplex: the
quiet message-boundary timing axis, several concurrent sessions with one idle,
control-frame races against in-flight data, and frame reassembly under
out-of-order delivery. Each family asserts the soak's three properties over its
cycles — per-stream payload integrity, every job complete, every cycle inside
the 2 s bound with a never-completing cycle reported as a hang rather than a
late one — and fails if it ran no cycles; `reassembly_gap_family` additionally
fails if the reorderer delivered nothing out of order, so its own instrument
cannot be vacuous.

- `MUX_FAMILY_CYCLES` (`:139`, default `DEFAULT_CYCLES = 400`, `:74`) sizes
  each family's cycle count. Cycles are the trial unit — each is a fresh stream
  set — and the run's detection limit is the rule-of-three bound `3/N` per
  cycle.
- `MUX_FAMILY_SEED` (`:143`, default `0x5EED_2244_ABCD_0001`) seeds the
  per-cycle draw of stream counts, message lengths and shapes. It widens the
  *volume* the fixed schedule is applied to; the schedule itself is fixed by
  the cycle index, so another seed is not an independent family.
- `MUX_FAMILY_STRAND` (`:1533`, default `0`, off) is the red-proof fault
  injection: the n-th data frame in each direction is never released. A
  non-zero value must turn `reassembly_gap_family` red inside its first cycle;
  it is a detector check, and no green run uses it.

### The soak and its cycle replay: `MUX_SOAK_*`

`tests/interactive_liveness_soak.rs` is the sustained-concurrency arm: many
streams per cycle, interactive request/response interleaved with bulk
transfers, writers parked on the fair-queue reserve path, a reader or writer
dropped mid-flight, `Fin` racing pending data, thousands of open/close cycles.
It asserts the same three properties and compares the staged and received byte
totals at the end, so a silent loss fails even when every cycle returned. Four
of its variables scale it:

- `MUX_SOAK_CYCLES` (`:962`, default `DEFAULT_CYCLES = 1_500`, `:66`) is the
  soak's cost key — its whole planned cycle count. Cycles are the trial unit
  and the run's detection limit is the rule-of-three bound `3/N` per cycle.
- `MUX_SOAK_SEED` (`:971`, default `0x5EED_1234_ABCD_0001`) seeds the
  per-cycle volume draw of stream counts, round counts and message lengths. As
  for `MUX_FAMILY_SEED`, it widens the *volume* the cycle-index-fixed injection
  schedule is applied to — `cycle % 7` for the `Fin` race, `% 11` for the
  dropped reader, `% 13` for the flood, `% 5` for the extra bulk stream — so
  another seed is a different draw under the same schedule, not an independent
  one.
- `MUX_SOAK_REPLAY_TO` (`:1049`, default `0`, off) advances the schedule to
  that cycle index without running the earlier ones, so a stall seen at cycle
  k is reached without paying for the cycles before it.
- `MUX_SOAK_REPEAT` (`:1050`, default `0`, off) runs that one cycle this many
  times with the RNG state restored between repeats, so an intermittent stall
  at one schedule position is re-rolled rather than averaged over a schedule.

### The egress component soak: `MUX_EGRESS_SOAK_ROUNDS`

The egress fair-queue/scheduler path has a second, component-level soak inside
the lib target:
`central_io::scheduler::tests::concurrent_streams_stage_and_close_without_losing_a_byte`
(`src/central_io/scheduler.rs:1481`). It is the model form of the session soak
above with the transport removed: each round opens 48 concurrent streams that
stage four 8 KiB chunks on the production reserve path and then close, while
one consumer drains; it asserts byte conservation (every staged byte
dispatched), that every stream's close surfaces as a `Fin`, and that the round
finishes inside a bound. A lost wakeup on the fair queue's ready or close mark
strands bytes or the `Fin` — the multi-second interactive stall this crate's
audit programme cannot reach by mutation.

It is a `lib`-target test in the default tier, so it appears in no scenario
block and no other row covers it: `MUX_EGRESS_SOAK_ROUNDS`
(`src/central_io/scheduler.rs:1487`, default `64`) sizes its round count, and
that surface is therefore given a row of its own. All four variables above and
this one are read through a direct `std::env::var` literal, which the checker's
reader half now resolves.

### The churn/retention soak: `MUX_GROWTH_*`

`tests/session_growth_soak.rs` is the long-lived-session growth arm.
`MUX_GROWTH_ROUNDS` (`:601`, default derived from the checkpoint: `MUX_GROWTH_CHECKPOINT * 10 / 3 + 8`,
i.e. `667` at the default checkpoint) sizes the run; `MUX_GROWTH_CHECKPOINT` (`:626`, default `200`)
places the first matched point, and the second is always ten times it. The round is the trial unit,
so the rule-of-three bound is stated per round. `MUX_GROWTH_FAULT` (`:797`, default unset) selects the
red-proof modes: `leak_stream` leaks one stream's peer-side halves per round, so the release and growth
assertions must fail, and `no_stall` disables the stall phase's hold, so the stall-applied check must
fail. Both are detectors, never part of a green run; a run with a fault set fails its own last assertion
even if no property check happened to catch it, so it cannot be read as a pass.

### Cost

The families row's load is the measured default shape, not a derived one: all
four families at `MUX_FAMILY_CYCLES=400`, i.e. 4 × 400 = **1 600 cycle
executions in 2.33 s** (one `--ignored --test-threads=1` run of the target,
`finished in 2.33s`, exit 0, four summary lines; per family 2.21 s quiet,
0.02 s concurrent, 0.04 s control, 0.08 s reassembly). The run reported
9 004 completed jobs and, from the reassembly family, 3 671 out-of-order
deliveries and 542 close overtakes — the non-vacuity evidence that the reorder
path was reached. The row's `bound=7.5e-3/cycle` is the rule of three at
`MUX_FAMILY_CYCLES=400` (3/400), stated per family rather than over the summed
1 600: the four families are different schedules sharing one build and one
host, so they are not independent draws of one schedule.

The soak row's load is now the measured default shape: `MUX_SOAK_CYCLES=1500`,
i.e. 1 500 cycle executions in **4.04 s** (one `--ignored --exact` run of the
target, `finished in 4.04s`, exit 0: `cycles=1500/1500`, 9 016 streams opened,
18 032 completed jobs, `staged_bytes == received_bytes == 182 059 642`);
earlier runs of the same shape measured 4.07 and 4.12 s. Its
`total=MUX_SOAK_CYCLES` is the count the shape yields — one arm, one cycle per
unit — and `bound=2.0e-3/cycle` is the rule of three at 1 500 cycles (3/1500).
`MUX_SOAK_REPLAY_TO` and `MUX_SOAK_REPEAT` select and repeat one cycle for
reproduction and do not size the arm's cost, so the load does not name them; a
replay run's exclusion covers only the repeated cycle. `MUX_SOAK_SEED` is a
volume seed and not a count, so it is declared but is not a load factor.

The egress row's load is the measured default shape of that component soak:
`MUX_EGRESS_SOAK_ROUNDS=64`, i.e. `total=48*MUX_EGRESS_SOAK_ROUNDS` = 3 072
stream open/stage/close cycles in **0.14 s** (an `--exact` run of the lib
target, `finished in 0.14s`, exit 0). The round is the trial unit — each round
builds a fresh fair queue and fresh producers — so `bound=4.7e-2/round` is the
rule of three at 64 rounds (3/64), stated per round rather than per stream
because the 48 streams inside a round share one scheduler instance and one host
and are not independent draws.

```gate-env-tier
liveness-schedule-families = MUX_FAMILY_CYCLES,MUX_FAMILY_SEED,MUX_FAMILY_STRAND | - | the per-family liveness of the interactive path under the four schedules the soak holds fixed: quiet message-boundary publishes, three concurrent sessions with one idle, control-frame races against in-flight data, and frame-reassembly under out-of-order delivery, each asserting per-stream payload integrity, per-job completion and a 2 s per-cycle bound, with MUX_FAMILY_STRAND=n the red-proof hold that must fail the reassembly family | liveness@shape=quiet-boundary+metric=per-job-completion, liveness@shape=multi-session-idle+metric=per-job-completion, liveness@shape=control-race+metric=per-job-completion, liveness@shape=reassembly-reorder+metric=per-job-completion, payload-integrity@shape=control-race-and-reassembly+order=per-stream, liveness-rate@metric=rule-of-three+unit=cycle, stall-detection@fault=strand-never-release | MUX_FAMILY_CYCLES=400,total=4*MUX_FAMILY_CYCLES,wall=2.33s,bound=7.5e-3/cycle
interactive-path-soak = MUX_SOAK_CYCLES,MUX_SOAK_SEED,MUX_SOAK_REPLAY_TO,MUX_SOAK_REPEAT | - | the sustained interactive-path liveness soak sized by MUX_SOAK_CYCLES and volume-seeded by MUX_SOAK_SEED, with MUX_SOAK_REPLAY_TO and MUX_SOAK_REPEAT selecting one cycle index and re-running it with the RNG restored for reproduction: many streams per cycle, interactive request/response interleaved with bulk transfers, writers parked on the fair-queue reserve path, a reader or writer dropped mid-flight and Fin racing pending data, asserting staged-equals-received byte conservation, per-job completion and the 2 s per-cycle bound | liveness@shape=interactive-bulk-concurrency+metric=per-job-completion, byte-conservation@shape=soak+metric=staged-vs-received, liveness-rate@metric=rule-of-three+unit=cycle, reproduction@mode=single-cycle-replay+determinism=rng-restored, stall-localisation@shape=replayed-cycle+probe=in-flight-jobs | MUX_SOAK_CYCLES=1500,total=MUX_SOAK_CYCLES,wall=4.04s,bound=2.0e-3/cycle
egress-component-soak = MUX_EGRESS_SOAK_ROUNDS | - | the egress fair-queue/scheduler component soak in the lib target, sized by MUX_EGRESS_SOAK_ROUNDS: each round opens 48 concurrent streams that stage four 8 KiB chunks on the production reserve path and close while one consumer drains, asserting byte conservation, a Fin for every stream's close and a bounded round | liveness@shape=egress-reserve-drain+metric=per-stream-fin, byte-conservation@shape=egress-soak+metric=staged-vs-dispatched, liveness-rate@metric=rule-of-three+unit=round | MUX_EGRESS_SOAK_ROUNDS=64,total=48*MUX_EGRESS_SOAK_ROUNDS,wall=0.14s,bound=4.7e-2/round
spike-survival-soak = MUX_SPIKE_ROUNDS,MUX_SPIKE_FAULT | - | the long-lived-session spike soak sized by MUX_SPIKE_ROUNDS, with MUX_SPIKE_FAULT=no_stall the red-proof mode that disables the gate's hold: one session held open across a fixed schedule of delivery stalls (190 ms / 1063 ms / 3205 ms / 19.9 s) with the receive-deadline ledger asserted, so a spike costs time and the session survives, and the default-tier detector arm proves the deadline still fires past its window | liveness@shape=long-lived-session+metric=per-job-completion, timer-ledger@metric=receive-deadline+state=armed-not-expired, stall-detection@fault=no-stall-gate, stall-detection@control=deadline-crossed-tears-down | MUX_SPIKE_ROUNDS=120,total=MUX_SPIKE_ROUNDS,wall=1.20s,bound=2.5e-2/cycle
growth-soak = MUX_GROWTH_ROUNDS,MUX_GROWTH_CHECKPOINT,MUX_GROWTH_FAULT | - | the long-lived-session churn/retention soak sized by MUX_GROWTH_ROUNDS with its matched points placed by MUX_GROWTH_CHECKPOINT, and MUX_GROWTH_FAULT the red-proof selector (leak_stream leaks a peer half per round, no_stall disables the stall phase's hold): a frame_reassembly-on session driven through a fixed eight-round phase schedule (concurrent echo, a peer that stops reading, streams dropped mid-transfer with the client's halves closed first, a closed-without-reading burst with alternating close order, and a 1063 ms delivery stall) with a full recovery probe every round, asserting at two matched points that every per-stream structure reads zero and did not grow and that the egress token table drains | growth@metric=per-stream-structure-live-count+points=matched, release@structure=stream-table+state=closed-but-retained, release@structure=reorder-buffer+metric=pending-frames-and-bytes, release@structure=egress-token-table+metric=token-queues, liveness@shape=churn+metric=per-round-recovery-probe, liveness@shape=slow-consumer+metric=recovery, liveness@shape=dropped-mid-transfer+metric=recovery, liveness@shape=closed-without-reading+metric=recovery, liveness@shape=stall+metric=recovery, admission@metric=concurrent-streams-vs-egress-token-cap, staleness@fault=leak-stream-and-no-stall | MUX_GROWTH_CHECKPOINT=200,MUX_GROWTH_ROUNDS=667,total=MUX_GROWTH_ROUNDS,wall=3.39s,bound=4.5e-3/round
```

## Opt-in targets outside this manifest

`check-gate.py` covers only the mux scenario targets; mux has none, so the
checker only confirms the blocks above stay consistent with the compiled test
binaries. `tests/reassembly_stream_release.rs` and `tests/stream_writer.rs` are non-scenario
targets (their tests run in the default tier and are not gated as scenarios). The `nightly` bench (`mux`
with `--features nightly`, `bench::profile_mux_send`) is an infinite profiling
loop and is never run to completion by any gate. The harness crate has its own
gate (`netem_test/tests/GATE.md`) and the cooperation crate's gate is
`rtp_mux/GATE.md`.
