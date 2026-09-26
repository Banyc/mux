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

The mux layer keeps no `#[ignore]`d scenario. Every `mux`-over-`rtp` scenario
is asserted by the cooperation crate (`rtp_mux`) that owns the dual-lane
topology, so `rtp_mux/GATE.md` holds those tiers, manifests and gates. What
mux retains is the transport-free kit (`mux::testkit`, behind `testing`), the
mux-only test targets, and the lib unit tests that always run — in particular
the memory floor below.

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
floor above) and the mux-only non-scenario target `stream_writer`, whose tests
assert that a mux stream's write/close ordering and EOF semantics hold. There
is no `#[ignore]`d scenario left in this crate, so the `gate-default-required`
block is empty.

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
(the soak uses the stock wire); the real transport (mux does not depend on it).

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

All three are `#[ignore]`d under `standard`; `MUX_FAMILY_CYCLES` and
`MUX_FAMILY_SEED` widen the run. Detection limit, stated rather than implied:
a zero-hit run of N cycles excludes a per-cycle defect rate above ~3/N at
95 % — 0.05 % at this file's default 400 cycles per family, 0.0075 % at a
20 000-cycle family run — and, as for the soak, the cycles share one build,
one host and one in-memory transport and are seeded replications, so the
exclusion is order-of-magnitude, not a rate.

Coverage cells provided: publish-after-park timing at message boundaries
(`Fin` and CloseRead included); one-token-at-a-time egress with no cross-token
wake substitution; several independent sessions with one idle; and control
frames racing in-flight data. Cells deliberately **not** covered, with the
reason for each empty cell: network impairment (mux is transport-free here;
delay/loss/reordering belong to the harness scenarios and `rtp_mux/GATE.md`,
and a duplex cannot produce them); `frame_reassembly = true` (the families run
the stock wire — the reassembly cursor is covered by the default-tier lib test
`control::reassembly_tests::out_of_order_frame_delivery_reaches_the_reader_in_sent_order`,
which injects the out-of-order arrival directly, because no in-crate transport
can deliver a frame out of order to drive it here); and byte-for-byte wire
shape (a liveness family asserts delivery, not framing).

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
default). It asserts nothing, gates nothing and is not a scenario: it exists
so that a stall names the stage it parked in. The soak's own assertions,
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

## Opt-in manifest

Each line is `target::test_name = tier`. The set must equal the set of
non-`support` tests reported by `cargo test -p mux --test <target> -- --list
--ignored`.

```gate-manifest
interactive_liveness_soak::interactive_path_liveness_soak = standard
interactive_liveness_families::quiet_egress_tail_family = standard
interactive_liveness_families::concurrent_sessions_family = standard
interactive_liveness_families::control_race_family = standard
```

The `gate-asserting` block records the report-only/asserting split: every
`standard`/`full` scenario plus every default-tier assertion.

```gate-asserting
interactive_liveness_soak::interactive_path_liveness_soak
interactive_liveness_families::quiet_egress_tail_family
interactive_liveness_families::concurrent_sessions_family
interactive_liveness_families::control_race_family
```

## Perf-tier reach into asserting helpers

The direct-body scan only sees assertions in a `perf` scenario's own body, so
it would miss an assertion moved one call away into a helper. The
`gate-perf-guard-helpers` block records every asserting crate-local helper the
perf tier reaches. mux has no `perf` scenario, so this block is empty.

```gate-perf-guard-helpers
```

## Opt-in targets outside this manifest

`check-gate.py` covers only the mux scenario targets; mux has none, so the
checker only confirms the blocks above stay consistent with the compiled test
binaries. `tests/stream_writer.rs` is a non-scenario target (its tests run in
the default tier and are not gated as scenarios). The `nightly` bench (`mux`
with `--features nightly`, `bench::profile_mux_send`) is an infinite profiling
loop and is never run to completion by any gate. The harness crate has its own
gate (`netem_test/tests/GATE.md`) and the cooperation crate's gate is
`rtp_mux/GATE.md`.
