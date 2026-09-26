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
```

The `gate-asserting` block records the report-only/asserting split: every
`standard`/`full` scenario plus every default-tier assertion.

```gate-asserting
interactive_liveness_soak::interactive_path_liveness_soak
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
