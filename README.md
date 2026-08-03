# Interlockutor

Claim-once work queue and broadcast event bus with interlocking safety guarantees.

## Status

Under construction. See [tasks#131](https://github.com/butterflyskies/tasks/issues/131).

## Formal verification

The leased-work transition kernel is allocation-free and shared verbatim with
an unpublished Kani proof crate. With Kani 0.67 installed, run all harnesses:

```console
cargo kani --package=interlockutor-kani
```

The proofs cover exclusive live leases, monotonic non-wrapping fences, stale
owner and fence rejection, renewal identity, terminal acknowledgement, release
and reclaim, exact idempotent-replay classification, and — for contended claims
— that a live holder is reported exactly, that acknowledged and fence-exhausted
work is never reported as a current holder, and that the lossy `claim`
projection agrees with the detailed outcome. They do not model authorization,
locking, persistence, process restart, or transactional external effects.

They also do not model **token provenance**. Each harness reasons about a
supplied `(owner, fence)` pair; it proves a stale or wrong-owner token cannot
mutate, not that only the rightful holder can produce a valid one. That premise
is discharged by the type system instead — `Lease` is unconstructable outside
the crate — and is checked by the trybuild UI tests in
`crates/interlockutor/tests/ui`, not by a harness. Read "the kernel is proven"
as exactly that, and not as "token provenance is proven".

## Claiming and contention

`EventStore::claim_detailed` is the claim protocol contract. It returns
`ClaimOutcome::Granted`, `ClaimOutcome::Contended`, or `ClaimOutcome::Empty`, so
a losing claimant can tell "someone else holds this right now" apart from "there
is nothing to do", and can schedule a retry from the holder's lease expiry
instead of spinning.

`EventStoreExt::claim` is retained as the lossy compatibility surface: it is a
projection of the same single computation, collapsing `Contended` and `Empty`
into `None`. It is not the protocol contract, and new consumers should not use
it.

`EventStoreExt` is a blanket impl over every `EventStore`, not a provided trait
method. A backend cannot supply a second `claim` body, so the equivalence
`claim(..) == claim_detailed(..)?.granted()` holds because there is only one
implementation of it — previously that was a doc claim over an overridable
method. Callers need `EventStoreExt` in scope; backends implement `EventStore`
and get the projection for free.

**What excludes a second body is coherence, not sealing.** `EventStoreExt` does
carry a private `sealed::Sealed` supertrait, and that supertrait excludes
nothing: it is blanket-implemented over the same bound `EventStoreExt` already
requires, so it is satisfied exactly when `EventStore` is. The blanket impl is
the mechanism — it already covers every `T: EventStore`, so a second impl
overlaps it and the compiler rejects the overlap with `E0119`. Measured both
ways: removing the seal leaves all three compile-fail guards passing with
byte-identical output, and removing the blanket impl instead makes the
substitution guard compile.

This is worth keeping straight because the exclusions in this crate are
independent and easy to conflate. Lease forgery is held by `E0451` — `Lease` has
private fields and no constructor — which is a property of `Lease` alone and
holds with both the seal and the blanket impl removed. Sealing in general is a
real technique and is [unrelated to coherence][sealed-traits]; it is simply not
what is operating here.

The trybuild case pinning `E0119` is what *enforces* this; the citation only
explains it. A cited claim is verifiable but never automatically verified, and
nothing turns red when a link rots — so the committed `.stderr` is the guard,
and the footnote does not stand in for it.

Available work always wins — the scan looks for a grant across the whole topic
before reporting contention. Because `claim_detailed` takes no `EventId`, a
contended outcome names the **lowest-sequence** live-contended event. That
holder may be the calling consumer itself, when the caller already holds the
only live item in the topic.

### What a contended outcome discloses

`ClaimOutcome::Contended` reports `event_id`, `holder`, and `expires_at`. It
does **not** report the holder's active fence. Naming the holder and the expiry
is what turns a blind retry into a scheduled one; the fence is internal ordering
data a losing claimant has no use for.

Holder disclosure remains a deliberate exposure. It is an enumeration and
reconnaissance surface as soon as a durable or networked backend serves
mutually-distrusting claimants, because such a claimant can attempt claims
repeatedly to map who-holds-what. Reporting a single lowest-sequence holder
rather than the whole contention set keeps that surface as small as the contract
allows, but a backend with that threat model must gate or redact `Contended`
under its own policy. The `Authorizer` seam decides only whether a consumer may
claim a topic, not what it may learn about other consumers.

### Possession is the capability, and that depends on `Lease` being opaque

`Lease` has private fields, no public constructor, and no `Default`,
`Deserialize`, or `From` impl that rebuilds one from parts. The only ways to
obtain one are to win a claim or to renew a lease already held. Read-only
accessors (`event`, `owner`, `fence`, `expires_at`) serve the holder.

`renew`, `ack_work`, and `nack_work` authenticate the **token**, not the
**caller**. They validate the supplied lease against store state and have no
notion of who is calling. So a `Lease` is a capability: possessing one *is* the
authority to mutate that item, and handing one to another component hands over
that authority. Caller authentication distinct from token possession is
deliberately not implemented here; a backend that needs mutations bound to an
authenticated principal must add that binding itself.

**Possession-as-capability requires that the token be unconstructable.
Otherwise disclosure leaks the capability.**

That dependency is why both changes were needed, and why neither may be relaxed
alone. Validation gates on an exact match of owner, fence, and canonical event.
The event is public. When `Contended` disclosed the holder *and* the active
fence, and `Lease` fields were public, all three checks were satisfiable by a
constructed lease: a losing claimant could assemble the winner's lease and
terminally acknowledge work it never performed. Opacity and disclosure are
coupled — either the token is unconstructable, which makes `(holder, fence)`
inert data, or the fence must never be disclosed. This crate does both. Do not
re-add a public lease constructor "because the fence is not disclosed anyway",
and do not re-disclose the fence "because the lease is opaque anyway".

#### Known break: out-of-crate backends cannot implement `EventStore` right now

`claim_detailed` and `renew` return `Lease` values, and `Lease` is
unconstructable outside the crate, so a third-party backend cannot produce one.
Only in-crate backends can implement `EventStore` as of this change. That is a
real regression against the goal that persistent and distributed backends
implement the trait and pass the same conformance suite.

It is recorded rather than resolved, because every quick resolution is weaker
than it looks: a public constructor restores the forgery path; a cargo feature
is build-time role separation rather than a boundary, since features unify
across a dependency graph; a *properly* sealed minting trait — one whose private
supertrait is implemented only for named in-crate types, unlike the crate's
existing `sealed::Sealed`, which is blanket-implemented and excludes nothing —
works as a boundary and for exactly that reason excludes the party that needs
it. The shape that works is a store-bound lease, valid only against
the store that issued it, which needs a store-identity concept the crate does
not have. The seam needs a design decision, and reopening construction to
unblock an implementor before that decision would reintroduce the vulnerability.

The forgery is now unexpressible rather than merely rejected. Three trybuild UI
tests under `crates/interlockutor/tests/ui` record that, each with a committed
`.stderr` pinning the exact diagnostic, and the `lease_forgery` integration
trace walks every runtime route a losing claimant has. These were `compile_fail`
doctests, which CI never ran and which cannot pin an error code on stable; they
are ordinary `#[test]`s now, so `cargo nextest run` executes them with no extra
step. The illustrative snippets left in the docs are marked `ignore` and prove
nothing on their own — the UI tests are the guard.

## Courier dogfood contract

The `urgent_message_courier` integration trace exercises the public API as a
small courier service: a stable urgent-message event is claimed by one of two
competing couriers, accepted into a disk-backed recipient effect store,
redelivered after a lost queue acknowledgement, deduplicated at the recipient by
`EventId`, and finally acknowledged under the newer fence.

The contract is **one active courier at a time, at-least-once queue delivery,
and recipient-idempotent once-only acceptance**. Interlockutor does not promise
exactly-once delivery or effects, and this test adapter does not make the
process-local `MemoryStore` durable.

### What the trace establishes, and what it does not

Once-only acceptance is a property of the *recipient adapter*, not of the queue,
and it rests on the recipient's commit protocol: one file per `EventId`, staged
in `tmp/`, fsynced, then `hard_link`ed into `effects/` by an operation that fails
if the target exists. The link is the deduplication decision and the effect
record in a single atomic step.

The tests establish:

- **Atomic once-only acceptance under real concurrency.** A stale courier and
  the courier that superseded it are released together through a barrier across
  128 rounds; exactly one records the effect, one observes it, one record exists
  on disk. A check-then-append recipient fails this on the first round.
- **Torn writes cannot wedge redelivery.** Effects are read from the `effects/`
  *directory*, never a log tail, so a partial write — which can only ever exist
  in `tmp/` — is invisible as an effect. Stale staging files are scavenged by
  age on reopen.
- **Caller-supplied `EventId`s are never used as pathnames.** Ids are hex-encoded
  into one confined, injective path component, and the whole record — not just
  its key — is re-verified before any `Existing` answer.
- **A damaged record is not a receipt.** An existing path whose body is empty,
  truncated, names a different event, or carries a substituted `receipt_id` is
  reported as a distinct recoverable error, never as a prior acceptance. The
  check is full-record equality against the receipt the event deterministically
  produces; matching only the embedded `EventId` accepted any record filed under
  the right name.
- **A link that outlived its directory fsync is repaired before it counts.** If
  `hard_link` succeeds and the `effects/` fsync fails, the entry is visible but
  possibly not crash-durable and the attempt reports failure. The next attempt
  re-fsyncs `effects/` *before* validating, and propagates a repeated failure,
  so no courier acknowledges over an entry that was never made durable. Injected
  as a fault, not argued.
- **The keying is doing the deduplication.** Negative control A runs the
  identical commit protocol with `EventId` keying removed, on a sequential
  redelivery, and genuinely records twice. It isolates keying only — it is not a
  race control.
- **The atomicity is doing the race protection.** Negative control B is a
  check-then-append recipient whose dedup decision and effect record are two
  operations. The losing interleaving is *forced* — observe, observe, record,
  record — rather than sampled under a barrier, so it duplicates on every run.
  The same two attempts through the atomic protocol record exactly once.

The two negative controls establish different things and are not
interchangeable. Control A says nothing about concurrency; control B is a forced
interleaving rather than a sampled race.

The tests do **not** establish crash durability. No in-process test can pull
power, so the fsync steps are implemented to the standard commit protocol and
argued, not demonstrated.

### Failure semantics are stated, including the untested cells

The commit protocol was specified as a happy-path sequence, and three defects
came out of leaving its failure behaviour implied. The per-step failure
semantics — what is on disk if a step fails, what a retry observes, whether the
retry reaches a correct terminal state, and which test covers it — are now
tabulated in the `urgent_message_courier` module docs. Cells that are argued
rather than injected say so: step-1 and step-3 I/O failures, and crash
durability itself.

### Scavenging: age is a heuristic, not proof of abandonment

`tmp/` entries untouched for `stale_after` are reaped. This adapter has no
liveness marker, no advisory lock, and no ownership claim on a staging name, so
**age cannot distinguish a live attempt from debris**. An earlier version of
this document and of the code claimed a concurrent courier's staging file was
never removed. That was untrue, and the claim is now narrowed to the mechanism
rather than the mechanism strengthened to the claim.

The stated assumption is that no single accept attempt holds a staging file
longer than `stale_after` between creating it and linking it. An attempt that
violates it can have its live staging name reaped, after which its `hard_link`
fails with `NotFound`. The failure is conservative rather than corrupting — the
attempt fails loudly and records nothing, so the cost is an avoidable retry, not
a duplicated or lost effect. `EffectStore::open_with` enforces a floor on the
window; `open_exclusive` is the named escape hatch for recovery-time reaping,
where the caller supplies exclusivity instead of the heuristic inferring it.
Both directions are tested against a genuinely live stage, not simulated debris.

Unknown age is treated as no evidence in **both** directions. An entry whose
mtime is unreadable or dated in the future is never reaped — and, under a store
opened for concurrent use, never moved either. Relocating an entry takes its
staging name away from whoever owns it exactly as deleting it does, so the
owner's `hard_link` fails with `NotFound` all the same; a correct classification
paired with that action still broke a live commit. Only `open_exclusive`, where
the caller asserts that no other attempt is in flight, may move an undated entry
to `quarantine/`. A concurrent store leaves it in place and counts it.

Making this exact needs real ownership evidence — an advisory lock, a liveness
marker, or linking from an open descriptor so the pathname stops mattering.
That is deliberately out of scope here.

### `quarantine/` is visibility only, and is not evidence custody

Two things this directory does not promise, stated plainly because earlier
wording implied both:

- **It is not crash-durable.** Publication is a `hard_link` followed by an
  unlink of the original name, and neither `quarantine/` nor `tmp/` is fsynced
  afterwards. The entry is atomically *visible*, which is what makes the
  no-clobber property real, but a crash can lose the new link or leave the entry
  reachable under both names. `effects/` takes a directory fsync because a
  committed record must survive power loss. This directory takes none.
- **There is no recovery or disposition API in v1.** Nothing reads it back,
  nothing re-links an entry into `effects/`, nothing prunes it, and nothing
  bounds its size. It is indefinite, operator-owned debris — somewhere other
  than `/dev/null` for an exclusive pass to put an entry.

**The negative, because silence reads as permission:** nothing downstream may
treat `quarantine/` as evidence custody. Do not build effect preservation,
audit, or replay on it, and do not acknowledge anything because a file appeared
there. A best-effort store with no durability barrier and no recovery lifecycle
cannot carry those guarantees.

What it does promise is narrow and tested: an entry published there never
replaces one already there — publication is a no-clobber `hard_link`, not a
check-then-`rename` that could silently replace the destination — it is never
read as an effect, and it is never reaped by age.

### Durability boundary

Directory sync is a real `fsync` on unix only. There is no portable equivalent,
so on non-unix targets the call is a deliberate no-op.

- **On unix:** a committed record is atomic, visible, and fsynced, including the
  `effects/` directory entry that makes the link survive a crash. The dogfood
  tests that assert the durable once-only contract are gated to unix, because
  unix is where the mechanism behind that claim exists.
- **Everywhere else:** atomicity and visibility still hold — they come from
  `hard_link` itself — but **crash durability does not**, because the directory
  entry is never flushed. This is a strictly weaker guarantee, and it is
  untested. Do not read the unix contract as covering both.

## License

Apache-2.0

[sealed-traits]: https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/
