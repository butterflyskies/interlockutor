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

## Claiming and contention

`EventStore::claim_detailed` is the claim protocol contract. It returns
`ClaimOutcome::Granted`, `ClaimOutcome::Contended`, or `ClaimOutcome::Empty`, so
a losing claimant can tell "someone else holds this right now" apart from "there
is nothing to do", and can schedule a retry from the holder's lease expiry
instead of spinning.

`EventStore::claim` is retained as the lossy compatibility surface: it is a
projection of the same single computation, collapsing `Contended` and `Empty`
into `None`. It is not the protocol contract, and new consumers should not use
it.

Available work always wins — the scan looks for a grant across the whole topic
before reporting contention. Because `claim` does not name an `EventId`, a
contended outcome names the **lowest-sequence** live-contended event.

### Holder disclosure is a deliberate exposure

`ClaimOutcome::Contended` reveals the current lease owner's `ConsumerId` to any
caller that can attempt a claim. This is a chosen trade, appropriate to the
in-process, mutually-trusting consumer model the reference `MemoryStore` serves:
naming the holder is what makes a scheduled retry possible.

It becomes an enumeration and reconnaissance surface as soon as a durable or
networked backend serves mutually-distrusting claimants, because such a
claimant can attempt claims repeatedly to map who-holds-what. Reporting a single
lowest-sequence holder rather than the whole contention set keeps that surface
as small as the contract allows, but a backend with that threat model must gate
or redact `Contended` under its own policy. The `Authorizer` seam decides only
whether a consumer may claim a topic, not what it may learn about other
consumers.

The disclosed `holder` is a **coordination identifier, not a credential**.
Possessing another consumer's `ConsumerId` confers no authority: every lease
mutation requires an exact match on both the active fencing token and the
recorded owner, so a disclosed holder cannot be replayed to renew, acknowledge,
release, or steal a lease.

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
  into one confined, injective path component, and the original id is stored in
  the record and re-verified before any `Existing` answer.
- **A damaged record is not a receipt.** An existing path whose body is empty,
  truncated, or names a different event is reported as a distinct recoverable
  error, never as a prior acceptance.
- **The dedup is doing the work.** A negative control running the identical
  commit protocol with `EventId` keying removed genuinely records twice.

The tests do **not** establish crash durability. No in-process test can pull
power, so the fsync steps are implemented to the standard commit protocol and
argued, not demonstrated.

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
