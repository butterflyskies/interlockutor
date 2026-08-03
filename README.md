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
and reclaim, and exact idempotent-replay classification. They do not model
authorization, locking, persistence, process restart, or transactional external
effects.

## Courier dogfood contract

The `urgent_message_courier` integration trace exercises the public API as a
small courier service: a stable urgent-message event is claimed by one of two
competing couriers, accepted into a disk-backed recipient ledger, redelivered
after a lost queue acknowledgement, deduplicated at the recipient by `EventId`,
and finally acknowledged under the newer fence.

The contract is **one active courier at a time, at-least-once queue delivery,
and recipient-idempotent once-only acceptance**. Interlockutor does not promise
exactly-once delivery or effects, and this test adapter does not make the
process-local `MemoryStore` durable.

## License

Apache-2.0
