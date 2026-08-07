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

## License

Apache-2.0
