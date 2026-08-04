//! A backend cannot supply its own body for the `claim` projection.
//!
//! # Why this backend is deliberately complete
//!
//! An earlier version of this guard used a bare `struct Backend;` that never
//! implemented `EventStore`. That made it **vacuous**: it failed on the
//! unsatisfied supertrait bound, which it would have done whether or not the
//! projection were protected at all. It proved nothing about substitution.
//!
//! `Backend` below implements every `EventStore` method, so the supertrait bound
//! is satisfied and the *only* thing left to reject is the second `claim` body.
//! The bodies are `todo!()` on purpose: `claim_detailed` and `renew` return
//! `Lease`, which is unconstructable outside the crate, so a diverging body is
//! the only way an out-of-crate type can satisfy this trait at all. That is the
//! documented backend-seam break, and it is not what this test is about.
//!
//! # What rejects this is coherence. The seal rejects nothing.
//!
//! The committed expectation is **E0119, conflicting implementations** — not the
//! E0277 an unsatisfied `sealed::Sealed` bound would produce:
//!
//! - `sealed::Sealed` is blanket-implemented for `T: EventStore + ?Sized`, which
//!   is the *same* bound as the blanket `EventStoreExt` impl. So `Sealed` is
//!   satisfied exactly when `EventStore` is, and excludes nothing on its own.
//! - For a type that does implement `EventStore` — the only kind that matters
//!   here — the blanket impl already covers it, so a second impl is a coherence
//!   violation before sealing is ever consulted.
//! - For a type that does not, the `EventStore` supertrait bound rejects it, and
//!   `Sealed` again adds nothing. That is the vacuous case this file replaced.
//!
//! Both directions were measured rather than reasoned about. Dropping
//! `sealed::Sealed` from the `EventStoreExt` supertrait list leaves this case,
//! and the other two, failing with byte-identical `.stderr`. Dropping the
//! blanket impl instead makes this case **compile** — trybuild reports
//! "expected test case to fail to compile, but it succeeded" — while
//! `lease_is_unconstructable.rs` keeps failing with its unchanged `E0451`, which
//! is what shows the forgery guard does not depend on any of this either.
//!
//! Sealing is a real technique and is unrelated to coherence
//! (<https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/>). This
//! file, not that link, is what fails the build if the guarantee goes away.

use interlockutor::{
    AppendOutcome, BroadcastAck, ClaimOutcome, ConsumerId, Error, Event, EventStore, EventStoreExt,
    Lease, NewEvent, Topic, WorkAck,
};
use std::time::Duration;

struct Backend;

impl EventStore for Backend {
    type Lease = Lease;

    fn append(&self, _: &str, _: NewEvent) -> Result<AppendOutcome, Error> {
        todo!()
    }

    fn read_broadcast(&self, _: &ConsumerId, _: &Topic, _: usize) -> Result<Vec<Event>, Error> {
        todo!()
    }

    fn ack_broadcast(&self, _: &ConsumerId, _: &Topic, _: u64) -> Result<BroadcastAck, Error> {
        todo!()
    }

    fn claim_detailed(
        &self,
        _: &ConsumerId,
        _: &Topic,
        _: Duration,
    ) -> Result<ClaimOutcome<Lease>, Error> {
        todo!()
    }

    fn renew(&self, _: &Lease, _: Duration) -> Result<Lease, Error> {
        todo!()
    }

    fn ack_work(&self, _: &Lease) -> Result<WorkAck, Error> {
        todo!()
    }

    fn nack_work(&self, _: &Lease) -> Result<(), Error> {
        todo!()
    }
}

// `Backend` is a fully-formed backend. This second `claim` body is the thing
// that must not compile: the documented `claim == claim_detailed().granted()`
// equivalence holds by construction only if there is exactly one body.
impl EventStoreExt for Backend {
    fn claim(&self, _: &ConsumerId, _: &Topic, _: Duration) -> Result<Option<Lease>, Error> {
        Ok(None)
    }
}

fn main() {}
