//! A losing claimant has the canonical event and the disclosed holder, and still
//! cannot build the winner's lease: every field of `Lease` is private.
//!
//! This is the guard behind the possession-as-capability design note on
//! `interlockutor::Lease`. If it ever compiles, disclosure of `(holder, fence)`
//! stops being inert data and becomes forge material again.

use interlockutor::{ConsumerId, Event, Fence, Lease};

fn forge(event: Event, holder: ConsumerId) -> Lease {
    Lease {
        event,
        owner: holder,
        fence: Fence(1),
        expires_at: u64::MAX,
    }
}

fn main() {
    let _ = forge;
}
