//! `ClaimOutcome::Contended` must never project the holder's active fence.
//!
//! The fence was the last field a losing claimant needed to assemble the
//! holder's lease back when `Lease` was constructible. Opacity and non-disclosure
//! are coupled, so this guard stands alongside `lease_is_unconstructable.rs`
//! rather than being covered by it.

use interlockutor::{ClaimOutcome, Fence, Lease};

fn holders_fence(outcome: &ClaimOutcome<Lease>) -> Option<Fence> {
    match outcome {
        ClaimOutcome::Contended { fence, .. } => Some(*fence),
        _ => None,
    }
}

fn main() {
    let _ = holders_fence;
}
