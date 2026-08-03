//! Pure transition logic shared by the in-memory store and Kani.
//!
//! Keeping this kernel free of allocation, locking, and clock access makes the
//! safety contract small enough to exhaustively explore. The store remains
//! responsible for authorization, event lookup, and sampling authoritative
//! time before it asks the kernel to transition.

use crate::{Fence, Timestamp};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaseRecord<Owner> {
    pub(super) owner: Owner,
    pub(super) fence: Fence,
    pub(super) expires_at: Timestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase<Owner> {
    Available,
    Leased(LeaseRecord<Owner>),
    Acknowledged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaseKernel<Owner> {
    /// Highest fence ever issued for this item, retained between leases.
    last_issued: Fence,
    phase: Phase<Owner>,
}

impl<Owner> Default for LeaseKernel<Owner> {
    fn default() -> Self {
        Self {
            last_issued: Fence(0),
            phase: Phase::Available,
        }
    }
}

#[cfg(test)]
impl<Owner> LeaseKernel<Owner> {
    pub(super) fn available_after(fence: Fence) -> Self {
        Self {
            last_issued: fence,
            phase: Phase::Available,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Claim<Owner> {
    Granted(LeaseRecord<Owner>),
    Unavailable,
    /// The last fence was `u64::MAX`; this item can never be leased again.
    FenceExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeaseError {
    NotLeased,
    StaleFence,
    NotOwner,
    Expired,
}

impl<Owner: Clone + Eq> LeaseKernel<Owner> {
    pub(super) fn claim(
        &mut self,
        owner: Owner,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Claim<Owner> {
        debug_assert!(expires_at > now, "a new lease must expire in the future");

        match &self.phase {
            Phase::Acknowledged => return Claim::Unavailable,
            Phase::Leased(lease) if lease.expires_at > now => return Claim::Unavailable,
            Phase::Available | Phase::Leased(_) => {}
        }

        let Some(next) = self.last_issued.0.checked_add(1) else {
            return Claim::FenceExhausted;
        };
        let lease = LeaseRecord {
            owner,
            fence: Fence(next),
            expires_at,
        };
        self.last_issued = lease.fence;
        self.phase = Phase::Leased(lease.clone());
        Claim::Granted(lease)
    }

    pub(super) fn renew(
        &mut self,
        owner: &Owner,
        fence: Fence,
        now: Timestamp,
        expires_at: Timestamp,
    ) -> Result<LeaseRecord<Owner>, LeaseError> {
        debug_assert!(
            expires_at > now,
            "a renewed lease must expire in the future"
        );
        self.validate(owner, fence, now)?;

        let Phase::Leased(lease) = &mut self.phase else {
            unreachable!("validated lease disappeared")
        };
        lease.expires_at = expires_at;
        Ok(lease.clone())
    }

    pub(super) fn acknowledge(
        &mut self,
        owner: &Owner,
        fence: Fence,
        now: Timestamp,
    ) -> Result<(), LeaseError> {
        self.validate(owner, fence, now)?;
        self.phase = Phase::Acknowledged;
        Ok(())
    }

    pub(super) fn release(
        &mut self,
        owner: &Owner,
        fence: Fence,
        now: Timestamp,
    ) -> Result<(), LeaseError> {
        self.validate(owner, fence, now)?;
        self.phase = Phase::Available;
        Ok(())
    }

    pub(super) fn validate(
        &self,
        owner: &Owner,
        fence: Fence,
        now: Timestamp,
    ) -> Result<(), LeaseError> {
        let Phase::Leased(lease) = &self.phase else {
            return Err(LeaseError::NotLeased);
        };
        if lease.fence != fence {
            return Err(LeaseError::StaleFence);
        }
        if &lease.owner != owner {
            return Err(LeaseError::NotOwner);
        }
        if lease.expires_at <= now {
            return Err(LeaseError::Expired);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Replay {
    Exact,
    Conflict,
}

/// Classifies a retry after its idempotency key has located an existing event.
pub(super) fn classify_replay<Id: Eq, Topic: Eq, Payload: Eq>(
    stored: (&Id, &Topic, &Payload),
    proposed: (&Id, &Topic, &Payload),
) -> Replay {
    if stored == proposed {
        Replay::Exact
    } else {
        Replay::Conflict
    }
}

#[cfg(kani)]
mod proofs {
    use super::*;

    fn granted(owner: u8, previous_fence: u64) -> (LeaseKernel<u8>, LeaseRecord<u8>) {
        let mut kernel = LeaseKernel {
            last_issued: Fence(previous_fence),
            phase: Phase::Available,
        };
        let Claim::Granted(lease) = kernel.claim(owner, 0, 1) else {
            unreachable!()
        };
        (kernel, lease)
    }

    #[kani::proof]
    fn claim_is_exclusive_and_fences_never_wrap() {
        let previous_fence = kani::any::<u64>();
        let mut kernel = LeaseKernel {
            last_issued: Fence(previous_fence),
            phase: Phase::Available,
        };
        let before = kernel.clone();

        match kernel.claim(1_u8, 0, 1) {
            Claim::Granted(lease) => {
                assert!(previous_fence < u64::MAX);
                assert_eq!(lease.fence.0, previous_fence + 1);
                assert_eq!(kernel.claim(2, 0, 1), Claim::Unavailable);
            }
            Claim::FenceExhausted => {
                assert_eq!(previous_fence, u64::MAX);
                assert_eq!(kernel, before);
            }
            Claim::Unavailable => unreachable!(),
        }
    }

    #[kani::proof]
    fn stale_fence_and_owner_tokens_cannot_mutate_state() {
        let previous_fence = kani::any::<u64>();
        kani::assume(previous_fence < u64::MAX);
        let (mut kernel, lease) = granted(1, previous_fence);

        let before = kernel.clone();
        assert_eq!(
            kernel.renew(&1, Fence(previous_fence), 0, 2),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, before);

        assert_eq!(
            kernel.acknowledge(&1, Fence(previous_fence), 0),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, before);

        assert_eq!(
            kernel.release(&1, Fence(previous_fence), 0),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, before);

        assert_eq!(
            kernel.renew(&2, lease.fence, 0, 2),
            Err(LeaseError::NotOwner)
        );
        assert_eq!(kernel, before);

        assert_eq!(
            kernel.acknowledge(&2, lease.fence, 0),
            Err(LeaseError::NotOwner)
        );
        assert_eq!(kernel, before);

        assert_eq!(
            kernel.release(&2, lease.fence, 0),
            Err(LeaseError::NotOwner)
        );
        assert_eq!(kernel, before);
    }

    #[kani::proof]
    fn renew_preserves_owner_and_fence() {
        let previous_fence = kani::any::<u64>();
        kani::assume(previous_fence < u64::MAX);
        let (mut kernel, lease) = granted(7, previous_fence);

        let renewed = kernel.renew(&7, lease.fence, 0, 2).unwrap();
        assert_eq!(renewed.owner, lease.owner);
        assert_eq!(renewed.fence, lease.fence);
        assert_eq!(renewed.expires_at, 2);
    }

    #[kani::proof]
    fn release_requeues_with_a_higher_fence() {
        let previous_fence = kani::any::<u64>();
        kani::assume(previous_fence < u64::MAX - 1);
        let (mut kernel, first) = granted(1, previous_fence);

        assert_eq!(kernel.release(&1, first.fence, 0), Ok(()));
        let Claim::Granted(second) = kernel.claim(2, 0, 1) else {
            unreachable!()
        };
        assert_eq!(second.fence.0, first.fence.0 + 1);
        assert!(second.fence > first.fence);
    }

    #[kani::proof]
    fn release_after_the_last_fence_makes_exhaustion_terminal() {
        let (mut kernel, last) = granted(1, u64::MAX - 1);
        assert_eq!(last.fence, Fence(u64::MAX));
        assert_eq!(kernel.release(&1, last.fence, 0), Ok(()));

        let exhausted = kernel.clone();
        assert_eq!(kernel.claim(2, 0, 1), Claim::FenceExhausted);
        assert_eq!(kernel, exhausted);
    }

    #[kani::proof]
    fn lease_is_live_before_expiry_and_expired_at_the_exact_boundary() {
        let expires_at = kani::any::<u64>();
        kani::assume(expires_at > 0);
        let mut kernel = LeaseKernel {
            last_issued: Fence(1),
            phase: Phase::Leased(LeaseRecord {
                owner: 1_u8,
                fence: Fence(1),
                expires_at,
            }),
        };

        assert_eq!(kernel.validate(&1, Fence(1), expires_at - 1), Ok(()));
        let at_boundary = kernel.clone();
        assert_eq!(
            kernel.acknowledge(&1, Fence(1), expires_at),
            Err(LeaseError::Expired)
        );
        assert_eq!(kernel, at_boundary);
    }

    #[kani::proof]
    fn expired_lease_is_reclaimed_and_old_token_cannot_mutate_the_replacement() {
        let expires_at = kani::any::<u64>();
        kani::assume(expires_at > 0 && expires_at < u64::MAX);
        let mut kernel = LeaseKernel {
            last_issued: Fence(1),
            phase: Phase::Leased(LeaseRecord {
                owner: 1_u8,
                fence: Fence(1),
                expires_at,
            }),
        };

        let Claim::Granted(replacement) = kernel.claim(2, expires_at, expires_at + 1) else {
            unreachable!()
        };
        assert_eq!(replacement.fence, Fence(2));
        let after_reclaim = kernel.clone();

        assert_eq!(
            kernel.renew(&1, Fence(1), expires_at, expires_at + 1),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, after_reclaim);
        assert_eq!(
            kernel.acknowledge(&1, Fence(1), expires_at),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, after_reclaim);
        assert_eq!(
            kernel.release(&1, Fence(1), expires_at),
            Err(LeaseError::StaleFence)
        );
        assert_eq!(kernel, after_reclaim);
    }

    #[kani::proof]
    fn acknowledgment_is_terminal_and_not_idempotent() {
        let previous_fence = kani::any::<u64>();
        kani::assume(previous_fence < u64::MAX);
        let (mut kernel, lease) = granted(1, previous_fence);

        assert_eq!(kernel.acknowledge(&1, lease.fence, 0), Ok(()));
        let acknowledged = kernel.clone();
        assert_eq!(kernel.claim(2, 1, 2), Claim::Unavailable);
        assert_eq!(
            kernel.acknowledge(&1, lease.fence, 0),
            Err(LeaseError::NotLeased)
        );
        assert_eq!(kernel, acknowledged);
    }

    #[kani::proof]
    fn replay_requires_identical_content() {
        let stored = (kani::any::<u8>(), kani::any::<u8>(), kani::any::<u8>());
        let proposed = (kani::any::<u8>(), kani::any::<u8>(), kani::any::<u8>());

        let decision = classify_replay(
            (&stored.0, &stored.1, &stored.2),
            (&proposed.0, &proposed.1, &proposed.2),
        );
        assert_eq!(decision == Replay::Exact, stored == proposed);
    }
}
