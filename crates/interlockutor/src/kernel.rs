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

/// The authoritative outcome of one attempted transition on a single item.
///
/// `Occupied` and `Terminal` were a single `Unavailable` variant before the
/// contended-claim work. They are kept apart because they mean different
/// things to a losing claimant: `Occupied` names a live holder worth waiting
/// for, while `Terminal` and `FenceExhausted` name work that will never be
/// claimable again. Collapsing them is what made it impossible to report a
/// current holder without also reporting stale owners of finished work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Claim<Owner> {
    Granted(LeaseRecord<Owner>),
    /// A live, unexpired lease is held; the record describes the current holder.
    Occupied(LeaseRecord<Owner>),
    /// Work was acknowledged. There is no current holder and never will be.
    Terminal,
    /// The last fence was `u64::MAX`; this item can never be leased again.
    FenceExhausted,
}

impl<Owner> Claim<Owner> {
    /// Projects the detailed outcome onto the historical `Option`-shaped one.
    ///
    /// This is the single definition of the lossy public [`crate::EventStore::claim`]
    /// surface: it exists so there is exactly one authoritative computation and
    /// one projection of it, never two claim implementations that can diverge.
    pub(super) fn granted(self) -> Option<LeaseRecord<Owner>> {
        match self {
            Self::Granted(lease) => Some(lease),
            Self::Occupied(_) | Self::Terminal | Self::FenceExhausted => None,
        }
    }

    /// Borrows the current holder, if and only if this item is live-contended.
    ///
    /// Acknowledged and fence-exhausted work has no current holder. Reporting
    /// the stale owner of finished work as a holder would be a lie the type
    /// system endorsed, so those cases are `None` by construction here rather
    /// than by discipline at each call site.
    pub(super) fn contended(&self) -> Option<&LeaseRecord<Owner>> {
        match self {
            Self::Occupied(lease) => Some(lease),
            Self::Granted(_) | Self::Terminal | Self::FenceExhausted => None,
        }
    }
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
            Phase::Acknowledged => return Claim::Terminal,
            Phase::Leased(lease) if lease.expires_at > now => {
                return Claim::Occupied(lease.clone());
            }
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

/// # The premise these proofs rest on
///
/// Every harness below reasons about a *supplied* `(owner, fence)` pair against
/// kernel state. They prove that a stale or wrong-owner token cannot mutate, and
/// that remains true. They say nothing about **where a valid token comes from**.
///
/// That unstated premise — that only the rightful holder can produce a valid
/// token — is what a holder-and-fence disclosure broke: an attacker presenting a
/// *correct* owner and fence, synthesized from data the API published, satisfies
/// every check here by construction. No kernel-level proof could have caught it.
///
/// The premise is now discharged outside this file, by the type system:
/// [`crate::Lease`] is unconstructable outside the crate and
/// [`crate::ClaimOutcome::Contended`] no longer discloses the fence. That is a
/// compile-time property, so it is covered by the trybuild UI tests in
/// `tests/ui/` rather than by a harness. State it here so nobody reads "the
/// kernel is proven" as "token provenance is proven".
///
/// Those guards were `compile_fail` doctests until they were found to be
/// unenforced: CI's runner does not execute doctests, and a pinned error code on
/// a `compile_fail` block is silently ignored on stable. See
/// `tests/compile_fail.rs`.
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
                assert_eq!(kernel.claim(2, 0, 1), Claim::Occupied(lease));
            }
            Claim::FenceExhausted => {
                assert_eq!(previous_fence, u64::MAX);
                assert_eq!(kernel, before);
            }
            Claim::Occupied(_) | Claim::Terminal => unreachable!(),
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
        assert_eq!(kernel.claim(2, 1, 2), Claim::Terminal);
        assert_eq!(
            kernel.acknowledge(&1, lease.fence, 0),
            Err(LeaseError::NotLeased)
        );
        assert_eq!(kernel, acknowledged);
    }

    /// Builds a kernel in any reachable phase, relative to `now`.
    fn any_kernel(now: Timestamp) -> LeaseKernel<u8> {
        let last_issued = Fence(kani::any::<u64>());
        let expires_at = kani::any::<u64>();
        let phase = match kani::any::<u8>() % 4 {
            0 => Phase::Available,
            1 => Phase::Acknowledged,
            2 => {
                kani::assume(expires_at > now);
                Phase::Leased(LeaseRecord {
                    owner: kani::any::<u8>(),
                    fence: last_issued,
                    expires_at,
                })
            }
            _ => {
                kani::assume(expires_at <= now);
                Phase::Leased(LeaseRecord {
                    owner: kani::any::<u8>(),
                    fence: last_issued,
                    expires_at,
                })
            }
        };
        LeaseKernel { last_issued, phase }
    }

    #[kani::proof]
    fn contention_reports_the_exact_live_holder_without_mutation() {
        let now = kani::any::<u64>();
        kani::assume(now < u64::MAX);
        let expires_at = kani::any::<u64>();
        kani::assume(expires_at > now);
        let fence = Fence(kani::any::<u64>());

        let mut kernel = LeaseKernel {
            last_issued: fence,
            phase: Phase::Leased(LeaseRecord {
                owner: 1_u8,
                fence,
                expires_at,
            }),
        };
        let before = kernel.clone();

        let outcome = kernel.claim(2_u8, now, now + 1);
        let holder = outcome.contended().expect("a live lease is contention");
        assert_eq!(holder.owner, 1);
        assert_eq!(holder.fence, fence);
        assert_eq!(holder.expires_at, expires_at);
        assert!(outcome.granted().is_none());
        assert_eq!(kernel, before);
    }

    #[kani::proof]
    fn terminal_and_exhausted_are_never_reported_as_contention() {
        let now = kani::any::<u64>();
        kani::assume(now < u64::MAX);

        let mut acknowledged = LeaseKernel::<u8> {
            last_issued: Fence(kani::any::<u64>()),
            phase: Phase::Acknowledged,
        };
        let before = acknowledged.clone();
        let outcome = acknowledged.claim(3, now, now + 1);
        assert_eq!(outcome, Claim::Terminal);
        assert!(outcome.contended().is_none());
        assert!(outcome.granted().is_none());
        assert_eq!(acknowledged, before);

        let mut exhausted = LeaseKernel::<u8> {
            last_issued: Fence(u64::MAX),
            phase: Phase::Available,
        };
        let before = exhausted.clone();
        let outcome = exhausted.claim(3, now, now + 1);
        assert_eq!(outcome, Claim::FenceExhausted);
        assert!(outcome.contended().is_none());
        assert!(outcome.granted().is_none());
        assert_eq!(exhausted, before);

        // Exhaustion whose record still names the stale owner of expired work.
        // That owner is not a current holder and must never be disclosed as one.
        let stale_expiry = kani::any::<u64>();
        kani::assume(stale_expiry <= now);
        let mut exhausted_with_stale_owner = LeaseKernel {
            last_issued: Fence(u64::MAX),
            phase: Phase::Leased(LeaseRecord {
                owner: 7_u8,
                fence: Fence(u64::MAX),
                expires_at: stale_expiry,
            }),
        };
        let before = exhausted_with_stale_owner.clone();
        let outcome = exhausted_with_stale_owner.claim(3, now, now + 1);
        assert_eq!(outcome, Claim::FenceExhausted);
        assert!(outcome.contended().is_none());
        assert!(outcome.granted().is_none());
        assert_eq!(exhausted_with_stale_owner, before);
    }

    /// The kernel-level shadow of the store's `claim == claim_detailed.granted()`
    /// compatibility invariant: one authoritative outcome, one lossy projection.
    #[kani::proof]
    fn granted_projection_agrees_with_the_detailed_outcome() {
        let now = kani::any::<u64>();
        kani::assume(now < u64::MAX);
        let mut kernel = any_kernel(now);

        let outcome = kernel.claim(9_u8, now, now + 1);
        let was_granted = matches!(outcome, Claim::Granted(_));
        assert!(!(was_granted && outcome.contended().is_some()));
        assert_eq!(outcome.granted().is_some(), was_granted);
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
