//! Adversarial trace: a losing claimant tries to forge the winner's lease.
//!
//! This is the regression test for the disclosure-to-forgery path. Before the
//! repair, `ClaimOutcome::Contended` reported `event_id`, `holder`, `fence`, and
//! `expires_at`, and every `Lease` field was public. A topic-authorized worker B
//! could therefore lose a claim, read the canonical event off the broadcast
//! stream, assemble worker A's `Lease` by struct literal, and terminally
//! `ack_work` A's live item before A had performed the effect.
//!
//! # What is being asserted, and how
//!
//! The forgery is now *unexpressible* rather than merely rejected: `Lease` has
//! private fields and no public constructor, so the attack does not compile.
//! Compile-time absence cannot be asserted by a runtime `assert!`, so it is split
//! in two:
//!
//! - The **non-construction** half lives in the trybuild UI tests under
//!   `tests/ui`, each with a committed `.stderr` pinning the exact diagnostic —
//!   `E0451` for the lease itself, `E0026` for the fence disclosure. They are
//!   ordinary `#[test]`s, so `cargo nextest run` executes them. They were
//!   `compile_fail` doctests, which CI never ran and which cannot pin an error
//!   code on stable; the snippets remaining in `lib.rs` are marked `ignore` and
//!   are illustration, not enforcement. See `tests/compile_fail.rs`.
//! - The **runtime** half is this file. It walks every route the public API
//!   actually offers B for reaching a `Lease` over A's item, shows each one
//!   yields no lease, and confirms the only mutation verbs B can call are the
//!   ones over B's own work. It also proves the legitimate holder is unaffected.
//!
//! B *does* end up mutating A's item once — legitimately, by reclaiming it after
//! A's lease lapses. That path is included so the test cannot pass by simply
//! being unable to do anything at all.

use interlockutor::{
    AllowAll, AppendOutcome, ClaimOutcome, Clock, ConsumerId, Error, Event, EventId, EventStore,
    EventStoreExt, IdempotencyKey, Lease, MemoryStore, NewEvent, Payload, Topic,
};
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const TOPIC: &str = "work";
const LEASE: Duration = Duration::from_millis(10);

#[derive(Default)]
struct ManualClock(AtomicU64);

impl Clock for ManualClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl ManualClock {
    fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

fn event_named(id: &str) -> NewEvent {
    NewEvent {
        id: EventId(id.into()),
        topic: Topic(TOPIC.into()),
        idempotency_key: IdempotencyKey(id.into()),
        payload: Payload::from_bytes(b"payload".to_vec()),
    }
}

fn append(store: &MemoryStore, id: &str) -> Result<Event, Box<dyn StdError>> {
    match store.append("dispatcher", event_named(id))? {
        AppendOutcome::Appended(event) => Ok(event),
        AppendOutcome::Existing(_) => Err("append should be new".into()),
    }
}

/// Every public route B has for obtaining a `Lease` over `event_id`.
///
/// Kept as one function so the test cannot silently stop covering a route: if a
/// future change adds a public way to get a lease, it belongs here.
fn every_public_route_to_a_lease(
    store: &MemoryStore,
    claimant: &ConsumerId,
    event_id: &EventId,
) -> Vec<Option<Lease>> {
    let topic = Topic(TOPIC.into());
    let detailed = store
        .claim_detailed(claimant, &topic, LEASE)
        .expect("claimant is topic-authorized");
    vec![
        // 1. the lossy compatibility surface
        store.claim(claimant, &topic, LEASE).expect("authorized"),
        // 2. the protocol contract, projected
        detailed.granted(),
        // 3. anything the claimant already legitimately holds that names this
        //    event. It holds nothing for this event, by construction.
        store
            .claim(claimant, &topic, LEASE)
            .expect("authorized")
            .filter(|lease| lease.event().id == *event_id),
    ]
}

/// B loses the claim, reads the event, and cannot reach A's lease by any route.
#[test]
fn losing_claimant_cannot_forge_the_winners_lease() -> Result<(), Box<dyn StdError>> {
    let clock = Arc::new(ManualClock::default());
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    let topic = Topic(TOPIC.into());
    let worker_a = ConsumerId("worker-a".into());
    let worker_b = ConsumerId("worker-b".into());

    let appended = append(&store, "job-1")?;
    let a_lease = store
        .claim(&worker_a, &topic, LEASE)?
        .expect("A wins the only claimable item");

    // --- B loses, and reads what the protocol tells a loser. ---
    let outcome = store.claim_detailed(&worker_b, &topic, LEASE)?;
    // Exhaustive destructuring. If a fence — or any equivalent projection of the
    // holder's ordering token — is ever added back to this variant, this stops
    // compiling. That is the intent: the shape is part of the contract.
    let ClaimOutcome::Contended {
        event_id,
        holder,
        expires_at,
    } = outcome
    else {
        return Err("B must observe live contention, not a grant or an empty topic".into());
    };
    assert_eq!(event_id, appended.id);
    assert_eq!(holder, worker_a);
    assert_eq!(expires_at, a_lease.expires_at());

    // --- B reads the canonical event. This is public, and stays public. ---
    let canonical = store.read_broadcast(&worker_b, &topic, 10)?;
    assert_eq!(canonical, vec![appended.clone()]);
    let canonical_event = canonical.into_iter().next().expect("one event");
    // B now holds every published fact about A's hold: the canonical event, the
    // holder's identity, and the expiry. Under the old shape it also held the
    // active fence, which was the last piece needed to satisfy validation.
    assert_eq!(canonical_event.id, event_id);

    // --- Route 1: build a Lease from those facts. Does not compile. ---
    // See `tests/ui/lease_is_unconstructable.rs`, whose committed `.stderr`
    // pins the `E0451` this depends on.
    // `Lease` has no public fields, no public constructor, no `Default`, and no
    // `From`/`Deserialize` impl, so there is no expression to write here.

    // --- Route 2: get one from the store. Every route yields nothing. ---
    for (index, route) in every_public_route_to_a_lease(&store, &worker_b, &event_id)
        .into_iter()
        .enumerate()
    {
        assert!(
            route.is_none(),
            "route {index} handed B a lease over contended work"
        );
    }

    // --- Route 3: use a lease B legitimately holds over *other* work. ---
    // B claims a second item, so it has a real `Lease` value in hand.
    let other = append(&store, "job-2")?;
    let b_lease = store
        .claim(&worker_b, &topic, LEASE)?
        .expect("the newly appended item is claimable");
    assert_eq!(b_lease.event().id, other.id);
    // That lease cannot be repointed at A's item: `Lease::event` is not public
    // and there is no setter, so `b_lease.event = canonical_event` does not
    // compile. Spending it acts only on B's own item.
    store.ack_work(&b_lease)?;

    // --- A is untouched by any of it. ---
    assert_eq!(store.renew(&a_lease, LEASE)?.fence(), a_lease.fence());
    let a_ack = store.ack_work(&a_lease)?;
    assert_eq!(a_ack.event_id, appended.id);
    assert_eq!(a_ack.owner, worker_a);
    assert_eq!(a_ack.fence, a_lease.fence());

    // --- The reclaim path still works, so the test is not passing vacuously. ---
    let third = append(&store, "job-3")?;
    let a_third = store
        .claim(&worker_a, &topic, LEASE)?
        .expect("job-3 is claimable");
    clock.set(clock.0.load(Ordering::SeqCst) + 10);
    let b_third = store
        .claim(&worker_b, &topic, LEASE)?
        .expect("B reclaims after A's lease lapses");
    assert_eq!(b_third.event().id, third.id);
    assert!(b_third.fence() > a_third.fence());
    // A's superseded token is now inert, which is the property the Kani proofs
    // cover. It was never the property under attack.
    assert_eq!(store.ack_work(&a_third), Err(Error::StaleFence));
    store.ack_work(&b_third)?;
    Ok(())
}

/// A lease reaches its holder intact: opacity did not cost the holder anything.
#[test]
fn a_granted_lease_still_exposes_what_its_holder_needs() -> Result<(), Box<dyn StdError>> {
    let clock = Arc::new(ManualClock::default());
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    let topic = Topic(TOPIC.into());
    let worker = ConsumerId("worker".into());
    let appended = append(&store, "job-1")?;

    let lease = store.claim(&worker, &topic, LEASE)?.expect("claimable");
    assert_eq!(*lease.event(), appended);
    assert_eq!(*lease.owner(), worker);
    assert_eq!(lease.expires_at(), 10);

    clock.set(4);
    let renewed = store.renew(&lease, Duration::from_millis(20))?;
    assert_eq!(renewed.fence(), lease.fence());
    assert_eq!(renewed.expires_at(), 24);
    assert_eq!(*renewed.event(), appended);

    // An empty topic grants nothing, so there is no lease to hand back.
    assert_eq!(
        store.claim_detailed(
            &ConsumerId("other".into()),
            &Topic("empty-topic".into()),
            LEASE
        )?,
        ClaimOutcome::Empty
    );

    // Release and reclaim: the legacy projection still hands out a real,
    // usable lease, under a strictly higher fence.
    store.nack_work(&renewed)?;
    let reclaimed = store
        .claim(&ConsumerId("other".into()), &topic, LEASE)?
        .expect("released work is claimable again");
    assert!(reclaimed.fence() > renewed.fence());
    store.ack_work(&reclaimed)?;
    Ok(())
}
