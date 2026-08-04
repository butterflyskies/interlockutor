//! Positive conformance fixture: an out-of-crate backend that implements
//! `EventStore` with its own constructable lease type.
//!
//! Before the associated-type resolution, the only way to satisfy `EventStore`
//! from outside the crate was `todo!()` in `claim_detailed` and `renew`, because
//! [`interlockutor::Lease`] is deliberately unconstructable. This test proves the
//! seam now works: an external backend defines its own lease type, constructs it
//! freely, and passes through the full grant-renew-ack-nack lifecycle.
//!
//! The negative forgery guards are retained: `Lease`'s fields are still private
//! (`tests/ui/lease_is_unconstructable.rs`), `ClaimOutcome::Contended` still does
//! not disclose the fence (`tests/ui/contended_does_not_disclose_fence.rs`), and
//! `EventStoreExt` still cannot be substituted
//! (`tests/ui/event_store_ext_cannot_be_substituted.rs`).

use interlockutor::{
    AllowAll, AppendOutcome, Authorizer, BroadcastAck, ClaimOutcome, ConsumerId, Error, Event,
    EventId, EventStore, EventStoreExt, Fence, IdempotencyKey, NewEvent, Payload, Timestamp, Topic,
    WorkAck,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// An external backend's lease type. All fields are public because the backend
/// controls its own opacity.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ExternalLease {
    event: Event,
    owner: ConsumerId,
    fence: Fence,
    expires_at: Timestamp,
}

#[derive(Default)]
struct LeaseState {
    fence_counter: u64,
    holder: Option<(ConsumerId, Fence, Timestamp)>,
    terminal: bool,
}

#[derive(Default)]
struct StoreState {
    events: Vec<Event>,
    idem: HashMap<IdempotencyKey, Event>,
    event_ids: HashMap<EventId, IdempotencyKey>,
    cursors: HashMap<(ConsumerId, Topic), u64>,
    work: HashMap<EventId, LeaseState>,
    now: u64,
}

struct ExternalStore {
    authorizer: Arc<dyn Authorizer>,
    state: Mutex<StoreState>,
}

impl ExternalStore {
    fn new(authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            authorizer,
            state: Mutex::new(StoreState::default()),
        }
    }
}

impl EventStore for ExternalStore {
    type Lease = ExternalLease;

    fn append(&self, producer: &str, event: NewEvent) -> Result<AppendOutcome, Error> {
        if !self.authorizer.can_publish(producer, &event.topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.idem.get(&event.idempotency_key) {
            return Ok(AppendOutcome::Existing(existing.clone()));
        }
        if state.event_ids.contains_key(&event.id) {
            return Err(Error::DuplicateEventId);
        }
        let sequence = state
            .events
            .iter()
            .filter(|e| e.topic == event.topic)
            .count() as u64
            + 1;
        let stored = Event {
            id: event.id.clone(),
            topic: event.topic.clone(),
            idempotency_key: event.idempotency_key.clone(),
            payload: event.payload,
            sequence,
            appended_at: state.now,
        };
        state
            .event_ids
            .insert(stored.id.clone(), stored.idempotency_key.clone());
        state
            .idem
            .insert(stored.idempotency_key.clone(), stored.clone());
        state.work.insert(stored.id.clone(), LeaseState::default());
        state.events.push(stored.clone());
        Ok(AppendOutcome::Appended(stored))
    }

    fn read_broadcast(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        limit: usize,
    ) -> Result<Vec<Event>, Error> {
        if !self.authorizer.can_consume(consumer, topic) {
            return Err(Error::Unauthorized);
        }
        let state = self.state.lock().unwrap();
        let cursor = state
            .cursors
            .get(&(consumer.clone(), topic.clone()))
            .copied()
            .unwrap_or(0);
        Ok(state
            .events
            .iter()
            .filter(|e| e.topic == *topic && e.sequence > cursor)
            .take(limit)
            .cloned()
            .collect())
    }

    fn ack_broadcast(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        sequence: u64,
    ) -> Result<BroadcastAck, Error> {
        if !self.authorizer.can_consume(consumer, topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        let cursor = state
            .cursors
            .entry((consumer.clone(), topic.clone()))
            .or_insert(0);
        let expected = *cursor + 1;
        if sequence != expected {
            return Err(Error::OutOfOrderAck {
                expected,
                actual: sequence,
            });
        }
        *cursor = sequence;
        Ok(BroadcastAck {
            consumer: consumer.clone(),
            topic: topic.clone(),
            sequence,
            acknowledged_at: state.now,
        })
    }

    fn claim_detailed(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<ClaimOutcome<ExternalLease>, Error> {
        if !self.authorizer.can_consume(consumer, topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        let millis =
            u64::try_from(lease_for.as_millis()).map_err(|_| Error::InvalidLeaseDuration)?;
        if millis == 0 {
            return Err(Error::InvalidLeaseDuration);
        }
        let expires_at = state
            .now
            .checked_add(millis)
            .ok_or(Error::InvalidLeaseDuration)?;
        let now = state.now;
        let mut contended = None;
        let events: Vec<_> = state
            .events
            .iter()
            .filter(|e| e.topic == *topic)
            .cloned()
            .collect();
        for event in events {
            let work = state.work.get_mut(&event.id).expect("work state exists");
            if work.terminal {
                continue;
            }
            if let Some((holder, _fence, exp)) = &work.holder
                && *exp > now
            {
                contended.get_or_insert_with(|| ClaimOutcome::Contended {
                    event_id: event.id.clone(),
                    holder: holder.clone(),
                    expires_at: *exp,
                });
                continue;
            }
            // Grant: this is the key line the associated type enables.
            // An external backend can construct its own lease type.
            let next = work
                .fence_counter
                .checked_add(1)
                .ok_or(Error::InvalidLeaseDuration)?;
            work.fence_counter = next;
            let fence = Fence(next);
            work.holder = Some((consumer.clone(), fence, expires_at));
            return Ok(ClaimOutcome::Granted(ExternalLease {
                event: event.clone(),
                owner: consumer.clone(),
                fence,
                expires_at,
            }));
        }
        Ok(contended.unwrap_or(ClaimOutcome::Empty))
    }

    fn renew(&self, lease: &ExternalLease, lease_for: Duration) -> Result<ExternalLease, Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        let millis =
            u64::try_from(lease_for.as_millis()).map_err(|_| Error::InvalidLeaseDuration)?;
        if millis == 0 {
            return Err(Error::InvalidLeaseDuration);
        }
        let expires_at = state
            .now
            .checked_add(millis)
            .ok_or(Error::InvalidLeaseDuration)?;
        let now = state.now;
        let work = state
            .work
            .get_mut(&lease.event.id)
            .ok_or(Error::UnknownEvent)?;
        match &mut work.holder {
            Some((owner, fence, exp)) if *fence == lease.fence && *owner == lease.owner => {
                if *exp <= now {
                    return Err(Error::LeaseExpired);
                }
                *exp = expires_at;
                Ok(ExternalLease {
                    expires_at,
                    ..lease.clone()
                })
            }
            Some((_, fence, _)) if *fence != lease.fence => Err(Error::StaleFence),
            _ => Err(Error::NotLeaseOwner),
        }
    }

    fn ack_work(&self, lease: &ExternalLease) -> Result<WorkAck, Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        let now = state.now;
        let work = state
            .work
            .get_mut(&lease.event.id)
            .ok_or(Error::UnknownEvent)?;
        match &work.holder {
            Some((owner, fence, exp))
                if *fence == lease.fence && *owner == lease.owner && *exp > now =>
            {
                work.terminal = true;
                work.holder = None;
                Ok(WorkAck {
                    event_id: lease.event.id.clone(),
                    owner: lease.owner.clone(),
                    fence: lease.fence,
                    acknowledged_at: now,
                })
            }
            Some((_, fence, _)) if *fence != lease.fence => Err(Error::StaleFence),
            Some((_, _, exp)) if *exp <= now => Err(Error::LeaseExpired),
            _ => Err(Error::NotLeaseOwner),
        }
    }

    fn nack_work(&self, lease: &ExternalLease) -> Result<(), Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().unwrap();
        let now = state.now;
        let work = state
            .work
            .get_mut(&lease.event.id)
            .ok_or(Error::UnknownEvent)?;
        match &work.holder {
            Some((owner, fence, exp))
                if *fence == lease.fence && *owner == lease.owner && *exp > now =>
            {
                work.holder = None;
                Ok(())
            }
            Some((_, fence, _)) if *fence != lease.fence => Err(Error::StaleFence),
            Some((_, _, exp)) if *exp <= now => Err(Error::LeaseExpired),
            _ => Err(Error::NotLeaseOwner),
        }
    }
}

fn event_named(id: &str) -> NewEvent {
    NewEvent {
        id: EventId(id.into()),
        topic: Topic("work".into()),
        idempotency_key: IdempotencyKey(format!("key-{id}")),
        payload: Payload::from_bytes(id.as_bytes().to_vec()),
    }
}

fn append(store: &ExternalStore, id: &str) -> Event {
    match store.append("producer", event_named(id)).unwrap() {
        AppendOutcome::Appended(e) => e,
        _ => panic!("first append must append"),
    }
}

/// The full grant-renew-ack lifecycle through an out-of-crate backend.
#[test]
fn external_backend_grant_renew_ack() {
    let store = ExternalStore::new(Arc::new(AllowAll));
    let topic = Topic("work".into());
    let worker = ConsumerId("worker".into());
    let event = append(&store, "job-1");

    let lease = match store
        .claim_detailed(&worker, &topic, Duration::from_secs(60))
        .unwrap()
    {
        ClaimOutcome::Granted(l) => l,
        other => panic!("expected grant, got {other:?}"),
    };
    assert_eq!(lease.event, event);
    assert_eq!(lease.owner, worker);
    assert_eq!(lease.fence, Fence(1));

    let renewed = store.renew(&lease, Duration::from_secs(120)).unwrap();
    assert_eq!(renewed.fence, lease.fence);
    assert!(renewed.expires_at > lease.expires_at);

    let ack = store.ack_work(&renewed).unwrap();
    assert_eq!(ack.event_id, event.id);
    assert_eq!(ack.owner, worker);
    assert_eq!(ack.fence, Fence(1));
}

/// The nack-and-reclaim path through an out-of-crate backend.
#[test]
fn external_backend_nack_and_reclaim() {
    let store = ExternalStore::new(Arc::new(AllowAll));
    let topic = Topic("work".into());
    let worker = ConsumerId("worker".into());
    append(&store, "job-1");

    let first = match store
        .claim_detailed(&worker, &topic, Duration::from_secs(60))
        .unwrap()
    {
        ClaimOutcome::Granted(l) => l,
        other => panic!("expected grant, got {other:?}"),
    };
    assert_eq!(first.fence, Fence(1));

    store.nack_work(&first).unwrap();

    let reclaimed = match store
        .claim_detailed(&worker, &topic, Duration::from_secs(60))
        .unwrap()
    {
        ClaimOutcome::Granted(l) => l,
        other => panic!("expected grant after nack, got {other:?}"),
    };
    assert!(reclaimed.fence > first.fence);
    assert_eq!(reclaimed.fence, Fence(2));
    store.ack_work(&reclaimed).unwrap();
}

/// The blanket `EventStoreExt::claim` works through the external backend.
#[test]
fn external_backend_ext_claim_projection() {
    let store = ExternalStore::new(Arc::new(AllowAll));
    let topic = Topic("work".into());
    let worker = ConsumerId("worker".into());
    append(&store, "job-1");

    let lease: ExternalLease = store
        .claim(&worker, &topic, Duration::from_secs(60))
        .unwrap()
        .expect("claimable");
    assert_eq!(lease.fence, Fence(1));
    store.ack_work(&lease).unwrap();

    assert_eq!(
        store
            .claim(&worker, &topic, Duration::from_secs(60))
            .unwrap(),
        None
    );
}

/// Contention is reported through the external backend.
#[test]
fn external_backend_contention() {
    let store = ExternalStore::new(Arc::new(AllowAll));
    let topic = Topic("work".into());
    append(&store, "job-1");

    let worker_a = ConsumerId("a".into());
    let worker_b = ConsumerId("b".into());
    let _lease = store
        .claim(&worker_a, &topic, Duration::from_secs(60))
        .unwrap()
        .expect("a wins");

    let outcome = store
        .claim_detailed(&worker_b, &topic, Duration::from_secs(60))
        .unwrap();
    assert!(
        matches!(outcome, ClaimOutcome::Contended { .. }),
        "b should see contention"
    );
}
