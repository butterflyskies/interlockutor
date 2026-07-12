//! Neutral primitives for durable broadcast delivery and leased work queues.
//!
//! The reference [`MemoryStore`] is deliberately process-local. Persistent and
//! distributed backends implement [`EventStore`] and must pass the same
//! conformance suite.
//!
//! # Migrating from 0.1
//!
//! Version 0.2 replaces raw `Vec<u8>` event payloads with [`Payload`] and
//! changes [`MemoryStore::new`] to accept only an authorizer. Tests and custom
//! backends that inject a clock should use [`MemoryStore::with_clock`].

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Monotonic time in milliseconds from an implementation-defined epoch.
pub type Timestamp = u64;

/// A stable identifier supplied by the producer.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId(pub String);

/// A routing name. Ordering is guaranteed independently within each topic.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Topic(pub String);

/// An opaque consumer identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerId(pub String);

/// An idempotency key stable across producer retries.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(pub String);

/// Opaque event data.
///
/// Producers can supply already-encoded bytes with [`Payload::from_bytes`] or
/// serialize a value as JSON with [`Payload::json`]. Interlockutor deliberately
/// does not prescribe one encoding for every event in a topic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Payload(Vec<u8>);

impl Payload {
    /// Wraps an encoded payload without changing its bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Serializes a value as JSON.
    pub fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self, PayloadError> {
        serde_json::to_vec(value)
            .map(Self)
            .map_err(PayloadError::Json)
    }

    /// Borrows the encoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the owned encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_bytes(bytes)
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Failure to encode a typed value as an event payload.
#[derive(Debug)]
#[non_exhaustive]
pub enum PayloadError {
    Json(serde_json::Error),
}

impl fmt::Display for PayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "failed to serialize payload as JSON: {error}"),
        }
    }
}

impl std::error::Error for PayloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
        }
    }
}

/// A producer-authored event before it is appended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewEvent {
    pub id: EventId,
    pub topic: Topic,
    pub idempotency_key: IdempotencyKey,
    pub payload: Payload,
}

/// An event assigned a per-topic sequence by the store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub id: EventId,
    pub topic: Topic,
    pub idempotency_key: IdempotencyKey,
    pub payload: Payload,
    /// One-based, contiguous, and monotonic within `topic`.
    pub sequence: u64,
    pub appended_at: Timestamp,
}

/// Result of an idempotent append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Appended(Event),
    Existing(Event),
}

/// Receipt proving that one broadcast cursor advanced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BroadcastAck {
    pub consumer: ConsumerId,
    pub topic: Topic,
    pub sequence: u64,
    pub acknowledged_at: Timestamp,
}

/// A fencing token. A higher value supersedes every lower value for the item.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Fence(pub u64);

/// A temporary, renewable right to execute a work item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub event: Event,
    pub owner: ConsumerId,
    pub fence: Fence,
    pub expires_at: Timestamp,
}

/// Final acknowledgement of work under a current lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkAck {
    pub event_id: EventId,
    pub owner: ConsumerId,
    pub fence: Fence,
    pub acknowledged_at: Timestamp,
}

#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Unauthorized,
    DuplicateEventId,
    IdempotencyKeyConflict { key: IdempotencyKey },
    OutOfOrderAck { expected: u64, actual: u64 },
    UnknownEvent,
    NotLeaseOwner,
    StaleFence,
    LeaseExpired,
    InvalidLeaseDuration,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}

/// Injected monotonic clock. Backends must not read wall time directly.
///
/// [`MemoryStore`] samples its clock while holding the state lock so lease
/// decisions use the authoritative time at the state transition. Clock
/// implementations must therefore be fast and must not re-enter the store.
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// Process-monotonic clock used by the reference store.
///
/// Timestamps are milliseconds since this clock was created, not wall-clock
/// timestamps and not values clients should generate or compare remotely.
#[derive(Debug)]
struct MonotonicClock {
    origin: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> Timestamp {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }
}

/// Policy seam. The core assigns no meaning to principals or topics.
pub trait Authorizer: Send + Sync {
    fn can_publish(&self, producer: &str, topic: &Topic) -> bool;
    fn can_consume(&self, consumer: &ConsumerId, topic: &Topic) -> bool;
}

/// Permissive policy useful for local and conformance testing.
#[derive(Debug, Default)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn can_publish(&self, _: &str, _: &Topic) -> bool {
        true
    }
    fn can_consume(&self, _: &ConsumerId, _: &Topic) -> bool {
        true
    }
}

/// Storage contract shared by local and future durable implementations.
///
/// Delivery is at-least-once. Effects performed by consumers must therefore be
/// idempotent. Ordering is guaranteed per topic, never globally.
///
/// This synchronous trait is the local/reference contract. Network adapters
/// should expose their own asynchronous API rather than blocking an async
/// runtime behind this trait. The central store owns lease time; distributed
/// clients request durations but never supply timestamps or clocks. Retention
/// and compaction are intentionally not part of the MVP contract; the reference
/// store is unbounded.
pub trait EventStore: Send + Sync {
    fn append(&self, producer: &str, event: NewEvent) -> Result<AppendOutcome, Error>;

    fn read_broadcast(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        limit: usize,
    ) -> Result<Vec<Event>, Error>;

    /// Advances only over the next event, making cursor gaps explicit.
    fn ack_broadcast(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        sequence: u64,
    ) -> Result<BroadcastAck, Error>;

    fn claim(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<Option<Lease>, Error>;

    fn renew(&self, lease: &Lease, lease_for: Duration) -> Result<Lease, Error>;
    fn ack_work(&self, lease: &Lease) -> Result<WorkAck, Error>;

    /// Releases work immediately. The next claim receives a higher fence.
    fn nack_work(&self, lease: &Lease) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct MemoryStore {
    clock: Arc<dyn Clock>,
    authorizer: Arc<dyn Authorizer>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    topics: BTreeMap<Topic, Vec<Event>>,
    idempotency: HashMap<IdempotencyKey, Event>,
    event_ids: HashMap<EventId, IdempotencyKey>,
    cursors: HashMap<(ConsumerId, Topic), u64>,
    work: HashMap<EventId, WorkState>,
}

#[derive(Clone, Debug, Default)]
struct WorkState {
    fence: u64,
    lease: Option<LeaseState>,
    acknowledged: bool,
}

#[derive(Clone, Debug)]
struct LeaseState {
    owner: ConsumerId,
    fence: Fence,
    expires_at: Timestamp,
}

impl MemoryStore {
    /// Creates a store with a process-monotonic clock and explicit policy.
    pub fn new(authorizer: Arc<dyn Authorizer>) -> Self {
        Self::with_clock(Arc::new(MonotonicClock::default()), authorizer)
    }

    /// Creates a store with an injected clock, primarily for deterministic tests.
    pub fn with_clock(clock: Arc<dyn Clock>, authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            clock,
            authorizer,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn expiry_from(now: Timestamp, duration: Duration) -> Result<Timestamp, Error> {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        if millis == 0 {
            return Err(Error::InvalidLeaseDuration);
        }
        Ok(now.saturating_add(millis))
    }

    fn validate_lease(state: &State, lease: &Lease, now: Timestamp) -> Result<(), Error> {
        let key = state
            .event_ids
            .get(&lease.event.id)
            .ok_or(Error::UnknownEvent)?;
        let canonical = state.idempotency.get(key).ok_or(Error::UnknownEvent)?;
        if canonical != &lease.event {
            return Err(Error::UnknownEvent);
        }
        let work = state.work.get(&lease.event.id).ok_or(Error::UnknownEvent)?;
        let active = work.lease.as_ref().ok_or(Error::NotLeaseOwner)?;
        if active.fence != lease.fence {
            return Err(Error::StaleFence);
        }
        if active.owner != lease.owner {
            return Err(Error::NotLeaseOwner);
        }
        if active.expires_at <= now {
            return Err(Error::LeaseExpired);
        }
        Ok(())
    }
}

impl EventStore for MemoryStore {
    fn append(&self, producer: &str, event: NewEvent) -> Result<AppendOutcome, Error> {
        if !self.authorizer.can_publish(producer, &event.topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        if let Some(existing) = state.idempotency.get(&event.idempotency_key) {
            if existing.id != event.id
                || existing.topic != event.topic
                || existing.payload != event.payload
            {
                return Err(Error::IdempotencyKeyConflict {
                    key: event.idempotency_key,
                });
            }
            return Ok(AppendOutcome::Existing(existing.clone()));
        }
        if state.event_ids.contains_key(&event.id) {
            return Err(Error::DuplicateEventId);
        }
        let sequence = state
            .topics
            .get(&event.topic)
            .map_or(1, |events| events.len() as u64 + 1);
        let stored = Event {
            id: event.id.clone(),
            topic: event.topic.clone(),
            idempotency_key: event.idempotency_key.clone(),
            payload: event.payload,
            sequence,
            appended_at: self.clock.now(),
        };
        state
            .event_ids
            .insert(stored.id.clone(), stored.idempotency_key.clone());
        state
            .idempotency
            .insert(stored.idempotency_key.clone(), stored.clone());
        state.work.insert(stored.id.clone(), WorkState::default());
        state
            .topics
            .entry(stored.topic.clone())
            .or_default()
            .push(stored.clone());
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
        let state = self.state.lock().expect("memory store mutex poisoned");
        let cursor = state
            .cursors
            .get(&(consumer.clone(), topic.clone()))
            .copied()
            .unwrap_or(0);
        Ok(state
            .topics
            .get(topic)
            .into_iter()
            .flatten()
            .filter(|e| e.sequence > cursor)
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
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        let exists = state
            .topics
            .get(topic)
            .is_some_and(|events| events.iter().any(|e| e.sequence == sequence));
        if !exists {
            return Err(Error::UnknownEvent);
        }
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
            acknowledged_at: self.clock.now(),
        })
    }

    fn claim(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<Option<Lease>, Error> {
        if !self.authorizer.can_consume(consumer, topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        let now = self.clock.now();
        let expires_at = Self::expiry_from(now, lease_for)?;
        let events = state.topics.get(topic).cloned().unwrap_or_default();
        for event in events {
            let work = state
                .work
                .get_mut(&event.id)
                .expect("work state exists for event");
            if work.acknowledged {
                continue;
            }
            if work
                .lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > now)
            {
                continue;
            }
            work.fence = work.fence.checked_add(1).expect("fencing token exhausted");
            let fence = Fence(work.fence);
            work.lease = Some(LeaseState {
                owner: consumer.clone(),
                fence,
                expires_at,
            });
            return Ok(Some(Lease {
                event,
                owner: consumer.clone(),
                fence,
                expires_at,
            }));
        }
        Ok(None)
    }

    fn renew(&self, lease: &Lease, lease_for: Duration) -> Result<Lease, Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        let now = self.clock.now();
        let expires_at = Self::expiry_from(now, lease_for)?;
        Self::validate_lease(&state, lease, now)?;
        state
            .work
            .get_mut(&lease.event.id)
            .expect("validated")
            .lease
            .as_mut()
            .expect("validated")
            .expires_at = expires_at;
        Ok(Lease {
            expires_at,
            ..lease.clone()
        })
    }

    fn ack_work(&self, lease: &Lease) -> Result<WorkAck, Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        let now = self.clock.now();
        Self::validate_lease(&state, lease, now)?;
        let work = state.work.get_mut(&lease.event.id).expect("validated");
        work.acknowledged = true;
        work.lease = None;
        Ok(WorkAck {
            event_id: lease.event.id.clone(),
            owner: lease.owner.clone(),
            fence: lease.fence,
            acknowledged_at: now,
        })
    }

    fn nack_work(&self, lease: &Lease) -> Result<(), Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        let now = self.clock.now();
        Self::validate_lease(&state, lease, now)?;
        state
            .work
            .get_mut(&lease.event.id)
            .expect("validated")
            .lease = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
