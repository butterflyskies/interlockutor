//! Neutral primitives for durable broadcast delivery and leased work queues.
//!
//! The reference [`MemoryStore`] is deliberately process-local. Persistent and
//! distributed backends implement [`EventStore`] and must pass the same
//! conformance suite.
//!
//! # Migrating between minor versions
//!
//! - 0.1 to 0.2: wrap raw event bytes in [`Payload`]. [`MemoryStore::new`]
//!   accepts only an authorizer; tests and custom backends that inject a clock
//!   should use [`MemoryStore::with_clock`].
//! - 0.2 to 0.3: [`PayloadError`] and [`Error`] are exhaustive. Callers whose
//!   match already names every variant should remove an unreachable wildcard
//!   arm; intentionally partial matches need no change.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod kernel;

use kernel::{Claim, LeaseError, LeaseKernel, Replay, classify_replay};

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

/// A monotonically issued fencing token for one item.
///
/// Lease mutations require exact equality with the item's active fence. Both
/// stale lower values and unissued higher values are rejected.
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
        match self {
            Self::Unauthorized => f.write_str("operation is not authorized"),
            Self::DuplicateEventId => f.write_str("event ID is already in use"),
            Self::IdempotencyKeyConflict { key } => {
                write!(f, "idempotency key conflicts with an existing event: {}", key.0)
            }
            Self::OutOfOrderAck { expected, actual } => write!(
                f,
                "broadcast acknowledgement is out of order: expected {expected}, got {actual}"
            ),
            Self::UnknownEvent => f.write_str("event is unknown"),
            Self::NotLeaseOwner => f.write_str("caller does not own the active lease"),
            Self::StaleFence => f.write_str("fencing token does not match the active lease"),
            Self::LeaseExpired => f.write_str("lease has expired"),
            Self::InvalidLeaseDuration => f.write_str(
                "lease duration must be at least one millisecond and produce a representable expiration",
            ),
        }
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

    /// Claims the first available event.
    ///
    /// Durations are measured in whole milliseconds. Durations below one
    /// millisecond, durations outside the timestamp domain, and unrepresentable
    /// expiration times are rejected as [`Error::InvalidLeaseDuration`]. If an
    /// item's `u64` fencing-token space is exhausted, that item remains
    /// permanently unavailable and the search continues with later events.
    fn claim(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<Option<Lease>, Error>;

    /// Renews a current lease without changing its owner or fence.
    ///
    /// Duration validation is identical to [`EventStore::claim`].
    fn renew(&self, lease: &Lease, lease_for: Duration) -> Result<Lease, Error>;

    /// Makes work terminal in this store; it does not transact external effects.
    ///
    /// A successful acknowledgement is not itself retry-idempotent: calling
    /// this method again with the consumed lease returns [`Error::NotLeaseOwner`].
    /// The returned [`WorkAck`] is a freely constructible acknowledgement
    /// record, not an authenticated or durable receipt. Callers that need
    /// replay-stable acceptance must provide that guarantee at the recipient
    /// boundary before acknowledging the queue item.
    fn ack_work(&self, lease: &Lease) -> Result<WorkAck, Error>;

    /// Releases work immediately. The next claim receives a higher fence
    /// unless this lease consumed the last fencing token.
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
    work: HashMap<EventId, LeaseKernel<ConsumerId>>,
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
        let millis =
            u64::try_from(duration.as_millis()).map_err(|_| Error::InvalidLeaseDuration)?;
        if millis == 0 {
            return Err(Error::InvalidLeaseDuration);
        }
        now.checked_add(millis).ok_or(Error::InvalidLeaseDuration)
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
        state
            .work
            .get(&lease.event.id)
            .ok_or(Error::UnknownEvent)?
            .validate(&lease.owner, lease.fence, now)
            .map_err(map_lease_error)
    }
}

impl EventStore for MemoryStore {
    fn append(&self, producer: &str, event: NewEvent) -> Result<AppendOutcome, Error> {
        if !self.authorizer.can_publish(producer, &event.topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.state.lock().expect("memory store mutex poisoned");
        if let Some(existing) = state.idempotency.get(&event.idempotency_key) {
            if classify_replay(
                (&existing.id, &existing.topic, &existing.payload),
                (&event.id, &event.topic, &event.payload),
            ) == Replay::Conflict
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
        state.work.insert(stored.id.clone(), LeaseKernel::default());
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
            match work.claim(consumer.clone(), now, expires_at) {
                Claim::Granted(lease) => {
                    return Ok(Some(Lease {
                        event,
                        owner: lease.owner,
                        fence: lease.fence,
                        expires_at: lease.expires_at,
                    }));
                }
                Claim::Unavailable | Claim::FenceExhausted => continue,
            }
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
        let renewed = state
            .work
            .get_mut(&lease.event.id)
            .expect("validated lease state exists")
            .renew(&lease.owner, lease.fence, now, expires_at)
            .map_err(map_lease_error)?;
        Ok(Lease {
            owner: renewed.owner,
            fence: renewed.fence,
            expires_at: renewed.expires_at,
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
        state
            .work
            .get_mut(&lease.event.id)
            .expect("validated lease state exists")
            .acknowledge(&lease.owner, lease.fence, now)
            .expect("validated lease remains valid");
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
            .expect("validated lease state exists")
            .release(&lease.owner, lease.fence, now)
            .map_err(map_lease_error)?;
        Ok(())
    }
}

fn map_lease_error(error: LeaseError) -> Error {
    match error {
        LeaseError::NotLeased | LeaseError::NotOwner => Error::NotLeaseOwner,
        LeaseError::StaleFence => Error::StaleFence,
        LeaseError::Expired => Error::LeaseExpired,
    }
}

#[cfg(test)]
mod tests;
