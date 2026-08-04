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
//! - 0.3 to 0.4: [`Error`] gained [`Error::StorePoisoned`]. Because `Error` is
//!   exhaustive by the previous entry, a match written against 0.3 that names
//!   every variant stops compiling until it names this one too. Reaching it
//!   requires an injected [`Clock`] whose `now` panics — see the variant's docs
//!   for why it is a typed fail-stop rather than a panic, and why authorization
//!   is still answered ahead of it.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod kernel;

use kernel::{LeaseError, LeaseKernel, Replay, classify_replay};

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
///
/// # Unforgeable outside this crate
///
/// Every field is private and there is no public constructor, no `Default`, no
/// `Deserialize`, and no `From` impl that rebuilds one from parts. The only ways
/// to obtain a `Lease` are to win [`EventStore::claim_detailed`] (or its lossy
/// [`EventStoreExt::claim`] projection) and to [`EventStore::renew`] one already
/// held. A caller cannot assemble a lease out of data the API published about
/// somebody else's hold.
///
/// This is what makes the disclosure on [`ClaimOutcome::Contended`] safe. Before
/// it, a losing claimant could read the canonical [`Event`], take the disclosed
/// holder and fence, build the winner's lease by struct literal, and terminally
/// acknowledge work it never performed.
///
/// # Design position: possession is the capability, and it depends on opacity
///
/// [`EventStore::renew`], [`EventStore::ack_work`], and [`EventStore::nack_work`]
/// authenticate the **token**, not the **caller**. They validate the supplied
/// lease against store state; they have no notion of who is calling, and the
/// [`Authorizer`] seam decides only whether a consumer may touch a topic at all.
/// So a `Lease` is a capability: possessing one *is* the authority to mutate that
/// item, and handing one to another component hands over that authority.
///
/// **Possession-as-capability requires that the token be unconstructable.
/// Otherwise disclosure leaks the capability.**
///
/// That dependency is the whole point of this section. Validation gates on an
/// exact match of owner, fence, and canonical event. The event is public. When
/// `Contended` disclosed both the holder and the active fence, and `Lease` fields
/// were public, all three checks were satisfiable by a *constructed* lease — the
/// disclosed fence was the forge material. Opacity and disclosure are therefore
/// **coupled**, not independent: either the token is unconstructable, which makes
/// `(holder, fence)` inert data, or the fence must never be disclosed.
///
/// This crate does both, deliberately. Do not relax one on the grounds that the
/// other covers it. Re-adding a public constructor "because the fence is not
/// disclosed anyway" reopens the hole, and so does re-disclosing the fence
/// "because the lease is opaque anyway".
///
/// Caller authentication distinct from token possession is intentionally **not**
/// implemented here. A backend whose threat model needs mutations bound to an
/// authenticated principal — rather than to whoever holds the token — must add
/// that binding itself.
///
/// # Cloning and transfer are deliberate, and the store is the arbiter
///
/// `Lease` is [`Clone`], and that is intentional rather than an oversight of
/// the opacity work. Unconstructability stops a *non*-holder from manufacturing
/// a token; it says nothing about what a legitimate holder may do with the one
/// it was granted. Spelled out, because "unforgeable" is easy to misread as
/// "unique":
///
/// - **Every clone is the same capability, not a copy of a lesser one.**
///   Validation compares owner, fence, and canonical event. Clones are equal on
///   all three, so any clone authorizes [`EventStore::renew`],
///   [`EventStore::ack_work`], and [`EventStore::nack_work`] exactly as the
///   original does. There is no per-token identity to distinguish them.
/// - **Handing a clone to another component hands over the authority**, across
///   threads or tasks included. That is the intended way to delegate work; it is
///   also the whole risk, since the store cannot tell a delegate from the
///   original holder.
/// - **The store, not the token, decides when authority ends.** Terminal state
///   lives in the store, so the *first* successful `ack_work` or `nack_work`
///   consumes it and every outstanding clone — including the one the caller
///   still holds — becomes stale. Subsequent mutations through any copy fail
///   with [`Error::NotLeaseOwner`], never succeed twice. Cloning therefore
///   cannot duplicate an effect through this crate; at-least-once redelivery
///   still requires consumer effects to be idempotent.
/// - **Expiry is likewise store-side.** Holding a clone past `expires_at`
///   confers nothing: the item is reclaimable by anyone, and the reclaim issues
///   a higher fence that invalidates every copy of the old token at once.
///
/// A backend that needs a lease to be non-transferable must bind mutations to an
/// authenticated principal itself, as noted above. Removing `Clone` here would
/// not achieve it — a holder can still pass the original by value.
///
/// # Forging a lease does not compile
///
/// A losing claimant has the canonical event and the disclosed holder, and still
/// cannot build the winner's lease:
///
/// ```ignore
/// fn forge(event: Event, holder: ConsumerId) -> Lease {
///     Lease { event, owner: holder, fence: Fence(1), expires_at: u64::MAX }
/// }
/// ```
///
/// That is enforced by the trybuild case `tests/ui/lease_is_unconstructable.rs`,
/// whose committed `.stderr` pins the exact `E0451`. It is a UI test rather than
/// a `compile_fail` doctest because doctests are not run by CI's test runner and
/// because `compile_fail,E0451` does not self-enforce on stable. See
/// `tests/compile_fail.rs`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    event: Event,
    owner: ConsumerId,
    fence: Fence,
    expires_at: Timestamp,
}

impl Lease {
    /// The leased event.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// The consumer this lease was granted to.
    pub fn owner(&self) -> &ConsumerId {
        &self.owner
    }

    /// The fencing token this lease was issued under.
    ///
    /// Readable only by a holder, who by definition already has the token. It is
    /// deliberately absent from [`ClaimOutcome::Contended`], where it would be
    /// readable by a *non*-holder.
    pub fn fence(&self) -> Fence {
        self.fence
    }

    /// When this lease lapses, in the store's clock domain.
    pub fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// The detailed outcome of one claim attempt.
///
/// This is the protocol contract for claiming. [`EventStoreExt::claim`] is a lossy
/// projection of it, kept for compatibility; new consumers should match on this
/// type so a losing claimant can distinguish "someone else holds this right
/// now" from "there is nothing to do".
///
/// # Holder disclosure is deliberate
///
/// [`ClaimOutcome::Contended`] reveals the current lease owner's identity to
/// any caller that can attempt a claim on the topic. This is a chosen exposure,
/// not an oversight. It is appropriate for the in-process, mutually-trusting
/// consumer model the reference [`MemoryStore`] serves, where naming the holder
/// is what turns a blind retry into a scheduled one.
///
/// It becomes an enumeration and reconnaissance surface the moment a durable or
/// networked backend serves mutually-distrusting claimants: such an attacker can
/// attempt claims repeatedly to map who-holds-what. A backend with that threat
/// model must gate or redact `Contended` behind its own policy — the core
/// [`Authorizer`] seam only decides whether a consumer may claim the topic at
/// all, not what it may learn about other consumers. To keep the exposure as
/// small as the contract allows, a contended outcome names exactly one holder
/// — the lowest-sequence live-contended event — never the full contention set.
///
/// # The disclosed set is data, not capability — because a [`Lease`] cannot be built
///
/// What is disclosed is `event_id`, `holder`, and `expires_at`: enough to name the
/// winner and schedule a retry. The **active fence is not disclosed**. It is
/// internal ordering data that a losing claimant never needed, and disclosing it
/// was what turned this outcome into forge material.
///
/// The guarantee that these values are inert rests on [`Lease`] being
/// unconstructable outside this crate, *not* on the mutation checks alone. Lease
/// mutations validate owner, fence, and canonical event against store state — all
/// three of which a caller could once satisfy by assembling a lease from published
/// data. They are inert now because there is no route from `event_id`, `holder`,
/// and `expires_at` to a `Lease` value at all.
///
/// Both halves are required, and they are coupled. See the design note on
/// [`Lease`] before relaxing either: re-adding a public lease constructor or
/// re-disclosing the fence individually reopens the same hole.
///
/// # Reading a fence off a contended outcome does not compile
///
/// ```ignore
/// fn holders_fence(outcome: &ClaimOutcome) -> Option<Fence> {
///     match outcome {
///         ClaimOutcome::Contended { fence, .. } => Some(*fence),
///         _ => None,
///     }
/// }
/// ```
///
/// Enforced by `tests/ui/contended_does_not_disclose_fence.rs`, whose committed
/// `.stderr` pins the exact `E0026`. See `tests/compile_fail.rs` for why these
/// guards are UI tests rather than `compile_fail` doctests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimOutcome<L> {
    /// The caller now holds the lease described here.
    Granted(L),
    /// Nothing was claimable, and live work is held by another consumer.
    ///
    /// Reported only for work that is *currently leased and unexpired*.
    /// Acknowledged work and fence-exhausted work have no current holder and are
    /// never reported here, so a stale owner of finished work is never named.
    ///
    /// `holder` may be the calling consumer itself. A consumer that already holds
    /// the only live item in a topic, and claims again, is told that it is the
    /// holder. That is not an error and not a disclosure: it is the same fact the
    /// caller already had.
    ///
    /// When several earlier events are concurrently leased, this names the one
    /// with the **lowest sequence within the topic**. [`EventStore::claim_detailed`]
    /// takes no [`EventId`], so the choice is fixed by scan order to keep the
    /// outcome deterministic.
    ///
    /// The holder's active fencing token is deliberately **not** reported. It is
    /// internal ordering data with no scheduling value to a losing claimant, and
    /// disclosing it supplied the last field needed to forge the holder's lease
    /// back when [`Lease`] was constructible. Do not reintroduce it, or any
    /// equivalent projection of it.
    Contended {
        /// The contended event.
        event_id: EventId,
        /// The consumer currently holding the lease. See the type-level note:
        /// this names the winner, and confers nothing.
        holder: ConsumerId,
        /// When the holder's lease lapses, after which reclaim can succeed.
        /// This is what makes a scheduled retry possible instead of a blind one.
        expires_at: Timestamp,
    },
    /// The topic has no claimable work and no live-contended work.
    Empty,
}

impl<L> ClaimOutcome<L> {
    /// Projects onto the lossy [`EventStoreExt::claim`] shape.
    ///
    /// `Granted` becomes `Some`; `Contended` and `Empty` both collapse to
    /// `None`. This is the single definition of that projection, so
    /// `store.claim(..) == store.claim_detailed(..).granted()` holds by
    /// construction rather than by two implementations agreeing.
    pub fn granted(self) -> Option<L> {
        match self {
            Self::Granted(lease) => Some(lease),
            Self::Contended { .. } | Self::Empty => None,
        }
    }
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
    IdempotencyKeyConflict {
        key: IdempotencyKey,
    },
    OutOfOrderAck {
        expected: u64,
        actual: u64,
    },
    UnknownEvent,
    NotLeaseOwner,
    StaleFence,
    LeaseExpired,
    InvalidLeaseDuration,
    /// The store's internal lock was poisoned by a panic in an earlier
    /// operation, and the store has fail-stopped.
    ///
    /// The realistic source is an injected [`Clock`]: [`MemoryStore`] samples
    /// its clock *while holding the state lock*, so a panicking `now` poisons
    /// the store. [`MemoryStore::with_clock`] is a public seam, which makes this
    /// a foreseeable input rather than a corruption event.
    ///
    /// This is permanent and deliberate. Once poisoned the store never recovers.
    ///
    /// It is not, however, what *every* later call returns. Authorization is
    /// checked before the lock is taken, so a call the [`Authorizer`] denies
    /// still returns [`Error::Unauthorized`] on a poisoned store. That ordering
    /// is deliberate and load-bearing: the policy answer must not depend on the
    /// store's health, or a poisoned store would start disclosing which
    /// operations *would* have been permitted. Only calls that get past
    /// authorization report this error. See [`MemoryStore`] for why the
    /// fail-stop is reported rather than panicked, and why recovery is not
    /// offered even though the reference store's invariants do in fact survive.
    StorePoisoned,
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
            Self::StorePoisoned => {
                f.write_str("store is poisoned by an earlier panic and has fail-stopped")
            }
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
/// # Each store defines its own lease type
///
/// [`EventStore::Lease`] is an associated type. The reference [`MemoryStore`]
/// uses [`Lease`], whose fields are private to prevent consumer forgery — see
/// the design note on that type. External backends define their own lease type
/// with whatever opacity their threat model requires, and construct it freely
/// within their `claim_detailed` and `renew` implementations. A lease minted
/// by one store cannot be presented to another: the type system enforces this
/// at compile time.
///
/// This is the store-bound minting design. Forgery resistance for the reference
/// store rests on [`Lease`]'s private fields and on
/// [`ClaimOutcome::Contended`] not disclosing the active fence; those two
/// properties are coupled and neither may be relaxed on the grounds that the
/// other covers it. External backends decide their own opacity: the trait
/// requires only `Clone + Debug + Send + Sync`. A backend whose lease type is
/// publicly constructable must ensure that the data disclosed in `Contended` is
/// insufficient to forge a valid lease — that is the obligation the associated
/// type delegates.
///
/// This synchronous trait is the local/reference contract. Network adapters
/// should expose their own asynchronous API rather than blocking an async
/// runtime behind this trait. The central store owns lease time; distributed
/// clients request durations but never supply timestamps or clocks. Retention
/// and compaction are intentionally not part of the MVP contract; the reference
/// store is unbounded.
pub trait EventStore: Send + Sync {
    /// The lease token this store issues. Each backend defines its own opaque
    /// type; the reference [`MemoryStore`] uses [`Lease`].
    type Lease: Clone + fmt::Debug + Send + Sync;

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

    /// Claims the first available event, reporting contention when there is none.
    ///
    /// This is the claim protocol contract. Implementations must compute the
    /// outcome once, under whatever single critical section guards their state.
    ///
    /// Available work always wins: the scan looks for a grant across the whole
    /// topic first, so later claimable work is preferred over an earlier event
    /// that merely happens to be leased. Only when no grant exists does this
    /// report [`ClaimOutcome::Contended`], naming the lowest-sequence event that
    /// is *currently* leased and unexpired. Acknowledged and fence-exhausted
    /// items are skipped and are never reported as contention.
    ///
    /// Durations are measured in whole milliseconds. Durations below one
    /// millisecond, durations outside the timestamp domain, and unrepresentable
    /// expiration times are rejected as [`Error::InvalidLeaseDuration`]. If an
    /// item's `u64` fencing-token space is exhausted, that item remains
    /// permanently unavailable and the search continues with later events.
    fn claim_detailed(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<ClaimOutcome<Self::Lease>, Error>;

    /// Renews a current lease without changing its owner or fence.
    ///
    /// Duration validation is identical to [`EventStore::claim_detailed`].
    fn renew(&self, lease: &Self::Lease, lease_for: Duration) -> Result<Self::Lease, Error>;

    /// Makes work terminal in this store; it does not transact external effects.
    ///
    /// A successful acknowledgement is not itself retry-idempotent: calling
    /// this method again with the consumed lease returns [`Error::NotLeaseOwner`].
    /// The returned [`WorkAck`] is a freely constructible acknowledgement
    /// record, not an authenticated or durable receipt. Callers that need
    /// replay-stable acceptance must provide that guarantee at the recipient
    /// boundary before acknowledging the queue item.
    fn ack_work(&self, lease: &Self::Lease) -> Result<WorkAck, Error>;

    /// Releases work immediately. The next claim receives a higher fence
    /// unless this lease consumed the last fencing token.
    fn nack_work(&self, lease: &Self::Lease) -> Result<(), Error>;
}

mod sealed {
    /// Marker supertrait on [`super::EventStoreExt`].
    ///
    /// # This excludes nothing, and is not what protects the projection
    ///
    /// Said plainly, because the name invites the opposite reading. `Sealed` is
    /// blanket-implemented for `T: EventStore + ?Sized` — the *same* bound
    /// [`super::EventStoreExt`] already carries as a supertrait. It is therefore
    /// satisfied exactly when `EventStore` is, and it turns away no type that
    /// `EventStore` has not turned away first.
    ///
    /// Measured, not argued. Removing this bound from `EventStoreExt` leaves all
    /// three trybuild guards passing with byte-identical `.stderr`, the
    /// substitution case included. Removing the *blanket impl* instead makes
    /// that same case compile. The exclusion lives in the blanket impl.
    ///
    /// It is kept as a statement of intent and as a seal that would begin to do
    /// work under a future narrowing: were the blanket `EventStoreExt` impl ever
    /// restricted to named in-crate types, this bound would have to be
    /// restricted with it, and only then would it exclude anything. Sealing is a
    /// real technique and it is unrelated to coherence; it is simply not the
    /// mechanism operating here. See
    /// <https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/>.
    pub trait Sealed {}
    impl<T: super::EventStore + ?Sized> Sealed for T {}
}

/// The lossy compatibility projection of [`EventStore::claim_detailed`].
///
/// This lives outside [`EventStore`] on purpose. As a *provided trait method* it
/// was overridable: a backend could supply its own `claim` body, and then the
/// documented equivalence `claim(..) == claim_detailed(..)?.granted()` would hold
/// only by that author's goodwill — two computations, two lock acquisitions, two
/// chances to observe different state. The doc said "by construction" over
/// something a downstream impl could replace.
///
/// It is now a blanket impl over every `T: EventStore`. There is exactly one
/// body, no backend can substitute another, and the equivalence really does hold
/// by construction.
///
/// Backends implement [`EventStore`] and get this for free. Callers need
/// `EventStoreExt` in scope to call [`EventStoreExt::claim`].
///
/// # What excludes a second body is coherence. The seal excludes nothing.
///
/// "Sealed by a private supertrait" credited the wrong mechanism, and the
/// crate's own thesis is that mechanisms should be credited accurately. Three
/// exclusions are at work here and they are independent; none of them may
/// borrow another's credit:
///
/// - **Lease forgery** is held by `E0451`. [`Lease`]'s fields are private and
///   there is no constructor, so the type is unbuildable outside the crate.
///   That is a property of `Lease` alone. It has nothing to do with this trait,
///   with the blanket impl, or with sealing, and it holds identically with both
///   of those removed. Guard: `tests/ui/lease_is_unconstructable.rs`.
/// - **Substitution of this projection** is held by coherence, `E0119`. The
///   blanket impl below already covers every `T: EventStore`, so a second impl
///   for any backend overlaps it and the compiler rejects the overlap. Guard:
///   `tests/ui/event_store_ext_cannot_be_substituted.rs`.
/// - **`sealed::Sealed` holds nothing at all.** It is blanket-implemented over
///   the same bound this trait already requires, so it is satisfied exactly when
///   `EventStore` is. Removing it from the supertrait list leaves all three
///   guards passing with byte-identical `.stderr`; removing the blanket impl
///   instead makes the substitution case compile.
///
/// Sealing in general is a genuine technique and it is *unrelated to coherence*:
/// see <https://predr.ag/blog/definitive-guide-to-sealed-traits-in-rust/>, which
/// says so outright. Read that as background for why the two were conflated
/// here, not as the thing keeping the guarantee true.
///
/// **The trybuild case is the enforcement; the citation is only the
/// explanation.** A cited claim is verifiable but never automatically verified —
/// links rot, articles are revised, and nothing turns red when they do. The
/// committed `.stderr` pinning `E0119` is what fails the build if this stops
/// being true, and no footnote may be read as standing in for it.
///
/// # Substituting the projection does not compile
///
/// ```ignore
/// impl EventStoreExt for Backend {          // Backend: EventStore already
///     fn claim(&self, ..) -> Result<Option<Lease>, Error> { Ok(None) }
/// }
/// ```
///
/// Enforced by `tests/ui/event_store_ext_cannot_be_substituted.rs`. That case
/// uses a backend that **does** implement [`EventStore`]; an earlier version
/// used a bare `struct Backend;` that did not, which made the guard vacuous —
/// it failed on the missing supertrait and would have failed identically with no
/// protection at all.
pub trait EventStoreExt: EventStore + sealed::Sealed {
    /// Claims the first available event, discarding why a claim failed.
    ///
    /// This is the **lossy compatibility surface, not the protocol contract**.
    /// `Ok(None)` collapses "another consumer holds this right now" together
    /// with "there is nothing to do", so a losing claimant cannot tell them
    /// apart or learn when to come back. New consumers should call
    /// [`EventStore::claim_detailed`] instead.
    fn claim(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<Option<Self::Lease>, Error>;
}

impl<T: EventStore + ?Sized> EventStoreExt for T {
    fn claim(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<Option<Self::Lease>, Error> {
        Ok(self.claim_detailed(consumer, topic, lease_for)?.granted())
    }
}

/// Process-local reference store.
///
/// # A panicking [`Clock`] fail-stops the store, as an error
///
/// This store samples its clock while holding the state lock, so a panic inside
/// an injected `Clock::now` poisons that lock. Every later operation that
/// reaches the lock then reports [`Error::StorePoisoned`] instead of panicking
/// at it.
///
/// **Authorization comes first and stays first.** Each method consults the
/// [`Authorizer`] before calling `MemoryStore::lock`, so a denied call returns
/// [`Error::Unauthorized`] whether or not the store is poisoned. A blanket
/// "every later operation reports `StorePoisoned`" would be a stronger claim
/// than the code makes, and the weaker one is the one worth having: a poisoned
/// store must not become an oracle for which operations the policy would have
/// allowed. The precedence is asserted by
/// `authorization_is_decided_before_the_poisoned_fail_stop`.
///
/// Returning an error is chosen over the two alternatives on purpose.
///
/// - **Over panicking**, because these methods return [`Result`]. A caller that
///   handles every documented error still had its thread unwound by an
///   `expect` on the lock — an undocumented panic in a fallible API, and one a
///   library has no business inflicting on a caller's process because an
///   injected clock misbehaved. `Clock` is a public seam
///   ([`MemoryStore::with_clock`]), so a misbehaving implementation is a
///   foreseeable input.
/// - **Over recovering** with `PoisonError::into_inner`, even though the
///   reference store's invariants genuinely do survive: every clock sample is
///   taken either before any mutation in that critical section, or after a
///   mutation that had already completed consistently. Recovery is still not
///   offered, because that reasoning is a property of *this* implementation's
///   current statement order, not of the [`EventStore`] contract. Silently
///   continuing would bake a fragile audit into the API, and a later edit that
///   moved a clock sample between two mutations would turn it false with no
///   signal. Fail-stop is the honest boundary.
///
/// Poisoning is therefore permanent: the store never un-poisons, and there is no
/// reset. The failure is loud, typed, and terminal rather than silent.
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
    /// Per-topic index of the first event that is not *permanently* terminal.
    ///
    /// Everything below this index is acknowledged or fence-exhausted, so it can
    /// never yield a grant or a holder again. Claiming starts here instead of at
    /// sequence one, which is what stops a topic's finished history from being
    /// rescanned on every claim. See [`MemoryStore::claim_detailed`].
    scan_floor: HashMap<Topic, usize>,
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

    /// Locks the state, reporting poisoning instead of panicking.
    ///
    /// See the type-level note: poisoning is a permanent, deliberate fail-stop.
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        self.state.lock().map_err(|_| Error::StorePoisoned)
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
    type Lease = Lease;

    fn append(&self, producer: &str, event: NewEvent) -> Result<AppendOutcome, Error> {
        if !self.authorizer.can_publish(producer, &event.topic) {
            return Err(Error::Unauthorized);
        }
        let mut state = self.lock()?;
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
        let state = self.lock()?;
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
        let mut state = self.lock()?;
        // `append` assigns `len + 1` and pushes, so a topic's sequences are
        // exactly `1..=len`, contiguous and in order. Existence is therefore a
        // bounds check, not a search: the previous linear scan walked the whole
        // topic on every broadcast acknowledgement, which made draining a topic
        // quadratic in its length for no information the length did not already
        // carry.
        let length = state.topics.get(topic).map_or(0, Vec::len) as u64;
        if sequence == 0 || sequence > length {
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

    fn claim_detailed(
        &self,
        consumer: &ConsumerId,
        topic: &Topic,
        lease_for: Duration,
    ) -> Result<ClaimOutcome<Lease>, Error> {
        if !self.authorizer.can_consume(consumer, topic) {
            return Err(Error::Unauthorized);
        }
        let mut guard = self.lock()?;
        let now = self.clock.now();
        let expires_at = Self::expiry_from(now, lease_for)?;

        // Borrow the three fields the scan touches separately. The previous
        // version cloned the entire topic — every `Event`, and so every
        // `Payload` — on every claim, purely to dodge a borrow conflict between
        // `topics` (shared) and `work` (mutable). Destructuring gives disjoint
        // field borrows instead, so the scan reads events in place and exactly
        // one `Event` is cloned: the one actually granted.
        let State {
            topics,
            work,
            scan_floor,
            ..
        } = &mut *guard;
        let Some(events) = topics.get(topic) else {
            return Ok(ClaimOutcome::Empty);
        };

        // Advance past the contiguous prefix of permanently terminal work.
        // Acknowledged and fence-exhausted items can never yield a grant or a
        // holder, so skipping them is invisible to the outcome — but rescanning
        // them was what made a drained topic cost O(N) per claim, and O(N^2) to
        // drain. Each index is stepped over at most once in the store's
        // lifetime, so this loop is amortized O(1) per claim.
        let floor = scan_floor.entry(topic.clone()).or_insert(0);
        while *floor < events.len()
            && work
                .get(&events[*floor].id)
                .is_some_and(LeaseKernel::is_permanently_terminal)
        {
            *floor += 1;
        }

        // Remembers the earliest live-contended item while the scan keeps
        // looking for an outright grant. Available work must beat contention.
        let mut contended = None;
        for event in &events[*floor..] {
            let work = work
                .get_mut(&event.id)
                .expect("work state exists for event");
            let outcome = work.claim(consumer.clone(), now, expires_at);
            // Terminal and fence-exhausted items yield neither a grant nor a
            // holder, so they fall through both arms and are simply skipped.
            if let Some(holder) = outcome.contended() {
                // The holder's fence stays inside the kernel record. It is never
                // projected into the outcome: see the note on `Contended`.
                contended.get_or_insert_with(|| ClaimOutcome::Contended {
                    event_id: event.id.clone(),
                    holder: holder.owner.clone(),
                    expires_at: holder.expires_at,
                });
                continue;
            }
            if let Some(lease) = outcome.granted() {
                return Ok(ClaimOutcome::Granted(Lease {
                    event: event.clone(),
                    owner: lease.owner,
                    fence: lease.fence,
                    expires_at: lease.expires_at,
                }));
            }
        }
        Ok(contended.unwrap_or(ClaimOutcome::Empty))
    }

    fn renew(&self, lease: &Lease, lease_for: Duration) -> Result<Lease, Error> {
        if !self
            .authorizer
            .can_consume(&lease.owner, &lease.event.topic)
        {
            return Err(Error::Unauthorized);
        }
        let mut state = self.lock()?;
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
        let mut state = self.lock()?;
        let now = self.clock.now();
        Self::validate_lease(&state, lease, now)?;
        state
            .work
            .get_mut(&lease.event.id)
            .expect("validated lease state exists")
            .acknowledge(&lease.owner, lease.fence, now)
            .map_err(map_lease_error)?;
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
        let mut state = self.lock()?;
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
