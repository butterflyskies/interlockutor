use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Barrier, Mutex};

#[derive(Default)]
struct FakeClock(AtomicU64);

impl Clock for FakeClock {
    fn now(&self) -> Timestamp {
        self.0.load(Ordering::SeqCst)
    }
}
impl FakeClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::SeqCst);
    }

    fn set(&self, ms: u64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}

struct LockAssertingClock {
    state: Arc<Mutex<State>>,
    now: AtomicU64,
}

impl Clock for LockAssertingClock {
    fn now(&self) -> Timestamp {
        assert!(
            self.state.try_lock().is_err(),
            "clock was sampled before acquiring the contended state lock"
        );
        self.now.load(Ordering::SeqCst)
    }
}

impl LockAssertingClock {
    fn set(&self, now: Timestamp) {
        self.now.store(now, Ordering::SeqCst);
    }
}

fn lock_asserting_fixture() -> (MemoryStore, Arc<LockAssertingClock>) {
    let state = Arc::new(Mutex::new(State::default()));
    let clock = Arc::new(LockAssertingClock {
        state: state.clone(),
        now: AtomicU64::new(0),
    });
    let store = MemoryStore {
        clock: clock.clone(),
        authorizer: Arc::new(AllowAll),
        state,
    };
    (store, clock)
}

fn fixture() -> (MemoryStore, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::default());
    (
        MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll)),
        clock,
    )
}

fn new(id: &str, topic: &str) -> NewEvent {
    NewEvent {
        id: EventId(id.into()),
        topic: Topic(topic.into()),
        idempotency_key: IdempotencyKey(format!("key-{id}")),
        payload: Payload::from_bytes(id.as_bytes().to_vec()),
    }
}

fn append(store: &impl EventStore, id: &str, topic: &str) -> Event {
    match store.append("producer", new(id, topic)).unwrap() {
        AppendOutcome::Appended(e) => e,
        _ => panic!("first append must append"),
    }
}

#[test]
fn append_is_idempotent_and_order_is_per_topic() {
    let (store, _) = fixture();
    let a1 = append(&store, "a1", "a");
    let b1 = append(&store, "b1", "b");
    let a2 = append(&store, "a2", "a");
    assert_eq!((a1.sequence, b1.sequence, a2.sequence), (1, 1, 2));
    assert_eq!(
        store.append("p", new("a1", "a")).unwrap(),
        AppendOutcome::Existing(a1)
    );
}

#[test]
fn payload_json_serializes_compact_bytes() {
    #[derive(serde::Serialize)]
    struct Message<'a> {
        kind: &'a str,
        count: u8,
    }

    let payload = Payload::json(&Message {
        kind: "ready",
        count: 2,
    })
    .unwrap();

    assert_eq!(payload.as_bytes(), br#"{"kind":"ready","count":2}"#);

    let decoded: serde_json::Value = serde_json::from_slice(payload.as_bytes()).unwrap();
    assert_eq!(decoded, serde_json::json!({"kind": "ready", "count": 2}));
}

#[test]
fn payload_json_accepts_unsized_root_values() {
    assert_eq!(Payload::json("ready").unwrap().as_bytes(), br#""ready""#);
    assert_eq!(
        Payload::json(&[1_u8, 2, 3][..]).unwrap().as_bytes(),
        b"[1,2,3]"
    );
}

#[test]
fn payload_json_preserves_serialization_failure_source() {
    struct Fails;

    impl serde::Serialize for Fails {
        fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("intentional test failure"))
        }
    }

    let error = Payload::json(&Fails).unwrap_err();
    assert!(matches!(error, PayloadError::Json(_)));
    assert!(std::error::Error::source(&error).is_some());
    assert!(error.to_string().contains("intentional test failure"));
}

#[test]
fn payload_raw_bytes_round_trip_unchanged() {
    let bytes = vec![0, 0xff, 0x80, 1];
    let payload = Payload::from_bytes(bytes.clone());

    assert_eq!(payload.as_bytes(), bytes.as_slice());
    assert_eq!(payload.into_bytes(), bytes);
}

#[test]
fn default_clock_supports_claim_and_renew_without_client_time() {
    let store = MemoryStore::new(Arc::new(AllowAll));
    append(&store, "job", "work");
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_secs(60),
        )
        .unwrap()
        .unwrap();
    let renewed = store.renew(&lease, Duration::from_secs(60)).unwrap();

    assert_eq!(renewed.fence, lease.fence);
    assert!(renewed.expires_at >= lease.expires_at);
}

#[test]
fn idempotency_key_reuse_for_different_event_is_a_conflict() {
    let (store, _) = fixture();
    append(&store, "original", "topic-a");

    let mut conflicting = new("replacement", "topic-b");
    conflicting.idempotency_key = IdempotencyKey("key-original".into());
    assert_eq!(
        store.append("producer", conflicting),
        Err(Error::IdempotencyKeyConflict {
            key: IdempotencyKey("key-original".into())
        })
    );
    assert!(
        store
            .read_broadcast(&ConsumerId("reader".into()), &Topic("topic-b".into()), 10)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn broadcast_consumers_have_independent_contiguous_cursors() {
    let (store, _) = fixture();
    append(&store, "1", "news");
    append(&store, "2", "news");
    let a = ConsumerId("a".into());
    let b = ConsumerId("b".into());
    let topic = Topic("news".into());
    assert_eq!(store.read_broadcast(&a, &topic, 10).unwrap().len(), 2);
    assert_eq!(
        store.ack_broadcast(&a, &topic, 2),
        Err(Error::OutOfOrderAck {
            expected: 1,
            actual: 2
        })
    );
    let ack = store.ack_broadcast(&a, &topic, 1).unwrap();
    assert_eq!(ack.consumer, a);
    assert_eq!(ack.topic, topic);
    assert_eq!(ack.sequence, 1);
    assert_eq!(ack.acknowledged_at, 0);
    assert_eq!(store.read_broadcast(&a, &topic, 10).unwrap()[0].sequence, 2);
    assert_eq!(store.read_broadcast(&b, &topic, 10).unwrap().len(), 2);
}

#[test]
fn claim_is_exclusive_until_expiry_then_fence_increases() {
    let (store, clock) = fixture();
    append(&store, "job", "work");
    let topic = Topic("work".into());
    let a = ConsumerId("a".into());
    let b = ConsumerId("b".into());
    let first = store
        .claim(&a, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    assert_eq!(first.fence, Fence(1));
    assert_eq!(
        store.claim(&b, &topic, Duration::from_millis(10)).unwrap(),
        None
    );
    clock.advance(10);
    let second = store
        .claim(&b, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    assert_eq!(second.fence, Fence(2));
    assert_eq!(store.ack_work(&first), Err(Error::StaleFence));
    store.ack_work(&second).unwrap();
    assert_eq!(
        store.claim(&a, &topic, Duration::from_millis(10)).unwrap(),
        None
    );
}

#[test]
fn lease_transitions_sample_authoritative_time_under_the_state_lock() {
    let (store, clock) = lock_asserting_fixture();
    let event = append(&store, "job", "work");
    let consumer = ConsumerId("worker".into());
    let topic = Topic("work".into());

    let first = store
        .claim(&consumer, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    let renewed = store.renew(&first, Duration::from_millis(20)).unwrap();
    store.nack_work(&renewed).unwrap();

    clock.set(10);
    let second = store
        .claim(&consumer, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    let ack = store.ack_work(&second).unwrap();

    assert_eq!(second.event, event);
    assert_eq!(ack.acknowledged_at, 10);
}

#[test]
fn renewal_extends_lease_and_nack_requeues_with_new_fence() {
    let (store, clock) = fixture();
    append(&store, "job", "work");
    let topic = Topic("work".into());
    let a = ConsumerId("a".into());
    let b = ConsumerId("b".into());
    let first = store
        .claim(&a, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    clock.advance(5);
    let renewed = store.renew(&first, Duration::from_millis(20)).unwrap();
    assert_eq!(renewed.expires_at, 25);
    assert_eq!(renewed.fence, first.fence);
    clock.advance(10);
    assert_eq!(
        store.claim(&b, &topic, Duration::from_millis(10)).unwrap(),
        None
    );
    store.nack_work(&renewed).unwrap();
    let second = store
        .claim(&b, &topic, Duration::from_millis(10))
        .unwrap()
        .unwrap();
    assert_eq!(second.fence, Fence(2));
}

#[test]
fn expired_lease_is_rejected_without_reclaim() {
    let (store, clock) = fixture();
    append(&store, "job", "work");
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(10),
        )
        .unwrap()
        .unwrap();
    clock.advance(10);
    assert_eq!(store.ack_work(&lease), Err(Error::LeaseExpired));
    assert_eq!(
        store.renew(&lease, Duration::from_millis(10)),
        Err(Error::LeaseExpired)
    );
    assert_eq!(store.nack_work(&lease), Err(Error::LeaseExpired));
}

#[test]
fn renew_rejects_zero_duration_and_uses_one_clock_read() {
    let (store, clock) = fixture();
    append(&store, "job", "work");
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(10),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        store.renew(&lease, Duration::ZERO),
        Err(Error::InvalidLeaseDuration)
    );
    clock.set(3);
    assert_eq!(
        store
            .renew(&lease, Duration::from_millis(20))
            .unwrap()
            .expires_at,
        23
    );
}

#[test]
fn concurrent_claim_race_has_exactly_one_winner() {
    const WORKERS: usize = 16;
    let (store, _) = fixture();
    append(&store, "job", "work");
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(WORKERS));
    let handles: Vec<_> = (0..WORKERS)
        .map(|index| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .claim(
                        &ConsumerId(format!("worker-{index}")),
                        &Topic("work".into()),
                        Duration::from_secs(1),
                    )
                    .unwrap()
            })
        })
        .collect();
    let winners = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(Option::is_some)
        .count();
    assert_eq!(winners, 1);
}

#[test]
fn zero_duration_is_rejected() {
    let (store, _) = fixture();
    append(&store, "job", "work");
    assert_eq!(
        store.claim(
            &ConsumerId("a".into()),
            &Topic("work".into()),
            Duration::ZERO
        ),
        Err(Error::InvalidLeaseDuration)
    );
}

#[test]
fn lease_duration_error_explains_granularity_and_range() {
    assert_eq!(
        Error::InvalidLeaseDuration.to_string(),
        "lease duration must be at least one millisecond and produce a representable expiration"
    );
}

#[test]
fn timestamp_overflow_is_rejected_without_creating_a_lease() {
    let (store, clock) = fixture();
    append(&store, "job", "work");
    clock.set(u64::MAX);

    assert_eq!(
        store.claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(1),
        ),
        Err(Error::InvalidLeaseDuration)
    );

    clock.set(0);
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(1),
        )
        .unwrap()
        .unwrap();
    assert_eq!(lease.fence, Fence(1));
}

#[test]
fn duration_larger_than_the_timestamp_domain_is_rejected() {
    let (store, _) = fixture();
    append(&store, "job", "work");

    assert_eq!(
        store.claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_secs(u64::MAX),
        ),
        Err(Error::InvalidLeaseDuration)
    );
}

#[test]
fn exhausted_fence_skips_the_item_without_panicking() {
    let (store, _) = fixture();
    let first = append(&store, "first", "work");
    let second = append(&store, "second", "work");
    store
        .state
        .lock()
        .unwrap()
        .work
        .insert(first.id, LeaseKernel::available_after(Fence(u64::MAX)));

    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(1),
        )
        .unwrap()
        .unwrap();

    assert_eq!(lease.event, second);
    assert_eq!(lease.fence, Fence(1));
}

#[test]
fn successful_work_ack_is_terminal_but_not_retry_idempotent() {
    let (store, _) = fixture();
    append(&store, "job", "work");
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_millis(1),
        )
        .unwrap()
        .unwrap();

    store.ack_work(&lease).unwrap();
    assert_eq!(store.ack_work(&lease), Err(Error::NotLeaseOwner));
    assert_eq!(
        store
            .claim(
                &ConsumerId("other".into()),
                &Topic("work".into()),
                Duration::from_millis(1),
            )
            .unwrap(),
        None
    );
}

#[test]
fn many_events_preserve_order_and_are_claimed_once() {
    let (store, _) = fixture();
    let topic = Topic("work".into());
    let consumer = ConsumerId("worker".into());
    for i in 0..128 {
        append(&store, &format!("job-{i}"), "work");
    }
    for i in 0..128 {
        let lease = store
            .claim(&consumer, &topic, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(lease.event.sequence, i + 1);
        store.ack_work(&lease).unwrap();
    }
    assert_eq!(
        store
            .claim(&consumer, &topic, Duration::from_secs(1))
            .unwrap(),
        None
    );
}

#[test]
fn event_ids_cannot_be_reused_with_a_new_idempotency_key() {
    let (store, _) = fixture();
    append(&store, "same", "work");
    let mut duplicate = new("same", "work");
    duplicate.idempotency_key = IdempotencyKey("different-key".into());
    assert_eq!(
        store.append("producer", duplicate),
        Err(Error::DuplicateEventId)
    );
}

struct DenyAll;

impl Authorizer for DenyAll {
    fn can_publish(&self, _: &str, _: &Topic) -> bool {
        false
    }

    fn can_consume(&self, _: &ConsumerId, _: &Topic) -> bool {
        false
    }
}

#[derive(Default)]
struct ToggleAuthorizer(AtomicBool);

impl ToggleAuthorizer {
    fn allow(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn deny(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Authorizer for ToggleAuthorizer {
    fn can_publish(&self, _: &str, _: &Topic) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn can_consume(&self, _: &ConsumerId, _: &Topic) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

#[test]
fn authorization_is_enforced_at_publish_and_consume_boundaries() {
    let clock = Arc::new(FakeClock::default());
    let store = MemoryStore::with_clock(clock, Arc::new(DenyAll));
    assert_eq!(
        store.append("producer", new("job", "work")),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.read_broadcast(&ConsumerId("worker".into()), &Topic("work".into()), 1),
        Err(Error::Unauthorized)
    );
}

#[test]
fn authorization_revocation_blocks_all_lease_verbs() {
    let clock = Arc::new(FakeClock::default());
    let auth = Arc::new(ToggleAuthorizer::default());
    auth.allow();
    let store = MemoryStore::with_clock(clock, auth.clone());
    append(&store, "job", "work");
    let lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_secs(1),
        )
        .unwrap()
        .unwrap();
    auth.deny();
    assert_eq!(
        store.read_broadcast(&lease.owner, &lease.event.topic, 1),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.ack_broadcast(&lease.owner, &lease.event.topic, 1),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.renew(&lease, Duration::from_secs(1)),
        Err(Error::Unauthorized)
    );
    assert_eq!(store.ack_work(&lease), Err(Error::Unauthorized));
    assert_eq!(store.nack_work(&lease), Err(Error::Unauthorized));
    assert_eq!(
        store.claim(
            &ConsumerId("other".into()),
            &Topic("work".into()),
            Duration::from_secs(1)
        ),
        Err(Error::Unauthorized)
    );
}

/// In-crate defence in depth for the canonical-event check in `validate_lease`.
///
/// External callers cannot reach this at all — [`Lease`] fields are private, so
/// there is no public path that mutates a granted lease. This asserts the store
/// still refuses a lease whose event has drifted from the canonical record, so an
/// in-crate mistake cannot quietly become an authorization bypass.
#[test]
fn lease_event_cannot_be_forged_to_change_its_topic() {
    let (store, _) = fixture();
    append(&store, "job", "work");
    let mut lease = store
        .claim(
            &ConsumerId("worker".into()),
            &Topic("work".into()),
            Duration::from_secs(1),
        )
        .unwrap()
        .unwrap();
    lease.event.topic = Topic("different".into());
    assert_eq!(store.ack_work(&lease), Err(Error::UnknownEvent));
}

fn contention_fixture(scenario: &str) -> MemoryStore {
    let (store, _clock) = fixture();
    let topic = Topic("work".into());
    let hold = |name: &str| {
        store
            .claim(&ConsumerId(name.into()), &topic, Duration::from_secs(1))
            .unwrap()
            .expect("fixture expects claimable work")
    };
    match scenario {
        "empty" => {}
        "available" => {
            append(&store, "a", "work");
        }
        "contended" => {
            append(&store, "a", "work");
            hold("holder");
        }
        "acknowledged" => {
            append(&store, "a", "work");
            let lease = hold("holder");
            store.ack_work(&lease).unwrap();
        }
        "contention_then_available" => {
            append(&store, "a", "work");
            append(&store, "b", "work");
            hold("holder");
        }
        "all_contended" => {
            for id in ["a", "b", "c"] {
                append(&store, id, "work");
            }
            hold("holder-a");
            hold("holder-b");
            hold("holder-c");
        }
        other => panic!("unknown scenario {other}"),
    }
    store
}

fn probe(store: &MemoryStore) -> ClaimOutcome {
    store
        .claim_detailed(
            &ConsumerId("probe".into()),
            &Topic("work".into()),
            Duration::from_secs(1),
        )
        .unwrap()
}

#[test]
fn contention_names_the_current_holder_and_expiry_but_never_the_fence() {
    let store = contention_fixture("contended");
    // The holder's active fence is Fence(1) here and is deliberately absent from
    // the outcome: it is the field that made the disclosure forgeable.
    let held = ClaimOutcome::Contended {
        event_id: EventId("a".into()),
        holder: ConsumerId("holder".into()),
        expires_at: 1000,
    };
    assert_eq!(probe(&store), held);
    // Reporting contention must not mutate: the same probe repeats verbatim,
    // and the holder's own lease is untouched.
    assert_eq!(probe(&store), held);
    assert_eq!(
        store
            .claim(
                &ConsumerId("probe".into()),
                &Topic("work".into()),
                Duration::from_secs(1)
            )
            .unwrap(),
        None
    );
}

#[test]
fn later_available_work_beats_earlier_contention() {
    let store = contention_fixture("contention_then_available");
    let ClaimOutcome::Granted(lease) = probe(&store) else {
        panic!("available work must win over an earlier contended event");
    };
    assert_eq!(lease.event.id, EventId("b".into()));
    assert_eq!(lease.owner, ConsumerId("probe".into()));
}

#[test]
fn contention_names_the_lowest_sequence_holder() {
    let store = contention_fixture("all_contended");
    // `claim` does not name an event, so the disclosed holder is fixed by scan
    // order: the earliest contended event, and only that one.
    assert_eq!(
        probe(&store),
        ClaimOutcome::Contended {
            event_id: EventId("a".into()),
            holder: ConsumerId("holder-a".into()),
            expires_at: 1000,
        }
    );
}

#[test]
fn acknowledged_work_is_empty_rather_than_contended() {
    // Terminal work has no current holder; naming its stale owner would be a lie.
    assert_eq!(
        probe(&contention_fixture("acknowledged")),
        ClaimOutcome::Empty
    );
    assert_eq!(probe(&contention_fixture("empty")), ClaimOutcome::Empty);
}

#[test]
fn expired_holder_is_reclaimed_rather_than_reported_as_contention() {
    let (store, clock) = fixture();
    append(&store, "a", "work");
    let topic = Topic("work".into());
    store
        .claim(
            &ConsumerId("holder".into()),
            &topic,
            Duration::from_millis(10),
        )
        .unwrap()
        .unwrap();
    clock.set(10);
    let ClaimOutcome::Granted(lease) = probe(&store) else {
        panic!("an expired holder is not contention");
    };
    assert_eq!(lease.owner, ConsumerId("probe".into()));
    assert_eq!(lease.fence, Fence(2));
}

/// `claim` must equal the projection of `claim_detailed` in the same state.
///
/// Two identically-built stores are used because both calls mutate. This is the
/// compatibility invariant that keeps the legacy `Option<Lease>` surface, and
/// oracles written against it, meaningful after the outcome type widened.
#[test]
fn legacy_claim_equals_the_detailed_projection() {
    for scenario in [
        "empty",
        "available",
        "contended",
        "acknowledged",
        "contention_then_available",
        "all_contended",
    ] {
        let legacy = contention_fixture(scenario);
        let detailed = contention_fixture(scenario);
        let consumer = ConsumerId("probe".into());
        let topic = Topic("work".into());
        assert_eq!(
            legacy
                .claim(&consumer, &topic, Duration::from_secs(1))
                .unwrap(),
            detailed
                .claim_detailed(&consumer, &topic, Duration::from_secs(1))
                .unwrap()
                .granted(),
            "scenario {scenario}"
        );
    }
}

#[test]
fn claim_detailed_respects_authorization_before_disclosing_a_holder() {
    struct DenyConsume;
    impl Authorizer for DenyConsume {
        fn can_publish(&self, _: &str, _: &Topic) -> bool {
            true
        }
        fn can_consume(&self, _: &ConsumerId, _: &Topic) -> bool {
            false
        }
    }
    let store = MemoryStore::with_clock(Arc::new(FakeClock::default()), Arc::new(DenyConsume));
    store.append("producer", new("a", "work")).unwrap();
    assert_eq!(
        store.claim_detailed(
            &ConsumerId("probe".into()),
            &Topic("work".into()),
            Duration::from_secs(1)
        ),
        Err(Error::Unauthorized)
    );
}

fn scan_floor_of(store: &MemoryStore, topic: &Topic) -> usize {
    store
        .state
        .lock()
        .expect("fixture store is not poisoned")
        .scan_floor
        .get(topic)
        .copied()
        .unwrap_or(0)
}

/// Draining a topic must not rescan the history it already finished.
///
/// This is the observable half of the claim-scan complexity fix. Before it, each
/// claim walked the topic from sequence one, so acknowledged work was re-examined
/// on every subsequent claim and draining N events cost O(N^2) kernel
/// transitions. The floor makes the acknowledged prefix cost O(1) per claim,
/// amortized, and it is asserted directly here rather than inferred from timing.
#[test]
fn draining_a_topic_advances_the_scan_floor_past_acknowledged_history() {
    const EVENTS: usize = 8;
    let (store, _) = fixture();
    let topic = Topic("work".into());
    let consumer = ConsumerId("courier".into());
    for i in 0..EVENTS {
        append(&store, &format!("a{i}"), "work");
    }
    assert_eq!(scan_floor_of(&store, &topic), 0);

    for i in 0..EVENTS {
        let lease = store
            .claim(&consumer, &topic, Duration::from_secs(1))
            .unwrap()
            .expect("each event in turn is claimable");
        assert_eq!(lease.event().sequence as usize, i + 1);
        store.ack_work(&lease).unwrap();
        assert_eq!(scan_floor_of(&store, &topic), i, "floor after claim {i}");
    }

    // The drained topic is empty, and the next claim starts past every event
    // rather than walking all of them to discover that.
    assert_eq!(
        store
            .claim_detailed(&consumer, &topic, Duration::from_secs(1))
            .unwrap(),
        ClaimOutcome::Empty
    );
    assert_eq!(scan_floor_of(&store, &topic), EVENTS);
}

/// The floor may only cross a *contiguous* terminal prefix.
///
/// Work completes out of order in practice. If the floor advanced past any
/// terminal item rather than a contiguous run of them, an earlier claimable
/// event would be skipped forever — the fix would have changed behaviour, which
/// it must not.
#[test]
fn the_scan_floor_never_skips_work_that_is_still_claimable() {
    let (store, _) = fixture();
    let topic = Topic("work".into());
    let consumer = ConsumerId("courier".into());
    for i in 0..3 {
        append(&store, &format!("a{i}"), "work");
    }

    let first = store
        .claim(&consumer, &topic, Duration::from_secs(1))
        .unwrap()
        .expect("a0 is claimable");
    assert_eq!(first.event().sequence, 1);
    let second = store
        .claim(&consumer, &topic, Duration::from_secs(1))
        .unwrap()
        .expect("a1 is claimable while a0 is held");
    assert_eq!(second.event().sequence, 2);

    // A later event finishes while an earlier one is still live.
    store.ack_work(&second).unwrap();
    assert_eq!(
        scan_floor_of(&store, &topic),
        0,
        "a terminal item behind a live one must not move the floor"
    );

    // The earlier event returns to the queue and is still granted, in order.
    store.nack_work(&first).unwrap();
    let requeued = store
        .claim(&consumer, &topic, Duration::from_secs(1))
        .unwrap()
        .expect("the released event is claimable again");
    assert_eq!(requeued.event().sequence, 1);
    assert_eq!(scan_floor_of(&store, &topic), 0);
}

/// Fence-exhausted work is permanently unclaimable, so the floor crosses it too.
///
/// This builds the exhausted state directly, which is *not* the state the
/// lifecycle produces: a real grant at the last fence leaves a lapsed `Leased`
/// record behind, not an `Available` one. That path is covered by
/// [`the_scan_floor_crosses_a_lapsed_lease_that_spent_the_last_fence`], and it
/// is the one that was broken while this test passed.
#[test]
fn the_scan_floor_crosses_fence_exhausted_history() {
    let (store, _) = fixture();
    let topic = Topic("work".into());
    let exhausted = append(&store, "a0", "work");
    let claimable = append(&store, "a1", "work");
    store.state.lock().unwrap().work.insert(
        exhausted.id.clone(),
        LeaseKernel::available_after(Fence(u64::MAX)),
    );

    let lease = store
        .claim(
            &ConsumerId("courier".into()),
            &topic,
            Duration::from_secs(1),
        )
        .unwrap()
        .expect("the later event is still claimable");
    assert_eq!(lease.event().id, claimable.id);
    assert_eq!(scan_floor_of(&store, &topic), 1);
}

/// Grant, expiry, exhaustion: the whole lifecycle, and the floor still crosses.
///
/// A lease really can be issued at `Fence(u64::MAX)`, and when it lapses without
/// an acknowledgement or a release the item is unclaimable forever. Every later
/// claim reported `FenceExhausted` correctly and left the phase as a lapsed
/// `Leased` record, which is not terminal by inspection — so the floor could
/// never cross it, and every claim on this topic re-examined it for the life of
/// the store. That contradicts the amortized-`O(1)` guarantee the floor exists
/// to provide, which is what makes this a defect rather than a cosmetic one.
///
/// The three claims below are the three phases. The assertion that fails
/// without the fix is the last one: the floor moves past the dead item.
#[test]
fn the_scan_floor_crosses_a_lapsed_lease_that_spent_the_last_fence() {
    let (store, clock) = fixture();
    let topic = Topic("work".into());
    let courier = ConsumerId("courier".into());
    let doomed = append(&store, "a0", "work");
    let claimable = append(&store, "a1", "work");
    // One below the ceiling, so the next grant is the last fence there is.
    store.state.lock().unwrap().work.insert(
        doomed.id.clone(),
        LeaseKernel::available_after(Fence(u64::MAX - 1)),
    );

    // 1. Grant. This is a real lease, not a synthetic state: the holder could
    //    renew it, acknowledge it, or release it.
    let last = store
        .claim(&courier, &topic, Duration::from_millis(10))
        .unwrap()
        .expect("the item is claimable at the last fence");
    assert_eq!(last.event().id, doomed.id);
    assert_eq!(last.fence(), Fence(u64::MAX));
    assert_eq!(scan_floor_of(&store, &topic), 0);

    // 2. Expiry. The holder does neither, and the lease lapses.
    clock.advance(10);
    assert_eq!(store.ack_work(&last), Err(Error::LeaseExpired));

    // 3. Exhaustion. The reclaim finds no fence left, so the item is skipped and
    //    the scan moves on to work that is still claimable.
    let next = store
        .claim(&courier, &topic, Duration::from_millis(10))
        .unwrap()
        .expect("the later event is unaffected");
    assert_eq!(next.event().id, claimable.id);

    // The exhausted item is now terminal *as a phase*, so the next claim's floor
    // pass crosses it instead of walking it again. Without that, the floor sticks
    // at zero forever and this item is re-examined on every claim.
    assert_eq!(
        store
            .claim_detailed(&ConsumerId("other".into()), &topic, Duration::from_secs(1))
            .unwrap(),
        ClaimOutcome::Contended {
            event_id: claimable.id.clone(),
            holder: courier.clone(),
            expires_at: 20,
        },
        "the exhausted item is skipped; the live one is the contention"
    );
    assert_eq!(scan_floor_of(&store, &topic), 1);

    // And it stays crossed: a stale owner of exhausted work is never disclosed.
    store.ack_work(&next).unwrap();
    assert_eq!(
        store
            .claim_detailed(&courier, &topic, Duration::from_secs(1))
            .unwrap(),
        ClaimOutcome::Empty
    );
    assert_eq!(scan_floor_of(&store, &topic), 2);
}

/// A poisoned store still answers the policy question first.
///
/// "Every later operation reports `StorePoisoned`" was too broad. Every method
/// consults the [`Authorizer`] before it takes the lock, so a denied call
/// returns [`Error::Unauthorized`] on a poisoned store exactly as it would on a
/// healthy one.
///
/// The ordering is worth keeping rather than a wart to document around: if
/// poisoning outranked policy, a poisoned store would answer differently for
/// permitted and denied operations and so disclose which ones the policy would
/// have allowed. Both directions are asserted here, on one store, so the test
/// cannot pass by the authorizer being consulted in neither case.
///
/// One panic is expected on stderr while this test runs: it is the injected one.
#[test]
fn authorization_is_decided_before_the_poisoned_fail_stop() {
    struct PanickingClock {
        armed: AtomicBool,
    }
    impl Clock for PanickingClock {
        fn now(&self) -> Timestamp {
            assert!(
                !self.armed.load(Ordering::SeqCst),
                "injected clock panic, under the state lock"
            );
            0
        }
    }

    let clock = Arc::new(PanickingClock {
        armed: AtomicBool::new(false),
    });
    let auth = Arc::new(ToggleAuthorizer::default());
    auth.allow();
    let store = MemoryStore::with_clock(clock.clone(), auth.clone());
    let topic = Topic("work".into());
    let consumer = ConsumerId("courier".into());
    append(&store, "a0", "work");
    let lease = store
        .claim(&consumer, &topic, Duration::from_secs(1))
        .unwrap()
        .expect("claimable before the clock misbehaves");

    clock.armed.store(true, Ordering::SeqCst);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.append("producer", new("a1", "work"));
    }));
    assert!(unwound.is_err(), "the armed clock must poison the lock");

    // Permitted calls reach the lock and report the fail-stop.
    assert_eq!(
        store.claim_detailed(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::StorePoisoned)
    );
    assert_eq!(store.ack_work(&lease), Err(Error::StorePoisoned));

    // Denied calls never reach it, so policy is what they report.
    auth.deny();
    assert_eq!(
        store.append("producer", new("a2", "work")),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.read_broadcast(&consumer, &topic, 10),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.ack_broadcast(&consumer, &topic, 1),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.claim_detailed(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.claim(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::Unauthorized)
    );
    assert_eq!(
        store.renew(&lease, Duration::from_secs(1)),
        Err(Error::Unauthorized)
    );
    assert_eq!(store.ack_work(&lease), Err(Error::Unauthorized));
    assert_eq!(store.nack_work(&lease), Err(Error::Unauthorized));

    // Re-permitting them brings the fail-stop back: poisoning is still permanent
    // and the authorizer is not masking it.
    auth.allow();
    assert_eq!(store.ack_work(&lease), Err(Error::StorePoisoned));
}

/// A panic in an injected `Clock::now` fail-stops the store as a typed error.
///
/// The store samples its clock under the state lock, so a panicking clock
/// poisons that lock. Every `Result`-returning operation used to panic at
/// `lock().expect(..)` instead of reporting anything — an undocumented panic in
/// a fallible API, reachable through the public `with_clock` seam.
///
/// One panic is expected on stderr while this test runs: it is the injected one,
/// caught below.
#[test]
fn a_panicking_clock_fail_stops_the_store_as_an_error_not_a_panic() {
    struct PanickingClock {
        armed: AtomicBool,
    }
    impl Clock for PanickingClock {
        fn now(&self) -> Timestamp {
            assert!(
                !self.armed.load(Ordering::SeqCst),
                "injected clock panic, under the state lock"
            );
            0
        }
    }

    let clock = Arc::new(PanickingClock {
        armed: AtomicBool::new(false),
    });
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    let topic = Topic("work".into());
    let consumer = ConsumerId("courier".into());
    append(&store, "a0", "work");
    let lease = store
        .claim(&consumer, &topic, Duration::from_secs(1))
        .unwrap()
        .expect("the event is claimable before the clock misbehaves");

    clock.armed.store(true, Ordering::SeqCst);
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = store.append("producer", new("a1", "work"));
    }));
    assert!(
        unwound.is_err(),
        "the armed clock must panic under the lock"
    );

    // Every operation that reaches the lock now reports the fail-stop rather
    // than panicking at it, including the ones that never touch the clock at
    // all. This fixture allows everything, so that is all of them here;
    // `authorization_is_decided_before_the_poisoned_fail_stop` covers the
    // denied case, which reports policy instead.
    assert_eq!(
        store.append("producer", new("a2", "work")),
        Err(Error::StorePoisoned)
    );
    assert_eq!(
        store.read_broadcast(&consumer, &topic, 10),
        Err(Error::StorePoisoned)
    );
    assert_eq!(
        store.ack_broadcast(&consumer, &topic, 1),
        Err(Error::StorePoisoned)
    );
    assert_eq!(
        store.claim_detailed(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::StorePoisoned)
    );
    assert_eq!(
        store.claim(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::StorePoisoned)
    );
    assert_eq!(
        store.renew(&lease, Duration::from_secs(1)),
        Err(Error::StorePoisoned)
    );
    assert_eq!(store.ack_work(&lease), Err(Error::StorePoisoned));
    assert_eq!(store.nack_work(&lease), Err(Error::StorePoisoned));

    // Poisoning is permanent. Repairing the clock does not repair the store.
    clock.armed.store(false, Ordering::SeqCst);
    assert_eq!(store.ack_work(&lease), Err(Error::StorePoisoned));
    assert_eq!(
        store.claim(&consumer, &topic, Duration::from_secs(1)),
        Err(Error::StorePoisoned)
    );
}
