use super::*;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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

fn fixture() -> (MemoryStore, Arc<FakeClock>) {
    let clock = Arc::new(FakeClock::default());
    (MemoryStore::new(clock.clone(), Arc::new(AllowAll)), clock)
}

fn new(id: &str, topic: &str) -> NewEvent {
    NewEvent {
        id: EventId(id.into()),
        topic: Topic(topic.into()),
        idempotency_key: IdempotencyKey(format!("key-{id}")),
        payload: id.as_bytes().to_vec(),
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
    let store = MemoryStore::new(clock, Arc::new(DenyAll));
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
    let store = MemoryStore::new(clock, auth.clone());
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
