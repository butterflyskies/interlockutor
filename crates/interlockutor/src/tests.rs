use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

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
    assert!(matches!(
        store.append("p", new("a1", "a")).unwrap(),
        AppendOutcome::Existing(_)
    ));
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
    store.ack_broadcast(&a, &topic, 1).unwrap();
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
