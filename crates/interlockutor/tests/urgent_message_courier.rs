use interlockutor::{
    AllowAll, AppendOutcome, Clock, ConsumerId, Error, Event, EventId, EventStore, IdempotencyKey,
    MemoryStore, NewEvent, Payload, Topic,
};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AcceptanceReceipt {
    event_id: String,
    receipt_id: String,
}

#[derive(Debug, Eq, PartialEq)]
enum AcceptanceOutcome {
    Recorded(AcceptanceReceipt),
    Existing(AcceptanceReceipt),
}

struct ScratchLedger(PathBuf);

impl ScratchLedger {
    fn create() -> Result<Self, Box<dyn StdError>> {
        let directory = Path::new(env!("CARGO_TARGET_TMPDIR"));
        fs::create_dir_all(directory)?;
        let path = directory.join(format!(
            "urgent-message-{}-{}.jsonl",
            std::process::id(),
            SCRATCH_ID.fetch_add(1, Ordering::Relaxed)
        ));
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        File::open(directory)?.sync_all()?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchLedger {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Test recipient whose append-only ledger outlives any one courier attempt.
///
/// `sync_all` makes the acceptance record durable before the caller may ACK the
/// queue. Reconstructing this adapter from the same path models a fresh courier
/// attempt without making `MemoryStore` itself durable.
struct RecipientLedger<'a> {
    path: &'a Path,
}

impl<'a> RecipientLedger<'a> {
    fn open(path: &'a Path) -> Self {
        Self { path }
    }

    fn accept(&self, event: &Event) -> Result<AcceptanceOutcome, Box<dyn StdError>> {
        if let Some(receipt) = self
            .receipts()?
            .into_iter()
            .find(|receipt| receipt.event_id == event.id.0)
        {
            return Ok(AcceptanceOutcome::Existing(receipt));
        }

        let receipt = acceptance_receipt(event);
        self.append(&receipt)?;
        Ok(AcceptanceOutcome::Recorded(receipt))
    }

    fn accept_without_dedup(&self, event: &Event) -> Result<AcceptanceReceipt, Box<dyn StdError>> {
        let receipt = acceptance_receipt(event);
        self.append(&receipt)?;
        Ok(receipt)
    }

    fn receipts(&self) -> Result<Vec<AcceptanceReceipt>, Box<dyn StdError>> {
        let reader = BufReader::new(File::open(self.path)?);
        reader
            .lines()
            .filter(|line| line.as_ref().is_ok_and(|line| !line.is_empty()))
            .map(|line| Ok(serde_json::from_str(&line?)?))
            .collect()
    }

    fn append(&self, receipt: &AcceptanceReceipt) -> Result<(), Box<dyn StdError>> {
        let mut file = OpenOptions::new().append(true).open(self.path)?;
        serde_json::to_writer(&mut file, receipt)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    }
}

fn acceptance_receipt(event: &Event) -> AcceptanceReceipt {
    AcceptanceReceipt {
        event_id: event.id.0.clone(),
        receipt_id: format!("recipient-accepted:{}", event.id.0),
    }
}

fn urgent_message() -> NewEvent {
    NewEvent {
        id: EventId("urgent-message-2026-08-03-001".into()),
        topic: Topic("urgent-messages".into()),
        idempotency_key: IdempotencyKey("urgent-message-2026-08-03-001".into()),
        payload: Payload::from_bytes(b"the east gate is open".to_vec()),
    }
}

fn claim_after_expiry(
    store: &MemoryStore,
    clock: &ManualClock,
    first_owner: &ConsumerId,
) -> interlockutor::Lease {
    clock.set(10);
    let next_owner = if first_owner.0 == "courier-a" {
        ConsumerId("courier-b".into())
    } else {
        ConsumerId("courier-a".into())
    };
    store
        .claim(
            &next_owner,
            &Topic("urgent-messages".into()),
            Duration::from_millis(10),
        )
        .expect("reclaim should be authorized")
        .expect("expired urgent message should be redelivered")
}

#[test]
fn urgent_message_has_one_live_courier_and_once_only_recipient_acceptance()
-> Result<(), Box<dyn StdError>> {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll)));
    let message = urgent_message();
    let appended = match store.append("dispatcher", message.clone())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("first append unexpectedly found an event".into()),
    };
    assert_eq!(
        store.append("dispatcher", message)?,
        AppendOutcome::Existing(appended.clone())
    );

    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = ["courier-a", "courier-b"]
        .into_iter()
        .map(|name| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.claim(
                    &ConsumerId(name.into()),
                    &Topic("urgent-messages".into()),
                    Duration::from_millis(10),
                )
            })
        })
        .collect();
    let mut claims = handles
        .into_iter()
        .map(|handle| handle.join().expect("courier thread should not panic"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);
    let first_lease = claims
        .iter_mut()
        .find_map(Option::take)
        .expect("exactly one courier should hold the live lease");
    assert_eq!(first_lease.event, appended);

    let scratch = ScratchLedger::create()?;
    let first_recipient = RecipientLedger::open(scratch.path());
    let first_receipt = match first_recipient.accept(&first_lease.event)? {
        AcceptanceOutcome::Recorded(receipt) => receipt,
        AcceptanceOutcome::Existing(_) => return Err("first acceptance was not new".into()),
    };
    assert_eq!(first_recipient.receipts()?, vec![first_receipt.clone()]);

    // The courier loses the queue ACK after the recipient has durably accepted
    // the message. Expiry therefore causes an intentional at-least-once retry.
    let second_lease = claim_after_expiry(&store, &clock, &first_lease.owner);
    assert_eq!(second_lease.event, appended);
    assert!(second_lease.fence > first_lease.fence);
    assert_eq!(store.ack_work(&first_lease), Err(Error::StaleFence));

    let redelivery_recipient = RecipientLedger::open(scratch.path());
    assert_eq!(
        redelivery_recipient.accept(&second_lease.event)?,
        AcceptanceOutcome::Existing(first_receipt.clone())
    );
    assert_eq!(redelivery_recipient.receipts()?, vec![first_receipt]);

    let queue_ack = store.ack_work(&second_lease)?;
    assert_eq!(queue_ack.event_id, appended.id);
    assert_eq!(queue_ack.fence, second_lease.fence);
    assert_eq!(
        store.claim(
            &ConsumerId("courier-c".into()),
            &Topic("urgent-messages".into()),
            Duration::from_millis(10),
        )?,
        None
    );
    Ok(())
}

#[test]
fn redelivery_duplicates_recipient_effect_without_event_id_dedup() -> Result<(), Box<dyn StdError>>
{
    let clock = Arc::new(ManualClock::default());
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    store.append("dispatcher", urgent_message())?;
    let first_lease = store
        .claim(
            &ConsumerId("courier-a".into()),
            &Topic("urgent-messages".into()),
            Duration::from_millis(10),
        )?
        .expect("urgent message should be claimable");

    let scratch = ScratchLedger::create()?;
    let recipient = RecipientLedger::open(scratch.path());
    recipient.accept_without_dedup(&first_lease.event)?;

    let second_lease = claim_after_expiry(&store, &clock, &first_lease.owner);
    recipient.accept_without_dedup(&second_lease.event)?;
    store.ack_work(&second_lease)?;

    let receipts = recipient.receipts()?;
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].event_id, receipts[1].event_id);
    Ok(())
}
