//! Domain-neutral dogfood trace: an urgent-message courier service.
//!
//! The recipient adapter here is a crash-safe, directory-backed effect store.
//! It exists to exercise the public API against a recipient that survives a
//! courier attempt, not to make [`MemoryStore`] durable.
//!
//! # Recipient commit protocol
//!
//! One file per accepted [`EventId`], never a shared append log. A shared log
//! made the dedup decision and the effect record two separate operations, so
//! two couriers could both observe absence and both record the effect; it also
//! let one torn tail record wedge every future redelivery. Committing is:
//!
//! 1. create a unique staging file in `tmp/`, on the same filesystem
//! 2. write the whole record
//! 3. `sync_all` the staging *file*
//! 4. `hard_link` it to `effects/<encoded-id>`, which fails if that path exists
//! 5. `sync_all` the `effects/` *directory*
//!
//! The link is the dedup decision *and* the effect record, in one atomic step,
//! so there is no window in which a courier has claimed acceptance without
//! having recorded it. A partial write can only ever exist in `tmp/`, which is
//! never read as an effect, so a torn record cannot wedge redelivery.

use interlockutor::{
    AllowAll, AppendOutcome, ClaimOutcome, Clock, ConsumerId, Error, Event, EventId, EventStore,
    IdempotencyKey, Lease, MemoryStore, NewEvent, Payload, Topic,
};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime};

static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);
static STAGING_ID: AtomicU64 = AtomicU64::new(0);

/// Conservative default: only debris no in-flight attempt can still own.
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(3600);

/// Filenames must fit the common single-component limit of 255 bytes.
const MAX_NAME_BYTES: usize = 255;

const TOPIC: &str = "urgent-messages";

/// Flushes a directory's entries to stable storage.
///
/// On unix this is a real `fsync` of the directory. Without it, a hard link is
/// atomically *visible* but not crash-durable: the new entry can still vanish
/// on power loss. There is no portable equivalent of this call, so on every
/// other platform it is a deliberate no-op, and the crash-durability half of
/// the recipient contract does not hold there. Atomicity and visibility still
/// do, because those come from `hard_link` itself.
#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

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

/// Failures the recipient can report, separately from a successful acceptance.
#[derive(Debug)]
enum AcceptError {
    /// The encoded [`EventId`] does not fit in one path component.
    EventIdTooLong {
        encoded: usize,
    },
    /// A record exists at the target path but is unreadable, truncated, empty,
    /// or names a different event.
    ///
    /// This is explicitly **not** a prior acceptance. The recipient cannot know
    /// whether the effect behind an unreadable record was performed, so it
    /// refuses rather than guessing, and the courier must not acknowledge the
    /// queue item. Reporting this as `Existing` would convert media corruption
    /// into silent effect loss; repairing it in place would convert it into
    /// silent effect duplication.
    UnusableRecord {
        path: PathBuf,
        reason: String,
    },
    Io(io::Error),
}

impl fmt::Display for AcceptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventIdTooLong { encoded } => write!(
                f,
                "encoded event ID is {encoded} bytes, over the {MAX_NAME_BYTES}-byte name limit"
            ),
            Self::UnusableRecord { path, reason } => {
                write!(
                    f,
                    "existing record at {} is unusable: {reason}",
                    path.display()
                )
            }
            Self::Io(error) => write!(f, "recipient I/O failed: {error}"),
        }
    }
}

impl StdError for AcceptError {}

impl From<io::Error> for AcceptError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Encodes an [`EventId`] as one safe path component.
///
/// A caller-supplied [`EventId`] is never used as a pathname directly. Raw ids
/// can traverse (`../..`), collide with a sibling record, or name nothing at all
/// (the empty string), and any of those turns into a *false acceptance*: a read
/// of some other event's record answering "already accepted" for this one.
///
/// Lowercase-hex encoding of the id's UTF-8 bytes, behind a fixed `e` prefix, is
/// injective and its charset contains no separator, so distinct ids always map
/// to distinct names inside `effects/` and no id can escape it. Being injective
/// rather than a digest, it also cannot collide — but the original id is still
/// stored in the record body and re-verified on every `Existing` path, so an
/// encoding accident could not silently read as acceptance of a different event.
fn encode_event_id(id: &EventId) -> Result<String, AcceptError> {
    let mut encoded = String::with_capacity(id.0.len() * 2 + 1);
    encoded.push('e');
    for byte in id.0.as_bytes() {
        encoded.push(char::from_digit(u32::from(byte >> 4), 16).expect("nibble is a hex digit"));
        encoded.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("nibble is a hex digit"));
    }
    if encoded.len() > MAX_NAME_BYTES {
        return Err(AcceptError::EventIdTooLong {
            encoded: encoded.len(),
        });
    }
    Ok(encoded)
}

fn acceptance_receipt(event: &Event) -> AcceptanceReceipt {
    AcceptanceReceipt {
        event_id: event.id.0.clone(),
        receipt_id: format!("recipient-accepted:{}", event.id.0),
    }
}

/// Test recipient whose effect records outlive any one courier attempt.
///
/// Reconstructing this adapter from the same root models a fresh courier
/// attempt, and a restarted recipient process, without making the process-local
/// [`MemoryStore`] durable.
struct EffectStore {
    root: PathBuf,
}

/// Result of the atomic link step, before the record is validated.
enum Commit {
    Linked,
    AlreadyExists,
}

impl EffectStore {
    /// Opens the store, creating its layout and scavenging crashed staging files.
    fn open(root: &Path) -> Result<Self, AcceptError> {
        Self::open_with(root, DEFAULT_STALE_AFTER)
    }

    fn open_with(root: &Path, stale_after: Duration) -> Result<Self, AcceptError> {
        let store = Self {
            root: root.to_path_buf(),
        };
        fs::create_dir_all(store.effects_dir())?;
        fs::create_dir_all(store.staging_dir())?;
        fs::create_dir_all(store.duplicates_dir())?;
        sync_dir(&store.root)?;
        store.scavenge(stale_after)?;
        Ok(store)
    }

    fn effects_dir(&self) -> PathBuf {
        self.root.join("effects")
    }

    fn staging_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    fn duplicates_dir(&self) -> PathBuf {
        self.root.join("duplicates")
    }

    fn record_path(&self, id: &EventId) -> Result<PathBuf, AcceptError> {
        Ok(self.effects_dir().join(encode_event_id(id)?))
    }

    /// Removes staging files that no in-flight attempt can still own.
    ///
    /// A staging file is only ever linked into `effects/` after its own fsync,
    /// so anything left in `tmp/` is either an attempt in progress or debris
    /// from a crashed one. Age discriminates: entries untouched for at least
    /// `stale_after` are removed, so a concurrent courier's staging file is
    /// never deleted out from under it. Without this, crashed attempts would
    /// accumulate in `tmp/` forever.
    fn scavenge(&self, stale_after: Duration) -> Result<usize, AcceptError> {
        let mut removed = 0;
        for entry in fs::read_dir(self.staging_dir())? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            let age = metadata
                .modified()
                .ok()
                .and_then(|modified| SystemTime::now().duration_since(modified).ok());
            // An unreadable or future-dated mtime is treated as stale rather
            // than as a reason to keep debris forever.
            if age.is_none_or(|age| age >= stale_after) && fs::remove_file(entry.path()).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Records the effect for `event` exactly once, keyed by [`EventId`].
    fn accept(&self, event: &Event) -> Result<AcceptanceOutcome, AcceptError> {
        let target = self.record_path(&event.id)?;
        let receipt = acceptance_receipt(event);
        match self.commit(&target, &receipt)? {
            Commit::Linked => Ok(AcceptanceOutcome::Recorded(receipt)),
            Commit::AlreadyExists => {
                // The filename alone is not a durable receipt: a path can exist
                // with a truncated, empty, or corrupt body. Read it, parse it,
                // and confirm it names this event before calling it acceptance.
                let existing = self.read_record(&target)?;
                if existing.event_id != event.id.0 {
                    return Err(AcceptError::UnusableRecord {
                        path: target,
                        reason: format!(
                            "record names event {:?}, not {:?}",
                            existing.event_id, event.id.0
                        ),
                    });
                }
                Ok(AcceptanceOutcome::Existing(existing))
            }
        }
    }

    /// Negative control: the identical commit protocol with the [`EventId`]
    /// keying removed, so every attempt lands under a fresh name.
    fn accept_without_dedup(&self, event: &Event) -> Result<AcceptanceReceipt, AcceptError> {
        let receipt = acceptance_receipt(event);
        let target = self
            .duplicates_dir()
            .join(format!("d{}", STAGING_ID.fetch_add(1, Ordering::Relaxed)));
        match self.commit(&target, &receipt)? {
            Commit::Linked => Ok(receipt),
            Commit::AlreadyExists => Err(AcceptError::UnusableRecord {
                path: target,
                reason: "unkeyed duplicate name was reused".into(),
            }),
        }
    }

    /// The five-step commit. `target`'s parent directory must already exist.
    fn commit(&self, target: &Path, receipt: &AcceptanceReceipt) -> Result<Commit, AcceptError> {
        // 1. unique staging file on the same filesystem, via create_new
        let staging = self.staging_dir().join(format!(
            "s{}-{}",
            std::process::id(),
            STAGING_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staging)?;

        // 2. write the full record, then 3. sync the FILE
        let staged = serde_json::to_vec(receipt)
            .map_err(io::Error::other)
            .and_then(|mut bytes| {
                bytes.push(b'\n');
                file.write_all(&bytes)?;
                file.sync_all()
            });
        drop(file);
        if let Err(error) = staged {
            let _ = fs::remove_file(&staging);
            return Err(AcceptError::Io(error));
        }

        // 4. atomically link into place; fails if the target already exists
        let linked = fs::hard_link(&staging, target);
        let parent = target.parent().expect("record path has a parent directory");
        let result = match linked {
            // 5. sync the DIRECTORY so the new entry survives a crash, before
            //    the staging copy that could have replaced it goes away.
            Ok(()) => sync_dir(parent)
                .map(|()| Commit::Linked)
                .map_err(Into::into),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(Commit::AlreadyExists),
            Err(error) => Err(AcceptError::Io(error)),
        };
        // Clean up staging on every outcome, so crashed attempts are the only
        // thing scavenging ever has to handle.
        let _ = fs::remove_file(&staging);
        result
    }

    fn read_record(&self, path: &Path) -> Result<AcceptanceReceipt, AcceptError> {
        let bytes = fs::read(path).map_err(|error| AcceptError::UnusableRecord {
            path: path.to_path_buf(),
            reason: format!("unreadable: {error}"),
        })?;
        serde_json::from_slice(&bytes).map_err(|error| AcceptError::UnusableRecord {
            path: path.to_path_buf(),
            reason: format!("not a complete record: {error}"),
        })
    }

    /// Reads recorded effects from the `effects/` **directory**.
    ///
    /// Never a log tail: a torn write can only exist in `tmp/`, so it is not
    /// visible here and cannot make this call fail for every future redelivery.
    fn receipts(&self) -> Result<Vec<AcceptanceReceipt>, AcceptError> {
        self.read_dir_records(&self.effects_dir())
    }

    fn duplicate_receipts(&self) -> Result<Vec<AcceptanceReceipt>, AcceptError> {
        self.read_dir_records(&self.duplicates_dir())
    }

    fn read_dir_records(&self, dir: &Path) -> Result<Vec<AcceptanceReceipt>, AcceptError> {
        let mut records = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            records.push(self.read_record(&entry.path())?);
        }
        records.sort_by(|a, b| (&a.event_id, &a.receipt_id).cmp(&(&b.event_id, &b.receipt_id)));
        Ok(records)
    }

    fn staging_entries(&self) -> Result<usize, AcceptError> {
        Ok(fs::read_dir(self.staging_dir())?.count())
    }
}

struct ScratchRoot(PathBuf);

impl ScratchRoot {
    fn create() -> io::Result<Self> {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "urgent-message-{}-{}",
            std::process::id(),
            SCRATCH_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn urgent_message() -> NewEvent {
    NewEvent {
        id: EventId("urgent-message-2026-08-03-001".into()),
        topic: Topic(TOPIC.into()),
        idempotency_key: IdempotencyKey("urgent-message-2026-08-03-001".into()),
        payload: Payload::from_bytes(b"the east gate is open".to_vec()),
    }
}

fn event_named(id: &str) -> NewEvent {
    NewEvent {
        id: EventId(id.into()),
        topic: Topic(TOPIC.into()),
        idempotency_key: IdempotencyKey(id.into()),
        payload: Payload::from_bytes(b"the east gate is open".to_vec()),
    }
}

fn claim_after_expiry(store: &MemoryStore, clock: &ManualClock, first_owner: &ConsumerId) -> Lease {
    clock.set(10);
    let next_owner = if first_owner.0 == "courier-a" {
        ConsumerId("courier-b".into())
    } else {
        ConsumerId("courier-a".into())
    };
    store
        .claim(&next_owner, &Topic(TOPIC.into()), Duration::from_millis(10))
        .expect("reclaim should be authorized")
        .expect("expired urgent message should be redelivered")
}

/// The durable once-only contract, end to end.
///
/// Gated to unix: this is the platform where the directory sync that makes a
/// committed record crash-durable is actually implemented, so it is the only
/// platform where asserting the durable contract would be honest.
#[cfg(unix)]
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
                store.claim_detailed(
                    &ConsumerId(name.into()),
                    &Topic(TOPIC.into()),
                    Duration::from_millis(10),
                )
            })
        })
        .collect();
    let outcomes = handles
        .into_iter()
        .map(|handle| handle.join().expect("courier thread should not panic"))
        .collect::<Result<Vec<_>, _>>()?;

    let first_lease = outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            ClaimOutcome::Granted(lease) => Some(lease.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(first_lease.len(), 1, "exactly one courier holds the lease");
    let first_lease = first_lease.into_iter().next().expect("checked above");
    assert_eq!(first_lease.event, appended);

    // The loser is told who holds the message, under which fence, and when that
    // hold lapses — enough to schedule a retry instead of spinning blindly.
    let denied: Vec<_> = outcomes
        .iter()
        .filter(|outcome| !matches!(outcome, ClaimOutcome::Granted(_)))
        .collect();
    assert_eq!(denied.len(), 1);
    assert_eq!(
        denied[0],
        &ClaimOutcome::Contended {
            event_id: appended.id.clone(),
            holder: first_lease.owner.clone(),
            fence: first_lease.fence,
            expires_at: first_lease.expires_at,
        }
    );

    let scratch = ScratchRoot::create()?;
    let first_recipient = EffectStore::open(scratch.path())?;
    let first_receipt = match first_recipient.accept(&first_lease.event)? {
        AcceptanceOutcome::Recorded(receipt) => receipt,
        AcceptanceOutcome::Existing(_) => return Err("first acceptance was not new".into()),
    };
    assert_eq!(first_recipient.receipts()?, vec![first_receipt.clone()]);
    assert_eq!(first_recipient.staging_entries()?, 0);

    // The courier loses the queue ACK after the recipient has durably accepted
    // the message. Expiry therefore causes an intentional at-least-once retry.
    let second_lease = claim_after_expiry(&store, &clock, &first_lease.owner);
    assert_eq!(second_lease.event, appended);
    assert!(second_lease.fence > first_lease.fence);
    assert_eq!(store.ack_work(&first_lease), Err(Error::StaleFence));

    // A fresh recipient process, reading the same root.
    let redelivery_recipient = EffectStore::open(scratch.path())?;
    assert_eq!(
        redelivery_recipient.accept(&second_lease.event)?,
        AcceptanceOutcome::Existing(first_receipt.clone())
    );
    assert_eq!(redelivery_recipient.receipts()?, vec![first_receipt]);
    assert_eq!(redelivery_recipient.staging_entries()?, 0);

    let queue_ack = store.ack_work(&second_lease)?;
    assert_eq!(queue_ack.event_id, appended.id);
    assert_eq!(queue_ack.fence, second_lease.fence);

    // Acknowledged work is terminal: no grant, and no holder disclosed either.
    assert_eq!(
        store.claim_detailed(
            &ConsumerId("courier-c".into()),
            &Topic(TOPIC.into()),
            Duration::from_millis(10),
        )?,
        ClaimOutcome::Empty
    );
    assert_eq!(
        store.claim(
            &ConsumerId("courier-c".into()),
            &Topic(TOPIC.into()),
            Duration::from_millis(10),
        )?,
        None
    );
    Ok(())
}

/// A stale courier and the courier that superseded it, released together.
///
/// Both observe the message as undelivered at the same instant. The link step
/// is the dedup decision *and* the effect record, so exactly one can win
/// regardless of interleaving. The round is repeated on a fresh root each time
/// because one barrier release only samples one interleaving; a check-then-append
/// recipient duplicates on some fraction of these rounds rather than on all.
#[cfg(unix)]
#[test]
fn simultaneous_couriers_record_the_effect_exactly_once() -> Result<(), Box<dyn StdError>> {
    const ROUNDS: usize = 128;

    for round in 0..ROUNDS {
        let clock = Arc::new(ManualClock::default());
        let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
        let appended = match store.append("dispatcher", urgent_message())? {
            AppendOutcome::Appended(event) => event,
            AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
        };

        let stale_lease = store
            .claim(
                &ConsumerId("courier-a".into()),
                &Topic(TOPIC.into()),
                Duration::from_millis(10),
            )?
            .expect("urgent message should be claimable");
        let fresh_lease = claim_after_expiry(&store, &clock, &stale_lease.owner);
        assert!(fresh_lease.fence > stale_lease.fence);
        assert_eq!(store.ack_work(&stale_lease), Err(Error::StaleFence));

        let scratch = ScratchRoot::create()?;
        let recipient = Arc::new(EffectStore::open(scratch.path())?);
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [stale_lease, fresh_lease]
            .into_iter()
            .map(|lease| {
                let recipient = recipient.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    recipient.accept(&lease.event)
                })
            })
            .collect();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().expect("recipient thread should not panic"))
            .collect::<Result<Vec<_>, _>>()?;

        let recorded = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptanceOutcome::Recorded(_)))
            .count();
        let existing = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptanceOutcome::Existing(_)))
            .count();
        assert_eq!(recorded, 1, "round {round}: exactly one courier records");
        assert_eq!(existing, 1, "round {round}: the other observes the record");

        let receipts = recipient.receipts()?;
        assert_eq!(receipts.len(), 1, "round {round}: one record on disk");
        assert_eq!(receipts[0], acceptance_receipt(&appended));
        assert_eq!(recipient.staging_entries()?, 0);
    }
    Ok(())
}

/// An attempt that died mid-write leaves debris in `tmp/`, never in `effects/`.
#[cfg(unix)]
#[test]
fn interrupted_record_does_not_wedge_redelivery() -> Result<(), Box<dyn StdError>> {
    let clock = Arc::new(ManualClock::default());
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("first append unexpectedly found an event".into()),
    };
    let lease = store
        .claim(
            &ConsumerId("courier-a".into()),
            &Topic(TOPIC.into()),
            Duration::from_millis(10),
        )?
        .expect("urgent message should be claimable");

    let scratch = ScratchRoot::create()?;
    let crashed = EffectStore::open(scratch.path())?;

    // A courier that died between "write the record" and "link it into place".
    let torn = crashed.staging_dir().join("s-crashed-mid-write");
    fs::write(&torn, br#"{"event_id":"urgent-message-2026-08-03-0"#)?;
    assert!(torn.exists());
    // A separately corrupted attempt: an empty staging file.
    let empty = crashed.staging_dir().join("s-crashed-before-write");
    fs::write(&empty, b"")?;

    // Reading effects is unaffected by either: they are not effects.
    assert_eq!(crashed.receipts()?, vec![]);

    // A fresh recipient process scavenges the debris and proceeds normally.
    let reopened = EffectStore::open_with(scratch.path(), Duration::ZERO)?;
    assert!(!torn.exists(), "stale staging file should be scavenged");
    assert!(!empty.exists(), "stale staging file should be scavenged");
    assert_eq!(reopened.staging_entries()?, 0);

    let receipt = match reopened.accept(&lease.event)? {
        AcceptanceOutcome::Recorded(receipt) => receipt,
        AcceptanceOutcome::Existing(_) => {
            return Err("torn debris was misread as a prior acceptance".into());
        }
    };
    assert_eq!(receipt, acceptance_receipt(&appended));

    // Redelivery after the interruption still yields the same receipt.
    let second_lease = claim_after_expiry(&store, &clock, &lease.owner);
    let redelivery = EffectStore::open(scratch.path())?;
    assert_eq!(
        redelivery.accept(&second_lease.event)?,
        AcceptanceOutcome::Existing(receipt.clone())
    );
    assert_eq!(redelivery.receipts()?, vec![receipt]);
    store.ack_work(&second_lease)?;
    Ok(())
}

/// Hostile ids must not escape `effects/`, collide, or read as each other.
#[test]
fn hostile_event_ids_are_confined_and_never_collide() -> Result<(), Box<dyn StdError>> {
    let hostile = [
        "../../../etc/passwd",
        "..",
        ".",
        "",
        "urgent/../urgent",
        "effects",
        "a b\tc\nd",
        "\u{202e}gnp.exe",
        "café-☕",
    ];

    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));

    let mut names = Vec::new();
    for id in hostile {
        let appended = match store.append("dispatcher", event_named(id))? {
            AppendOutcome::Appended(event) => event,
            AppendOutcome::Existing(_) => return Err("append should be new for each id".into()),
        };
        let path = recipient.record_path(&appended.id)?;

        assert_eq!(
            recipient.accept(&appended)?,
            AcceptanceOutcome::Recorded(acceptance_receipt(&appended))
        );
        // Confinement: the record is a direct child of effects/, nowhere else.
        assert_eq!(path.parent(), Some(recipient.effects_dir().as_path()));
        assert!(path.is_file(), "record for {id:?} should exist in effects/");
        names.push(path);

        // Re-delivering the same hostile id dedups against its own record only.
        assert_eq!(
            recipient.accept(&appended)?,
            AcceptanceOutcome::Existing(acceptance_receipt(&appended))
        );
    }

    let mut unique = names.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), hostile.len(), "encoding must be injective");
    assert_eq!(recipient.receipts()?.len(), hostile.len());
    // Nothing escaped: effects/ holds exactly the records and no strays exist.
    assert_eq!(
        fs::read_dir(recipient.effects_dir())?.count(),
        hostile.len()
    );
    assert_eq!(recipient.staging_entries()?, 0);
    Ok(())
}

/// An id too long to encode is refused up front, not silently truncated.
#[test]
fn oversized_event_id_is_refused_rather_than_truncated() -> Result<(), Box<dyn StdError>> {
    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;
    let long = EventId("x".repeat(MAX_NAME_BYTES));
    assert!(matches!(
        recipient.record_path(&long),
        Err(AcceptError::EventIdTooLong { .. })
    ));
    Ok(())
}

/// A damaged record on the `AlreadyExists` path is never reported as acceptance.
#[test]
fn corrupt_existing_record_is_not_reported_as_prior_acceptance() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("first append unexpectedly found an event".into()),
    };

    for (label, damage) in [
        ("truncated", &br#"{"event_id":"urgent-mes"#[..]),
        ("empty", &b""[..]),
        (
            "wrong event",
            &br#"{"event_id":"someone-else","receipt_id":"x"}"#[..],
        ),
    ] {
        let scratch = ScratchRoot::create()?;
        let recipient = EffectStore::open(scratch.path())?;
        assert!(matches!(
            recipient.accept(&appended)?,
            AcceptanceOutcome::Recorded(_)
        ));

        fs::write(recipient.record_path(&appended.id)?, damage)?;
        let error = recipient
            .accept(&appended)
            .expect_err("a {label} record must not read as prior acceptance");
        assert!(
            matches!(error, AcceptError::UnusableRecord { .. }),
            "{label} record produced {error:?}"
        );
        assert_eq!(recipient.staging_entries()?, 0, "{label} leaked staging");
    }
    Ok(())
}

/// Negative control: the same commit protocol, minus [`EventId`] keying, really
/// does record the effect twice. Dedup is doing the work, not the filesystem.
#[test]
fn redelivery_duplicates_recipient_effect_without_event_id_dedup() -> Result<(), Box<dyn StdError>>
{
    let clock = Arc::new(ManualClock::default());
    let store = MemoryStore::with_clock(clock.clone(), Arc::new(AllowAll));
    store.append("dispatcher", urgent_message())?;
    let first_lease = store
        .claim(
            &ConsumerId("courier-a".into()),
            &Topic(TOPIC.into()),
            Duration::from_millis(10),
        )?
        .expect("urgent message should be claimable");

    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;
    recipient.accept_without_dedup(&first_lease.event)?;

    let second_lease = claim_after_expiry(&store, &clock, &first_lease.owner);
    recipient.accept_without_dedup(&second_lease.event)?;
    store.ack_work(&second_lease)?;

    let receipts = recipient.duplicate_receipts()?;
    assert_eq!(
        receipts.len(),
        2,
        "undeduplicated acceptance must duplicate"
    );
    assert_eq!(receipts[0].event_id, receipts[1].event_id);
    // The deduplicated view of the same two attempts holds nothing at all.
    assert_eq!(recipient.receipts()?, vec![]);
    Ok(())
}
