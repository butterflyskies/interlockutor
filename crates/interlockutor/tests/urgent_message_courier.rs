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
//!
//! # Failure semantics, per step
//!
//! The happy path above was specified first and its failure behaviour was left
//! implied. Three defects came out of that gap. The protocol's per-step failure
//! semantics are therefore stated here rather than inferred, including the cells
//! that are argued rather than tested.
//!
//! | step | on-disk state if it fails | what a retry observes | terminal state | covered by |
//! |---|---|---|---|---|
//! | 1. `create_new` staging | nothing: the name is either unused or already taken by a live attempt | `effects/` unchanged | attempt fails with `Io`; nothing is accepted | argued, not injected — see *Untested cells* |
//! | 2. write record | partial bytes under `tmp/` only; removed on the error path, and any leak is reaped by age | `effects/` unchanged; torn bytes are never readable as an effect | correct: redelivery re-stages and links | `interrupted_record_does_not_wedge_redelivery` |
//! | 3. `sync_all` file | staging file with unflushed bytes; removed on the error path | identical to step 2 — the debris shape is the same | correct | debris shape covered by the step-2 test; the fsync error itself is argued |
//! | 4. `hard_link` | `AlreadyExists` means another attempt's entry is present; any other error leaves no entry | `AlreadyExists` → durability repair, then full-receipt validation | correct | `simultaneous_couriers_record_the_effect_exactly_once`, `corrupt_existing_record_is_not_reported_as_prior_acceptance` |
//! | 5. `sync_all` `effects/` | **entry is visible but may not be crash-durable**, and `commit` returns `Err` | `AlreadyExists`; the retry re-fsyncs `effects/` *before* validating, and propagates a repeated failure | correct — previously a courier could ACK a non-durable entry | `post_link_directory_sync_failure_is_repaired_before_existing_is_returned` |
//! | 6. remove staging | the record is committed but debris remains in `tmp/` | the record is already in `effects/`, so redelivery reports `Existing` | attempt reports `Err` over a durable record; conservative, never duplicated | `a_failed_staging_cleanup_is_reported_rather_than_swallowed`, `a_cleanup_failure_never_masks_the_primary_failure` |
//! | scavenge, known age | a **live** staging name can be removed when an attempt outlives `stale_after` | that attempt's `hard_link` fails with `NotFound` | attempt fails loudly; `effects/` is never wrong and never duplicated | `live_staging_file_may_be_reaped_and_the_attempt_fails_loudly` |
//! | scavenge, unknown age, concurrent | unchanged: the entry is left exactly where it is and counted | nothing: the owner's staging name is still its own | correct — an independently-owned attempt commits normally | `concurrent_scavenging_leaves_an_undated_stage_for_its_owner` |
//! | scavenge, unknown age, exclusive | the entry is moved to `quarantine/`, never deleted | staging name is gone | the caller asserted no attempt was in flight, so there is nothing to break | `future_dated_staging_files_are_quarantined_rather_than_reaped` |
//! | `Existing` validation | — | a record at the right path is only acceptance if it equals the expected receipt in full | mismatch is `UnusableRecord`, never `Existing`, never repaired in place | `corrupt_existing_record_is_not_reported_as_prior_acceptance` |
//!
//! ## Untested cells, stated rather than smoothed over
//!
//! - **Step 1 and step 3 failures are argued, not injected.** Both are `io`
//!   failures with no distinguishing on-disk residue: step 1 leaves nothing, and
//!   step 3 leaves exactly the debris shape step 2 already produces and that the
//!   torn-write test already covers. Injecting them would exercise the same two
//!   recovery paths again, so they are reasoned about here instead.
//! - **Crash durability is not demonstrated.** No in-process test can pull
//!   power. The fsync steps are implemented to the standard commit protocol and
//!   argued; nothing here proves them.
//! - **The scavenger's reap window is a heuristic, not a proof of abandonment.**
//!   See [`EffectStore::scavenge`].
//!
//! # Scavenging: age is a heuristic
//!
//! An earlier version of this adapter claimed a concurrent courier's staging file
//! was "never" deleted. That was untrue. Age does not establish abandonment: an
//! attempt that pauses longer than `stale_after` between staging and linking can
//! have its live staging name removed by a concurrent reaper. The claim is now
//! narrowed to match the mechanism, and zero-age reaping is confined to an
//! explicitly exclusive entry point. See [`EffectStore::scavenge`].
//!
//! Age being the only evidence cuts both ways, and the scavenger used to get the
//! second direction backwards: an entry whose mtime was unreadable or dated in
//! the future was treated as *maximally* stale and deleted on the spot, at any
//! window, bypassing the `MIN_CONCURRENT_STALE_AFTER` floor entirely. No
//! evidence of age is not evidence of abandonment — a future-dated file is a
//! clock step or a nonmonotonic filesystem, and is at least as likely to be live
//! work. Only known ages meeting the threshold are reaped.
//!
//! Correcting that belief left a second defect behind it, because the belief and
//! the action had come apart. Having concluded "no evidence of abandonment", the
//! scavenger still *renamed* the entry into `quarantine/` — and a rename takes
//! the staging name away from its owner exactly as a delete does, so the live
//! attempt's `hard_link` failed with `NotFound` all the same. Right belief,
//! wrong act, and under a concurrently-opened store it broke an independently
//! owned in-flight commit on no evidence at all.
//!
//! What may be done with an undated entry is therefore decided by exclusivity,
//! not by the entry. A store opened for concurrent use leaves it untouched and
//! reports it. Only [`EffectStore::open_exclusive`], where the caller asserts
//! that no other attempt is in flight, may move it out of the staging namespace.

use interlockutor::{
    AllowAll, AppendOutcome, ClaimOutcome, Clock, ConsumerId, Error, Event, EventId, EventStore,
    EventStoreExt, IdempotencyKey, Lease, MemoryStore, NewEvent, Payload, Topic,
};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime};

static SCRATCH_ID: AtomicU64 = AtomicU64::new(0);
static STAGING_ID: AtomicU64 = AtomicU64::new(0);

/// Conservative default: only debris no in-flight attempt can still own.
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(3600);

/// Floor on the reap window for any store that may be used concurrently.
///
/// Age is the only evidence this adapter has, so the window has to be longer
/// than the longest plausible attempt. Below this floor the heuristic stops
/// approximating abandonment at all, so [`EffectStore::open_with`] refuses it
/// rather than letting a caller opt into reaping live work by accident.
const MIN_CONCURRENT_STALE_AFTER: Duration = Duration::from_secs(60);

/// Filenames must fit the common single-component limit of 255 bytes.
const MAX_NAME_BYTES: usize = 255;

/// How many colliding quarantine names to try before giving up and reporting.
///
/// Bounded rather than unbounded so a pathological directory cannot make a
/// scavenging pass spin forever. Exhausting it is reported, never resolved by
/// overwriting something.
const QUARANTINE_NAME_ATTEMPTS: u32 = 1024;

const TOPIC: &str = "urgent-messages";

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
    /// or is not the receipt this event should have produced.
    ///
    /// This is explicitly **not** a prior acceptance. The recipient cannot know
    /// whether the effect behind an unreadable record was performed, so it
    /// refuses rather than guessing, and the courier must not acknowledge the
    /// queue item. Reporting this as `Existing` would convert media corruption
    /// into silent effect loss; repairing it in place would convert it into
    /// silent effect duplication.
    ///
    /// `source` keeps the underlying parse or I/O failure attached rather than
    /// flattening it into `reason`, so callers can still walk the error chain.
    UnusableRecord {
        path: PathBuf,
        reason: String,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    },
    /// A reap window short enough to delete live staging files was requested for
    /// a store that may be used concurrently.
    ScavengeWindowTooShort {
        requested: Duration,
        minimum: Duration,
    },
    Io(io::Error),
}

impl AcceptError {
    fn unusable(
        path: &Path,
        reason: String,
        source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    ) -> Self {
        Self::UnusableRecord {
            path: path.to_path_buf(),
            reason,
            source,
        }
    }
}

impl fmt::Display for AcceptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EventIdTooLong { encoded } => write!(
                f,
                "encoded event ID is {encoded} bytes, over the {MAX_NAME_BYTES}-byte name limit"
            ),
            Self::UnusableRecord { path, reason, .. } => {
                write!(
                    f,
                    "existing record at {} is unusable: {reason}",
                    path.display()
                )
            }
            Self::ScavengeWindowTooShort { requested, minimum } => write!(
                f,
                "scavenge window {requested:?} is below the {minimum:?} floor for concurrent use"
            ),
            Self::Io(error) => write!(f, "recipient I/O failed: {error}"),
        }
    }
}

impl StdError for AcceptError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::UnusableRecord { source, .. } => source
                .as_ref()
                .map(|error| &**error as &(dyn StdError + 'static)),
            Self::EventIdTooLong { .. } | Self::ScavengeWindowTooShort { .. } => None,
        }
    }
}

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
/// stored in the record body, and the whole record is re-verified on every
/// `Existing` path, so an encoding accident could not silently read as
/// acceptance of a different event.
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

/// Injectable failures, so recovery paths can be exercised rather than argued.
///
/// Held per store rather than in a global, so one store's injected fault cannot
/// leak into another test whether the harness isolates by process or by thread.
/// A store reopened from the same root models a fresh process and starts clean,
/// which is what makes "retry after a failed step" expressible.
#[derive(Default)]
struct Faults {
    dir_syncs_to_fail: AtomicU64,
    staging_removals_to_fail: AtomicU64,
}

/// What the scavenger may do with an entry whose age it cannot establish.
///
/// Unknown age is *no evidence of abandonment*, and that belief has to govern
/// the action as well as the classification. Moving an entry out of the staging
/// namespace takes the name away from whoever owns it just as surely as deleting
/// it does: the owner's `hard_link` then fails with `NotFound`. So the
/// distinction is not "delete versus preserve", it is **who is allowed to touch
/// live work at all**.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UndatedPolicy {
    /// Concurrent use: leave the entry exactly where it is and count it.
    ///
    /// Another attempt may own it right now, and there is no evidence either
    /// way. Reporting is the only action the evidence supports.
    Report,
    /// Exclusive recovery: move the entry to `quarantine/`.
    ///
    /// Licensed only by the caller's assertion in [`EffectStore::open_exclusive`]
    /// that no other attempt is in flight, which is what makes "this is debris"
    /// a fact supplied from outside rather than an inference from age.
    Quarantine,
}

/// What one scavenging pass did, split by the evidence it had.
///
/// The counts are kept apart because they answer different questions. `removed`
/// is the only one age licenses on its own; `undated_left_in_place` is the
/// operator-visible signal that entries exist which nothing here can classify.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ScavengeReport {
    /// Entries whose age was known and at least `stale_after`.
    removed: usize,
    /// Undated entries moved to `quarantine/`, under [`UndatedPolicy::Quarantine`].
    quarantined: usize,
    /// Undated entries left untouched, under [`UndatedPolicy::Report`].
    undated_left_in_place: usize,
}

/// Test recipient whose effect records outlive any one courier attempt.
///
/// Reconstructing this adapter from the same root models a fresh courier
/// attempt, and a restarted recipient process, without making the process-local
/// [`MemoryStore`] durable.
struct EffectStore {
    root: PathBuf,
    undated: UndatedPolicy,
    faults: Faults,
}

/// Result of the atomic link step, before the record is validated.
enum Commit {
    Linked,
    AlreadyExists,
}

impl EffectStore {
    /// Opens the store with the conservative default reap window.
    fn open(root: &Path) -> Result<Self, AcceptError> {
        Self::open_with(root, DEFAULT_STALE_AFTER)
    }

    /// Opens a store that may be used concurrently with other attempts.
    ///
    /// Refuses a reap window below [`MIN_CONCURRENT_STALE_AFTER`]: see
    /// [`EffectStore::scavenge`] for why a short window is not a safe knob.
    ///
    /// Undated entries are reported, never moved. This store has no exclusivity
    /// to trade on, so it has no licence to touch an entry it cannot classify.
    fn open_with(root: &Path, stale_after: Duration) -> Result<Self, AcceptError> {
        if stale_after < MIN_CONCURRENT_STALE_AFTER {
            return Err(AcceptError::ScavengeWindowTooShort {
                requested: stale_after,
                minimum: MIN_CONCURRENT_STALE_AFTER,
            });
        }
        Self::open_unchecked(root, stale_after, UndatedPolicy::Report)
    }

    /// Opens a store for recovery, with **no** floor on the reap window.
    ///
    /// The caller asserts that no other attempt is in flight against this root.
    /// That assertion is what licenses reaping by age at all — it replaces the
    /// heuristic with an actual exclusivity guarantee supplied from outside.
    /// Nothing in this adapter checks it, so it is a named, deliberate handoff
    /// rather than a silent assumption.
    ///
    /// The same assertion is what licenses moving an *undated* entry to
    /// `quarantine/`. Absent it, an undated entry may be somebody's live stage
    /// and taking its name away breaks that attempt exactly as deleting it would.
    fn open_exclusive(root: &Path, stale_after: Duration) -> Result<Self, AcceptError> {
        Self::open_unchecked(root, stale_after, UndatedPolicy::Quarantine)
    }

    fn open_unchecked(
        root: &Path,
        stale_after: Duration,
        undated: UndatedPolicy,
    ) -> Result<Self, AcceptError> {
        let store = Self {
            root: root.to_path_buf(),
            undated,
            faults: Faults::default(),
        };
        fs::create_dir_all(store.effects_dir())?;
        fs::create_dir_all(store.staging_dir())?;
        fs::create_dir_all(store.duplicates_dir())?;
        fs::create_dir_all(store.quarantine_dir())?;
        store.sync_dir(&store.root)?;
        store.scavenge(stale_after)?;
        Ok(store)
    }

    /// Makes the next `n` directory syncs on this store fail.
    fn fail_next_dir_syncs(&self, n: u64) {
        self.faults.dir_syncs_to_fail.store(n, Ordering::SeqCst);
    }

    /// Makes the next `n` staging-file removals on this store fail.
    fn fail_next_staging_removals(&self, n: u64) {
        self.faults
            .staging_removals_to_fail
            .store(n, Ordering::SeqCst);
    }

    /// Unlinks a name, treating `NotFound` as the postcondition already met.
    ///
    /// The postcondition is **the name is gone**, not *this caller removed it*.
    /// A concurrent scavenger is entitled to reap a staging name at any moment,
    /// so treating "someone else already removed it" as a failure would report
    /// an error for an operation that fully succeeded. Every unlink in this
    /// adapter goes through here, so that rule has one definition rather than
    /// one per call site — and no call site gets to quietly discard the outcome.
    ///
    /// `live_staging_file_may_be_reaped_and_the_attempt_fails_loudly` exercises
    /// the `NotFound` case on its concurrent path.
    fn remove_name(path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    /// Removes an attempt's own staging file, reporting failure instead of
    /// discarding it.
    ///
    /// This was `let _ = fs::remove_file(staging);` on every path. The module
    /// docs promise the staging file is "always removed, on every outcome, so
    /// crashed attempts are the only thing scavenging ever has to handle" — and
    /// a discarded error silently falsified exactly that promise, leaving debris
    /// that looks identical to a crashed attempt with nothing having reported a
    /// problem.
    ///
    /// Fault injection is deliberately confined to this entry point rather than
    /// to [`EffectStore::remove_name`]: the injected faults model an attempt's
    /// cleanup failing, and arming them must not also break the scavenger that
    /// every `open` runs.
    fn remove_staging(&self, staging: &Path) -> io::Result<()> {
        if self
            .faults
            .staging_removals_to_fail
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(io::Error::other("injected staging removal failure"));
        }
        Self::remove_name(staging)
    }

    /// Flushes a directory's entries to stable storage.
    ///
    /// On unix this is a real `fsync` of the directory. Without it, a hard link
    /// is atomically *visible* but not crash-durable: the new entry can still
    /// vanish on power loss. There is no portable equivalent of this call, so on
    /// every other platform it is a deliberate no-op, and the crash-durability
    /// half of the recipient contract does not hold there. Atomicity and
    /// visibility still do, because those come from `hard_link` itself.
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        if self
            .faults
            .dir_syncs_to_fail
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(io::Error::other("injected directory sync failure"));
        }
        Self::sync_dir_uninjected(path)
    }

    #[cfg(unix)]
    fn sync_dir_uninjected(path: &Path) -> io::Result<()> {
        File::open(path)?.sync_all()
    }

    #[cfg(not(unix))]
    fn sync_dir_uninjected(_path: &Path) -> io::Result<()> {
        Ok(())
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

    /// Holds staging entries whose age could not be established. Never read as
    /// an effect, never reaped by age — see [`EffectStore::scavenge`].
    fn quarantine_dir(&self) -> PathBuf {
        self.root.join("quarantine")
    }

    /// Shared append log used only by the racy negative control.
    fn naive_log(&self) -> PathBuf {
        self.root.join("naive-log")
    }

    fn record_path(&self, id: &EventId) -> Result<PathBuf, AcceptError> {
        Ok(self.effects_dir().join(encode_event_id(id)?))
    }

    /// Removes staging files that are *probably* abandoned.
    ///
    /// A staging file is only ever linked into `effects/` after its own fsync,
    /// so anything left in `tmp/` is either an attempt in progress or debris
    /// from a crashed one. This adapter cannot tell those apart: it has no
    /// liveness marker, no advisory lock, and no ownership claim on the name.
    ///
    /// **Age is a heuristic, not proof of abandonment.** Entries untouched for
    /// at least `stale_after` are removed. That is only approximately correct,
    /// and it rests on a stated assumption: *no single accept attempt holds a
    /// staging file for longer than `stale_after` between creating it and
    /// linking it.* An attempt that violates the assumption can have its live
    /// staging name reaped, after which its `hard_link` fails with `NotFound`.
    ///
    /// The failure is conservative, not corrupting: the attempt fails loudly and
    /// records nothing, so the outcome is an avoidable retry, never a duplicated
    /// or lost effect. [`EffectStore::open_with`] enforces a floor on the window
    /// so the assumption is at least plausible; [`EffectStore::open_exclusive`]
    /// is the named escape hatch for recovery-time reaping, where the caller
    /// supplies exclusivity instead.
    ///
    /// Making this exact rather than heuristic needs real ownership evidence —
    /// an advisory lock, a liveness marker, or linking from an open descriptor
    /// so the pathname stops mattering. That is deliberately not done here.
    /// # Unknown and future-dated mtimes are never reaped, and only moved under
    /// exclusivity
    ///
    /// Age is the only evidence, so an entry whose age cannot be *established*
    /// carries no evidence at all. This once treated those entries as stale and
    /// deleted them immediately — the exact opposite of what the evidence
    /// supports, and a bypass of the `stale_after` floor that
    /// [`EffectStore::open_with`] exists to enforce. A file with an unreadable
    /// mtime, or one dated in the future because of a clock step or a
    /// nonmonotonic filesystem, is *more* likely to be live work than debris,
    /// and a concurrently-opened store would delete it at age zero.
    ///
    /// Correcting the belief was not enough, because the *action* still moved
    /// the entry: a rename into `quarantine/` takes the staging name away from
    /// whoever owns it, and the owner's later `hard_link` fails with `NotFound`
    /// exactly as it would after a delete. Concluding "no evidence of
    /// abandonment" and then relocating the file anyway is the right belief
    /// paired with the wrong act, and under [`EffectStore::open_with`] it broke
    /// an independently-owned in-flight attempt with no evidence at all.
    ///
    /// So the action is now governed by [`UndatedPolicy`], fixed at open time:
    ///
    /// - [`UndatedPolicy::Report`] — the default for concurrent use. The entry
    ///   is left exactly where it is and counted. Nothing is claimed about it.
    /// - [`UndatedPolicy::Quarantine`] — reachable only through
    ///   [`EffectStore::open_exclusive`], where the caller has asserted that no
    ///   other attempt is in flight. That assertion, not the entry's age, is
    ///   what licenses moving it.
    ///
    /// Only entries whose age is known **and** at least `stale_after` are ever
    /// removed, under either policy. The returned [`ScavengeReport`] keeps the
    /// three outcomes apart rather than summing them.
    fn scavenge(&self, stale_after: Duration) -> Result<ScavengeReport, AcceptError> {
        let mut report = ScavengeReport::default();
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
            match age {
                // Known age, old enough: this is the only case age actually
                // licenses removing.
                Some(age) if age >= stale_after => {
                    // A reap that fails is reported, not silently miscounted.
                    // This used to be `if remove_file(..).is_ok()`, which turned
                    // a permission or I/O failure into "there was nothing to
                    // remove" and left debris nothing had reported.
                    Self::remove_name(&entry.path())?;
                    report.removed += 1;
                }
                // Known age, too young: leave it, it may be a live attempt.
                Some(_) => {}
                // Age unknown or in the future: no evidence either way, so what
                // happens next is decided by exclusivity, not by the entry.
                None => match self.undated {
                    UndatedPolicy::Report => report.undated_left_in_place += 1,
                    UndatedPolicy::Quarantine => {
                        self.quarantine(&entry.path())?;
                        report.quarantined += 1;
                    }
                },
            }
        }
        Ok(report)
    }

    /// Moves an entry out of `tmp/` without ever overwriting one already there.
    ///
    /// # No-clobber is the point of this directory, not a nicety
    ///
    /// The whole purpose of `quarantine/` is to be the thing that survives.
    /// A publication that can replace an existing entry is not a storage bug in
    /// this directory — it is the feature negating its own reason to exist.
    ///
    /// The previous implementation chose a name with `while target.exists()` and
    /// then took it with `fs::rename`. Both halves are wrong for that purpose:
    ///
    /// - **Check-then-act.** Everything learned by `exists()` is stale by the
    ///   time `rename` runs. Two passes can both observe the same name free.
    /// - **The tiebreak was process-local.** Collisions were resolved with a
    ///   counter held in this process, so two processes resolving the same
    ///   collision resolve it to the *same* name. Restarts make that reachable
    ///   rather than theoretical: staging names are process id and counter, and
    ///   both reset when a process restarts.
    /// - **`rename` replaces silently.** On unix it unlinks whatever is at the
    ///   destination. There is no flag on `std::fs::rename` to refuse.
    ///
    /// Publication is now `fs::hard_link`, which fails with `AlreadyExists`
    /// instead of replacing. A free name is therefore *claimed* rather than
    /// observed to be free, and the claim and the check are one operation, so
    /// there is no window between them. Losing the race is not an error: the
    /// next candidate name is tried.
    ///
    /// `renameat2(RENAME_NOREPLACE)` would be the single-syscall form, but it is
    /// Linux-specific and `std` does not expose it. Link-then-unlink is the
    /// portable POSIX idiom for the same guarantee.
    ///
    /// The unlink of the original name is *not* part of the atomic step, and
    /// does not need to be. If it fails, the entry exists in both places and the
    /// error is reported; a later pass files the surviving `tmp/` entry beside
    /// the first under a fresh name. That duplicates bytes, which is the
    /// direction this directory is allowed to fail in.
    ///
    /// Returns the name the entry was published under.
    fn quarantine(&self, staging: &Path) -> Result<PathBuf, AcceptError> {
        let name = staging
            .file_name()
            .expect("a staging entry always has a file name");
        let dir = self.quarantine_dir();
        for attempt in 0..QUARANTINE_NAME_ATTEMPTS {
            let mut candidate = name.to_os_string();
            if attempt > 0 {
                candidate.push(format!(".{attempt}"));
            }
            let target = dir.join(candidate);
            match fs::hard_link(staging, &target) {
                Ok(()) => {
                    Self::remove_name(staging)?;
                    return Ok(target);
                }
                // Somebody else holds this name. Nothing was touched; try the
                // next one. This is the branch that used to be a silent replace.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(AcceptError::Io(error)),
            }
        }
        Err(AcceptError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "no free quarantine name for {} within {QUARANTINE_NAME_ATTEMPTS} attempts",
                Path::new(name).display()
            ),
        )))
    }

    /// Records the effect for `event` exactly once, keyed by [`EventId`].
    ///
    /// # Redelivery does not write anything
    ///
    /// At-least-once delivery means the common case for an already-accepted
    /// event is a *retry*, and this used to run the full commit for one:
    /// create a staging file, write the record, `fsync` it, attempt the link,
    /// get `AlreadyExists`, then unlink the staging file — a write and a
    /// durability barrier per redelivery, to learn something already on disk.
    ///
    /// The existence check now comes first. That is purely an optimization and
    /// it weakens nothing: `hard_link` is still the only thing that decides who
    /// records, so two attempts that both observe absence still race into the
    /// link and exactly one wins. A hit on the fast path takes the identical
    /// durability repair and full-record validation as the `AlreadyExists` path,
    /// which remains in place for the racing case.
    fn accept(&self, event: &Event) -> Result<AcceptanceOutcome, AcceptError> {
        let target = self.record_path(&event.id)?;
        let receipt = acceptance_receipt(event);
        if target.exists() {
            return self.validate_existing(&target, &receipt);
        }
        match self.commit(&target, &receipt)? {
            Commit::Linked => Ok(AcceptanceOutcome::Recorded(receipt)),
            Commit::AlreadyExists => self.validate_existing(&target, &receipt),
        }
    }

    /// Repairs durability, then decides whether an existing entry is acceptance.
    ///
    /// Shared by the redelivery fast path and the racing `AlreadyExists` path so
    /// the two cannot drift: any weakening here would have to be made twice.
    fn validate_existing(
        &self,
        target: &Path,
        receipt: &AcceptanceReceipt,
    ) -> Result<AcceptanceOutcome, AcceptError> {
        let parent = target.parent().expect("record path has a parent directory");
        // Durability repair, before anything is called acceptance. A previous
        // attempt can have linked the entry and then failed its directory
        // fsync: it returned an error, but left the entry visible. Without
        // re-syncing here, this path would report a prior acceptance for an
        // entry that was never made crash-durable, and the courier would ACK
        // work that can still vanish.
        self.sync_dir(parent)?;

        // The filename alone is not a durable receipt: a path can exist with a
        // truncated, empty, corrupt, or substituted body. Read it and require it
        // to equal the receipt this event deterministically produces — the whole
        // record, not just its key. Matching only the event id accepted any
        // record filed under the right name, including one carrying somebody
        // else's receipt.
        let existing = self.read_record(target)?;
        if &existing != receipt {
            return Err(AcceptError::unusable(
                target,
                format!("record {existing:?} is not the expected receipt {receipt:?}"),
                None,
            ));
        }
        Ok(AcceptanceOutcome::Existing(existing))
    }

    /// Negative control A: the identical commit protocol with the [`EventId`]
    /// keying removed, so every attempt lands under a fresh name.
    ///
    /// This isolates the **keying**, and nothing else. It is sequential by
    /// construction and is *not* a race control — see
    /// [`EffectStore::naive_observe`] for that.
    fn accept_without_dedup(&self, event: &Event) -> Result<AcceptanceReceipt, AcceptError> {
        let receipt = acceptance_receipt(event);
        let target = self
            .duplicates_dir()
            .join(format!("d{}", STAGING_ID.fetch_add(1, Ordering::Relaxed)));
        match self.commit(&target, &receipt)? {
            Commit::Linked => Ok(receipt),
            Commit::AlreadyExists => Err(AcceptError::unusable(
                &target,
                "unkeyed duplicate name was reused".into(),
                None,
            )),
        }
    }

    /// Negative control B, half one: observe whether the effect is already
    /// recorded in a shared append log.
    ///
    /// This is the shape the real protocol exists to avoid — the dedup decision
    /// and the effect record are two operations, so an interleaving exists in
    /// which both couriers observe absence. Exposing the halves separately lets
    /// a test *force* that interleaving deterministically instead of sampling
    /// for it under a barrier and hoping it shows up.
    fn naive_observe(&self, event: &Event) -> Result<bool, AcceptError> {
        let log = self.naive_log();
        if !log.exists() {
            return Ok(false);
        }
        for line in io::BufReader::new(File::open(&log)?).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: AcceptanceReceipt = serde_json::from_str(&line).map_err(|error| {
                AcceptError::unusable(
                    &log,
                    format!("bad log line: {error}"),
                    Some(Box::new(error)),
                )
            })?;
            if record.event_id == event.id.0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Negative control B, half two: append the effect record to the shared log.
    fn naive_record(&self, event: &Event) -> Result<AcceptanceReceipt, AcceptError> {
        let receipt = acceptance_receipt(event);
        let mut bytes = serde_json::to_vec(&receipt).map_err(io::Error::other)?;
        bytes.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.naive_log())?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(receipt)
    }

    fn naive_log_records(&self) -> Result<Vec<AcceptanceReceipt>, AcceptError> {
        let log = self.naive_log();
        if !log.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for line in io::BufReader::new(File::open(&log)?).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(serde_json::from_str(&line).map_err(|error| {
                AcceptError::unusable(
                    &log,
                    format!("bad log line: {error}"),
                    Some(Box::new(error)),
                )
            })?);
        }
        Ok(records)
    }

    /// Steps 1 to 3: a unique, fully written, fsynced staging file.
    ///
    /// Returned as a path rather than consumed immediately so tests can hold a
    /// *live* stage across another operation.
    fn stage(&self, receipt: &AcceptanceReceipt) -> Result<PathBuf, AcceptError> {
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
            // The cleanup runs through the same postcondition as every other
            // unlink here, and its outcome is *subordinated*, not discarded.
            // `link_staged` already establishes the rule: a primary failure
            // wins, because it is the cause the caller needs. What differs from
            // the old `let _ = fs::remove_file(..)` is that the subordination is
            // now a stated policy at one place rather than an anonymous
            // discard at each. Residual debris in `tmp/` is what scavenging is
            // for, and this attempt has already reported an error.
            let _cleanup_is_subordinate_to_the_write_error = Self::remove_name(&staging);
            return Err(AcceptError::Io(error));
        }
        Ok(staging)
    }

    /// Steps 4 and 5: publish the staged record, then make the entry durable.
    ///
    /// Removes the staging file on every outcome, so crashed attempts are the
    /// only thing scavenging ever has to handle — and **reports** it when that
    /// removal fails rather than discarding the error, since a silent failure
    /// leaves debris indistinguishable from a crash.
    ///
    /// A cleanup failure never masks a primary failure: if the link or the
    /// directory sync already failed, that error is the one returned, because it
    /// is the cause. A cleanup failure over an otherwise successful commit *is*
    /// surfaced, which conservatively turns a durable commit into a reported
    /// error. That is safe here — the record is keyed by `EventId`, so the
    /// courier's retry observes it via the redelivery path and accepts once —
    /// and it is preferable to returning success while the stated invariant is
    /// quietly broken.
    fn link_staged(&self, staging: &Path, target: &Path) -> Result<Commit, AcceptError> {
        // 4. atomically link into place; fails if the target already exists
        let linked = fs::hard_link(staging, target);
        let parent = target.parent().expect("record path has a parent directory");
        let result = match linked {
            // 5. sync the DIRECTORY so the new entry survives a crash, before
            //    the staging copy that could have replaced it goes away.
            //
            //    If this fails the entry is already visible. `accept` repairs
            //    that on the next attempt's `AlreadyExists` path; the error is
            //    still propagated here, so this attempt never reports success.
            Ok(()) => self
                .sync_dir(parent)
                .map(|()| Commit::Linked)
                .map_err(Into::into),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(Commit::AlreadyExists),
            Err(error) => Err(AcceptError::Io(error)),
        };
        let cleaned = self.remove_staging(staging);
        // The primary outcome wins: a cleanup failure must not hide its cause.
        match (result, cleaned) {
            (Ok(commit), Ok(())) => Ok(commit),
            (Ok(_), Err(error)) => Err(AcceptError::Io(error)),
            (Err(primary), _) => Err(primary),
        }
    }

    /// The five-step commit. `target`'s parent directory must already exist.
    fn commit(&self, target: &Path, receipt: &AcceptanceReceipt) -> Result<Commit, AcceptError> {
        let staging = self.stage(receipt)?;
        self.link_staged(&staging, target)
    }

    fn read_record(&self, path: &Path) -> Result<AcceptanceReceipt, AcceptError> {
        let bytes = fs::read(path).map_err(|error| {
            AcceptError::unusable(path, format!("unreadable: {error}"), Some(Box::new(error)))
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            AcceptError::unusable(
                path,
                format!("not a complete record: {error}"),
                Some(Box::new(error)),
            )
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

    fn quarantine_entries(&self) -> Result<usize, AcceptError> {
        Ok(fs::read_dir(self.quarantine_dir())?.count())
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
    assert_eq!(*first_lease.event(), appended);

    // The loser is told who holds the message and when that hold lapses — enough
    // to schedule a retry instead of spinning blindly. It is *not* told the
    // holder's fence, which was the last field needed to forge the hold.
    let denied: Vec<_> = outcomes
        .iter()
        .filter(|outcome| !matches!(outcome, ClaimOutcome::Granted(_)))
        .collect();
    assert_eq!(denied.len(), 1);
    assert_eq!(
        denied[0],
        &ClaimOutcome::Contended {
            event_id: appended.id.clone(),
            holder: first_lease.owner().clone(),
            expires_at: first_lease.expires_at(),
        }
    );

    let scratch = ScratchRoot::create()?;
    let first_recipient = EffectStore::open(scratch.path())?;
    let first_receipt = match first_recipient.accept(first_lease.event())? {
        AcceptanceOutcome::Recorded(receipt) => receipt,
        AcceptanceOutcome::Existing(_) => return Err("first acceptance was not new".into()),
    };
    assert_eq!(first_recipient.receipts()?, vec![first_receipt.clone()]);
    assert_eq!(first_recipient.staging_entries()?, 0);

    // The courier loses the queue ACK after the recipient has durably accepted
    // the message. Expiry therefore causes an intentional at-least-once retry.
    let second_lease = claim_after_expiry(&store, &clock, first_lease.owner());
    assert_eq!(*second_lease.event(), appended);
    assert!(second_lease.fence() > first_lease.fence());
    assert_eq!(store.ack_work(&first_lease), Err(Error::StaleFence));

    // A fresh recipient process, reading the same root.
    let redelivery_recipient = EffectStore::open(scratch.path())?;
    assert_eq!(
        redelivery_recipient.accept(second_lease.event())?,
        AcceptanceOutcome::Existing(first_receipt.clone())
    );
    assert_eq!(redelivery_recipient.receipts()?, vec![first_receipt]);
    assert_eq!(redelivery_recipient.staging_entries()?, 0);

    let queue_ack = store.ack_work(&second_lease)?;
    assert_eq!(queue_ack.event_id, appended.id);
    assert_eq!(queue_ack.fence, second_lease.fence());

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
/// because one barrier release only samples one interleaving.
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
        let fresh_lease = claim_after_expiry(&store, &clock, stale_lease.owner());
        assert!(fresh_lease.fence() > stale_lease.fence());
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
                    recipient.accept(lease.event())
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
    // Zero-age reaping is only available through the exclusive entry point,
    // where the caller asserts no other attempt is in flight — which is exactly
    // the situation being modelled here.
    let reopened = EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?;
    assert!(!torn.exists(), "stale staging file should be scavenged");
    assert!(!empty.exists(), "stale staging file should be scavenged");
    assert_eq!(reopened.staging_entries()?, 0);

    let receipt = match reopened.accept(lease.event())? {
        AcceptanceOutcome::Recorded(receipt) => receipt,
        AcceptanceOutcome::Existing(_) => {
            return Err("torn debris was misread as a prior acceptance".into());
        }
    };
    assert_eq!(receipt, acceptance_receipt(&appended));

    // Redelivery after the interruption still yields the same receipt.
    let second_lease = claim_after_expiry(&store, &clock, lease.owner());
    let redelivery = EffectStore::open(scratch.path())?;
    assert_eq!(
        redelivery.accept(second_lease.event())?,
        AcceptanceOutcome::Existing(receipt.clone())
    );
    assert_eq!(redelivery.receipts()?, vec![receipt]);
    store.ack_work(&second_lease)?;
    Ok(())
}

/// A concurrently-usable store refuses a reap window that could delete live work.
#[test]
fn short_reap_windows_are_refused_outside_exclusive_recovery() -> Result<(), Box<dyn StdError>> {
    let scratch = ScratchRoot::create()?;
    let error = EffectStore::open_with(scratch.path(), Duration::ZERO)
        .err()
        .ok_or("a zero reap window must be refused for concurrent use")?;
    assert!(
        matches!(error, AcceptError::ScavengeWindowTooShort { .. }),
        "unexpected error {error:?}"
    );
    // The same window is available, named as such, for exclusive recovery.
    EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?;
    Ok(())
}

/// A live stage really can be reaped, and the attempt fails rather than lying.
///
/// This is the test the old "a concurrent courier's staging file is never
/// deleted" claim did not have: the previous coverage only reaped simulated
/// debris. Both directions are asserted — the reap happens and the attempt
/// fails loudly under a zero window, and a live stage survives a window above
/// the enforced floor.
#[cfg(unix)]
#[test]
fn live_staging_file_may_be_reaped_and_the_attempt_fails_loudly() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let receipt = acceptance_receipt(&appended);

    // Deterministic direction: a live stage, then a zero-window reap, then link.
    {
        let scratch = ScratchRoot::create()?;
        let recipient = EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?;
        let staging = recipient.stage(&receipt)?;
        assert!(staging.exists(), "the stage is live and unlinked");

        assert_eq!(
            recipient.scavenge(Duration::ZERO)?,
            ScavengeReport {
                removed: 1,
                ..ScavengeReport::default()
            },
            "age alone cannot tell a live stage from debris"
        );
        assert!(!staging.exists(), "the live stage was reaped");

        let target = recipient.record_path(&appended.id)?;
        let error = recipient
            .link_staged(&staging, &target)
            .err()
            .ok_or("linking a reaped stage must fail")?;
        assert!(matches!(error, AcceptError::Io(_)), "unexpected {error:?}");
        assert!(
            StdError::source(&error).is_some(),
            "the underlying I/O error must stay attached"
        );
        // Conservative, not corrupting: nothing was recorded, nothing was lost.
        assert_eq!(recipient.receipts()?, vec![]);
        assert!(!target.exists());
    }

    // Concurrent direction: a scavenger races a live stage. Whatever the
    // interleaving, effects/ ends up holding either nothing or exactly the
    // right record — never a wrong one and never two.
    {
        let scratch = ScratchRoot::create()?;
        let recipient = Arc::new(EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?);
        let staging = recipient.stage(&receipt)?;
        let target = recipient.record_path(&appended.id)?;
        let barrier = Arc::new(Barrier::new(2));

        let reaper = {
            let recipient = recipient.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                recipient.scavenge(Duration::ZERO)
            })
        };
        barrier.wait();
        let linked = recipient.link_staged(&staging, &target);
        reaper.join().expect("reaper thread should not panic")?;

        match linked {
            Ok(Commit::Linked) => assert_eq!(recipient.receipts()?, vec![receipt.clone()]),
            Ok(Commit::AlreadyExists) => return Err("nothing else could have linked".into()),
            Err(AcceptError::Io(_)) => assert_eq!(recipient.receipts()?, vec![]),
            Err(other) => return Err(format!("unexpected failure {other:?}").into()),
        }
        assert!(recipient.receipts()?.len() <= 1);
    }

    // A window above the floor leaves a fresh live stage alone.
    {
        let scratch = ScratchRoot::create()?;
        let recipient = EffectStore::open_with(scratch.path(), MIN_CONCURRENT_STALE_AFTER)?;
        let staging = recipient.stage(&receipt)?;
        assert_eq!(
            recipient.scavenge(MIN_CONCURRENT_STALE_AFTER)?,
            ScavengeReport::default()
        );
        assert!(staging.exists(), "a fresh stage is not stale");
        let target = recipient.record_path(&appended.id)?;
        assert!(matches!(
            recipient.link_staged(&staging, &target)?,
            Commit::Linked
        ));
        assert_eq!(recipient.receipts()?, vec![receipt]);
    }
    Ok(())
}

/// A link that outlived its directory fsync is repaired before it counts.
///
/// The failure being injected is step 5: `hard_link` succeeds, the `effects/`
/// fsync fails. The entry is visible but not crash-durable, and `commit`
/// correctly reports an error. The defect was that the *retry* then read the
/// visible entry, answered `Existing`, and never re-synced — so a courier could
/// acknowledge the queue item over an entry that could still vanish on power
/// loss. The retry now fsyncs before it validates anything.
#[cfg(unix)]
#[test]
fn post_link_directory_sync_failure_is_repaired_before_existing_is_returned()
-> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let scratch = ScratchRoot::create()?;

    // Attempt one: the link lands, the directory sync fails, and the attempt
    // reports failure. The courier must not acknowledge on this path.
    let first = EffectStore::open(scratch.path())?;
    first.fail_next_dir_syncs(1);
    let error = first
        .accept(&appended)
        .err()
        .ok_or("a failed directory sync must not report acceptance")?;
    assert!(matches!(error, AcceptError::Io(_)), "unexpected {error:?}");
    assert!(
        StdError::source(&error).is_some(),
        "the injected I/O error must stay attached as a source"
    );
    // The entry is visible even though the attempt failed. That is the gap.
    assert!(first.record_path(&appended.id)?.exists());
    assert_eq!(first.staging_entries()?, 0);

    // Attempt two, from a fresh recipient process: it takes the AlreadyExists
    // path. If its durability repair also fails, it must propagate, not answer
    // Existing over a possibly non-durable entry.
    let retry = EffectStore::open(scratch.path())?;
    retry.fail_next_dir_syncs(1);
    let error = retry
        .accept(&appended)
        .err()
        .ok_or("a failed durability repair must not report Existing")?;
    assert!(matches!(error, AcceptError::Io(_)), "unexpected {error:?}");

    // Attempt three: the repair succeeds, so the entry is durable and only then
    // is it reported as a prior acceptance.
    let repaired = EffectStore::open(scratch.path())?;
    assert_eq!(
        repaired.accept(&appended)?,
        AcceptanceOutcome::Existing(acceptance_receipt(&appended))
    );
    assert_eq!(repaired.receipts()?, vec![acceptance_receipt(&appended)]);
    assert_eq!(repaired.staging_entries()?, 0);
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
///
/// The **wrong receipt** case is the one that matters most: it carries the right
/// event id, so a check that only compared the embedded key accepted it and
/// returned that record's arbitrary `receipt_id` as a successful prior
/// acceptance. The check is now full-record equality against the receipt this
/// event deterministically produces.
#[test]
fn corrupt_existing_record_is_not_reported_as_prior_acceptance() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("first append unexpectedly found an event".into()),
    };

    for (label, damage, expect_source) in [
        ("truncated", &br#"{"event_id":"urgent-mes"#[..], true),
        ("empty", &b""[..], true),
        (
            "wrong event",
            &br#"{"event_id":"someone-else","receipt_id":"x"}"#[..],
            false,
        ),
        (
            "right event, substituted receipt",
            &br#"{"event_id":"urgent-message-2026-08-03-001","receipt_id":"attacker-supplied"}"#[..],
            false,
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
        // Parse and I/O failures keep their cause; a semantic mismatch has none.
        assert_eq!(
            StdError::source(&error).is_some(),
            expect_source,
            "{label} record source chain"
        );
        assert_eq!(recipient.staging_entries()?, 0, "{label} leaked staging");
    }
    Ok(())
}

/// Negative control A: the same commit protocol, minus [`EventId`] keying.
///
/// This isolates the keying and nothing else, on a **sequential** redelivery.
/// It shows that dedup by `EventId` is what collapses a redelivery into one
/// effect; it does *not* demonstrate anything about racing. The racy shape has
/// its own control below, and the two claims must not be conflated.
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
    recipient.accept_without_dedup(first_lease.event())?;

    let second_lease = claim_after_expiry(&store, &clock, first_lease.owner());
    recipient.accept_without_dedup(second_lease.event())?;
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

/// Negative control B: a check-then-append recipient duplicates the effect.
///
/// This is the control that discriminates the *atomicity* of the real protocol,
/// which control A does not. The naive recipient splits the dedup decision from
/// the effect record, and the interleaving is **forced** rather than sampled:
/// both couriers observe absence, and only then does either record. A barrier
/// might or might not produce that ordering on a given run; forcing it means the
/// control demonstrates the defect every time, on every platform.
///
/// The real protocol has no such interleaving to force. `hard_link` *is* the
/// decision and the record, so the same two attempts are run through `accept`
/// back to back and exactly one records.
#[test]
fn check_then_append_recipient_duplicates_where_the_atomic_link_does_not()
-> Result<(), Box<dyn StdError>> {
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
    let fresh_lease = claim_after_expiry(&store, &clock, stale_lease.owner());

    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;

    // Forced losing interleaving: observe, observe, record, record.
    assert!(!recipient.naive_observe(stale_lease.event())?);
    assert!(!recipient.naive_observe(fresh_lease.event())?);
    recipient.naive_record(stale_lease.event())?;
    recipient.naive_record(fresh_lease.event())?;

    let log = recipient.naive_log_records()?;
    assert_eq!(
        log.len(),
        2,
        "check-then-append records the effect twice under the forced interleaving"
    );
    assert_eq!(log[0], log[1]);
    assert_eq!(log[0], acceptance_receipt(&appended));

    // The same two attempts through the atomic protocol: one record, one
    // observation of it, one file on disk.
    let outcomes = [
        recipient.accept(stale_lease.event())?,
        recipient.accept(fresh_lease.event())?,
    ];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptanceOutcome::Recorded(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AcceptanceOutcome::Existing(_)))
            .count(),
        1
    );
    assert_eq!(recipient.receipts()?, vec![acceptance_receipt(&appended)]);
    store.ack_work(&fresh_lease)?;
    Ok(())
}

/// A staging file whose age cannot be established is quarantined, not deleted.
///
/// Age is the scavenger's only evidence, so an entry that carries none must not
/// be treated as the *most* stale thing in the directory. It was: an unreadable
/// or future-dated mtime reaped immediately, at any window, which bypassed the
/// `MIN_CONCURRENT_STALE_AFTER` floor that `open_with` exists to enforce — and a
/// future-dated mtime is a clock step or a nonmonotonic filesystem, not evidence
/// of abandonment.
///
/// A future-dated file is used because it is the reachable half of the same
/// branch: `duration_since` fails, `age` is `None`, and the unreadable-mtime
/// case joins it there.
#[cfg(unix)]
#[test]
fn future_dated_staging_files_are_quarantined_rather_than_reaped() -> Result<(), Box<dyn StdError>>
{
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let receipt = acceptance_receipt(&appended);

    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?;
    let staging = recipient.stage(&receipt)?;

    // Date it an hour into the future: `SystemTime::now().duration_since` fails,
    // so the age is unknown.
    let future = SystemTime::now() + Duration::from_secs(3600);
    File::options()
        .write(true)
        .open(&staging)?
        .set_times(fs::FileTimes::new().set_modified(future))?;

    // Even a zero window — the most aggressive setting there is — must not
    // delete it.
    assert_eq!(
        recipient.scavenge(Duration::ZERO)?,
        ScavengeReport {
            quarantined: 1,
            ..ScavengeReport::default()
        },
        "an unknown age is not evidence of staleness"
    );
    assert!(!staging.exists(), "the entry left the staging namespace");
    assert_eq!(recipient.staging_entries()?, 0);
    assert_eq!(
        recipient.quarantine_entries()?,
        1,
        "the entry is preserved for exclusive recovery"
    );

    // The bytes survived: quarantine preserves evidence, it does not destroy it.
    let preserved = fs::read_dir(recipient.quarantine_dir())?
        .next()
        .ok_or("quarantine should hold the entry")??;
    assert_eq!(recipient.read_record(&preserved.path())?, receipt);

    // Repeated passes do not re-quarantine or lose anything.
    assert_eq!(
        recipient.scavenge(Duration::ZERO)?,
        ScavengeReport::default()
    );
    assert_eq!(recipient.quarantine_entries()?, 1);

    // A known-age entry alongside it is still reaped normally, so quarantining
    // did not disable the scavenger.
    let ordinary = recipient.stage(&receipt)?;
    assert_eq!(
        recipient.scavenge(Duration::ZERO)?,
        ScavengeReport {
            removed: 1,
            ..ScavengeReport::default()
        }
    );
    assert!(!ordinary.exists());
    assert_eq!(recipient.quarantine_entries()?, 1);
    Ok(())
}

/// Creates `count` same-named entries with distinct bodies, in sibling dirs.
///
/// Same basename is what forces the collision, and a directory each is the only
/// way to have several at once — which is also how the collision arises in
/// practice, since a restarted process reuses staging names.
fn colliding_sources(root: &Path, count: usize) -> io::Result<Vec<(PathBuf, Vec<u8>)>> {
    (0..count)
        .map(|index| {
            let dir = root.join(format!("attempt-{index}"));
            fs::create_dir_all(&dir)?;
            let path = dir.join("s1234-0");
            let body = format!("evidence-{index}").into_bytes();
            fs::write(&path, &body)?;
            Ok((path, body))
        })
        .collect()
}

/// Publishing into `quarantine/` must never overwrite what is already there.
///
/// This directory exists to be the thing that survives, so a publication that
/// can replace an entry is not a storage bug — it is the feature negating its
/// own purpose. `while target.exists()` followed by `fs::rename` could do
/// exactly that: the check is stale before the act, the collision tiebreak was a
/// process-local counter so two processes pick the same "fresh" name, and unix
/// `rename` replaces the destination silently.
///
/// The collision is *forced* rather than raced for: every entry shares one file
/// name, so every publication after the first collides. What is asserted is not
/// that the names differ but that every distinct byte string is still readable.
#[cfg(unix)]
#[test]
fn quarantine_publication_never_overwrites_existing_evidence() -> Result<(), Box<dyn StdError>> {
    const ENTRIES: usize = 4;

    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?;
    let sources = colliding_sources(scratch.path(), ENTRIES)?;

    let mut published = Vec::new();
    for (path, _) in &sources {
        published.push(recipient.quarantine(path)?);
        assert!(!path.exists(), "the entry left its original name");
    }

    let mut names = published.clone();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), ENTRIES, "each collision took a distinct name");
    assert_eq!(recipient.quarantine_entries()?, ENTRIES);

    // The promise is the bytes, not the names.
    let mut survived = published
        .iter()
        .map(fs::read)
        .collect::<io::Result<Vec<_>>>()?;
    let mut expected: Vec<_> = sources.into_iter().map(|(_, body)| body).collect();
    survived.sort();
    expected.sort();
    assert_eq!(survived, expected, "every distinct body survived");
    Ok(())
}

/// The same collision, resolved concurrently, still loses nothing.
///
/// The forced-collision case above is deterministic but sequential, so it cannot
/// exercise the window the old shape actually opened: two publications both
/// observing a name free and both taking it. Here every thread races for the
/// same first name. `hard_link` makes losing the race a refusal rather than a
/// replacement, so the loser retries and every body survives on every
/// interleaving — which is why this is asserted rather than sampled.
#[cfg(unix)]
#[test]
fn concurrent_quarantine_publication_loses_nothing() -> Result<(), Box<dyn StdError>> {
    const THREADS: usize = 8;
    const ROUNDS: usize = 16;

    for round in 0..ROUNDS {
        let scratch = ScratchRoot::create()?;
        let recipient = Arc::new(EffectStore::open_exclusive(scratch.path(), Duration::ZERO)?);
        let sources = colliding_sources(scratch.path(), THREADS)?;
        let barrier = Arc::new(Barrier::new(THREADS));

        let handles: Vec<_> = sources
            .iter()
            .map(|(path, _)| {
                let recipient = recipient.clone();
                let barrier = barrier.clone();
                let path = path.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    recipient.quarantine(&path)
                })
            })
            .collect();
        let published = handles
            .into_iter()
            .map(|handle| handle.join().expect("quarantine thread should not panic"))
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            recipient.quarantine_entries()?,
            THREADS,
            "round {round}: one entry per publication"
        );
        let mut survived = published
            .iter()
            .map(fs::read)
            .collect::<io::Result<Vec<_>>>()?;
        let mut expected: Vec<_> = sources.into_iter().map(|(_, body)| body).collect();
        survived.sort();
        expected.sort();
        assert_eq!(survived, expected, "round {round}: no body was replaced");
    }
    Ok(())
}

/// A concurrent store leaves an entry it cannot date for whoever owns it.
///
/// Unknown age is no evidence of abandonment. The scavenger already classified
/// it that way and then moved the entry anyway — a rename out of `tmp/` takes
/// the staging name from its owner exactly as a delete does, and the owner's
/// `hard_link` failed with `NotFound`. The belief was right and the act was
/// wrong, and it cost an independently-owned commit.
///
/// The claim is checked by the owner *finishing*, not by inspecting directories.
/// A live stage is future-dated so its age becomes unestablishable, a second
/// store opened against the same root scavenges around it, and the original
/// owner then links successfully. Without the fix the link fails, which is the
/// exact failure the old code inflicted on a live courier.
#[cfg(unix)]
#[test]
fn concurrent_scavenging_leaves_an_undated_stage_for_its_owner() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let receipt = acceptance_receipt(&appended);

    let scratch = ScratchRoot::create()?;
    let owner = EffectStore::open_with(scratch.path(), MIN_CONCURRENT_STALE_AFTER)?;
    let staging = owner.stage(&receipt)?;

    // A clock step or a nonmonotonic filesystem: `duration_since` fails, so the
    // age is unknown. The file is nonetheless live work with an owner mid-commit.
    let future = SystemTime::now() + Duration::from_secs(3600);
    File::options()
        .write(true)
        .open(&staging)?
        .set_times(fs::FileTimes::new().set_modified(future))?;

    // A second courier process opens the same root and scavenges around it. Its
    // open runs a pass of its own, so this covers both entry points.
    let other = EffectStore::open_with(scratch.path(), MIN_CONCURRENT_STALE_AFTER)?;
    assert_eq!(
        other.scavenge(MIN_CONCURRENT_STALE_AFTER)?,
        ScavengeReport {
            undated_left_in_place: 1,
            ..ScavengeReport::default()
        },
        "an entry with no establishable age is reported, never moved"
    );
    assert!(
        staging.exists(),
        "the owner's staging name is still its own"
    );
    assert_eq!(other.quarantine_entries()?, 0, "nothing was relocated");
    assert_eq!(other.staging_entries()?, 1);

    // The owner completes its commit. This is what the rename used to break.
    let target = owner.record_path(&appended.id)?;
    assert!(matches!(
        owner.link_staged(&staging, &target)?,
        Commit::Linked
    ));
    assert_eq!(owner.receipts()?, vec![receipt]);
    assert_eq!(owner.staging_entries()?, 0);
    Ok(())
}

/// A young staging file is left alone even when its age is known.
///
/// The complement of the quarantine case: the floor is only meaningful if a
/// known age *below* it is also refused.
#[cfg(unix)]
#[test]
fn known_young_staging_files_are_neither_reaped_nor_quarantined() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open_with(scratch.path(), MIN_CONCURRENT_STALE_AFTER)?;
    let staging = recipient.stage(&acceptance_receipt(&appended))?;

    assert_eq!(
        recipient.scavenge(MIN_CONCURRENT_STALE_AFTER)?,
        ScavengeReport::default()
    );
    assert!(staging.exists(), "a fresh stage is live work");
    assert_eq!(recipient.quarantine_entries()?, 0);
    Ok(())
}

/// A failed staging cleanup is reported, not discarded.
///
/// The module docs promise the staging file is removed on every outcome. The
/// removal was `let _ = fs::remove_file(..)`, so a failure silently falsified
/// that promise and left debris identical in shape to a crashed attempt, with
/// nothing having reported anything.
#[cfg(unix)]
#[test]
fn a_failed_staging_cleanup_is_reported_rather_than_swallowed() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };

    // The link and the directory sync both succeed; only the cleanup fails.
    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;
    recipient.fail_next_staging_removals(1);
    let error = recipient
        .accept(&appended)
        .err()
        .ok_or("a failed staging cleanup must be reported")?;
    assert!(matches!(error, AcceptError::Io(_)), "unexpected {error:?}");
    assert!(
        StdError::source(&error).is_some(),
        "the underlying I/O error must stay attached"
    );

    // Conservative, not corrupting: the effect *is* recorded, and the courier's
    // retry observes it exactly once rather than recording a second time.
    assert_eq!(recipient.receipts()?, vec![acceptance_receipt(&appended)]);
    assert_eq!(
        recipient.accept(&appended)?,
        AcceptanceOutcome::Existing(acceptance_receipt(&appended))
    );
    assert_eq!(recipient.receipts()?, vec![acceptance_receipt(&appended)]);
    Ok(())
}

/// A cleanup failure must not mask the primary failure that caused it.
#[cfg(unix)]
#[test]
fn a_cleanup_failure_never_masks_the_primary_failure() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;

    // Both the post-link directory sync and the cleanup fail. The reported
    // error must be the durability failure, which is the one that matters.
    recipient.fail_next_dir_syncs(1);
    recipient.fail_next_staging_removals(1);
    let error = recipient
        .accept(&appended)
        .err()
        .ok_or("a failed directory sync must not report acceptance")?;
    let rendered = error.to_string();
    assert!(
        rendered.contains("directory sync"),
        "the primary failure must survive: {rendered}"
    );
    Ok(())
}

/// Redelivery of an accepted event writes nothing at all.
///
/// At-least-once delivery makes redelivery the common case, and it used to cost
/// a staging create, a full record write, an `fsync`, a failed link and an
/// unlink — every time — to discover a record that was already on disk. The
/// existence check now comes first.
///
/// Measured by fault injection rather than by timing: staging removals are armed
/// to fail, so *if* the fast path staged anything the attempt would report an
/// error. It returns `Existing`, so it did not stage.
#[cfg(unix)]
#[test]
fn redelivery_of_an_accepted_event_does_not_stage_or_fsync() -> Result<(), Box<dyn StdError>> {
    let store = MemoryStore::with_clock(Arc::new(ManualClock::default()), Arc::new(AllowAll));
    let appended = match store.append("dispatcher", urgent_message())? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let scratch = ScratchRoot::create()?;
    let recipient = EffectStore::open(scratch.path())?;
    assert!(matches!(
        recipient.accept(&appended)?,
        AcceptanceOutcome::Recorded(_)
    ));

    // Armed for a whole sequence of attempts: any staging at all trips it.
    recipient.fail_next_staging_removals(16);
    for _ in 0..4 {
        assert_eq!(
            recipient.accept(&appended)?,
            AcceptanceOutcome::Existing(acceptance_receipt(&appended))
        );
    }
    assert_eq!(recipient.staging_entries()?, 0, "nothing was staged");
    assert_eq!(recipient.receipts()?, vec![acceptance_receipt(&appended)]);

    // The racing path is unchanged: a first-time event still runs the full
    // five-step commit, and would trip the armed cleanup fault if it ran.
    let other = match store.append("dispatcher", event_named("second-message"))? {
        AppendOutcome::Appended(event) => event,
        AppendOutcome::Existing(_) => return Err("append unexpectedly found an event".into()),
    };
    let error = recipient
        .accept(&other)
        .err()
        .ok_or("a first acceptance must still stage, and so must trip the fault")?;
    assert!(matches!(error, AcceptError::Io(_)), "unexpected {error:?}");
    Ok(())
}
