//! Proof-only adapter for Interlockutor's semantic kernel.
//!
//! Kani 0.67 uses rustc 1.93, while the product crate follows the workspace's
//! rustc 1.95 MSRV. This unpublished crate supplies only the two primitive
//! domain types the kernel imports, then compiles the production source file
//! unchanged. There is one transition implementation, not a model that can
//! drift away from it.

#[cfg(kani)]
pub type Timestamp = u64;

#[cfg(kani)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Fence(pub u64);

#[cfg(kani)]
#[path = "../../interlockutor/src/kernel.rs"]
mod kernel;
