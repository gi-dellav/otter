//! otter: a BEAM-like JavaScript runtime built on QuickJS.
//!
//! Each process owns an isolated QuickJS runtime (its own heap) and a mailbox.
//! A fixed pool of worker threads multiplexes all processes: a process runs a
//! single job per scheduling slice, and suspends itself by awaiting `recv()`
//! when its mailbox is empty.

pub mod process;
pub mod rpc;
pub mod scheduler;
