use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Unknown = 0,
    Queued = 1,
    Running = 2,
    Succeeded = 3,
    Failed = 4,
    Cancelled = 5,
    TimedOut = 6,
}

impl TaskState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Queued,
            2 => Self::Running,
            3 => Self::Succeeded,
            4 => Self::Failed,
            5 => Self::Cancelled,
            6 => Self::TimedOut,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

/// Per-task status shared with `Handle`. Avoids locking a global map on every poll.
pub struct TaskSlot {
    state: AtomicU8,
}

impl TaskSlot {
    pub fn queued() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(TaskState::Queued as u8),
        })
    }

    pub fn get(&self) -> TaskState {
        TaskState::from_u8(self.state.load(Ordering::Acquire))
    }

    fn transition(&self, from: TaskState, to: TaskState) -> bool {
        self.state
            .compare_exchange(
                from as u8,
                to as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn swap(&self, next: TaskState) -> TaskState {
        TaskState::from_u8(self.state.swap(next as u8, Ordering::AcqRel))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Stats {
    pub queued: u64,
    pub in_flight: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub timed_out: u64,
}

impl Stats {
    pub fn completed(&self) -> u64 {
        self.succeeded + self.failed + self.cancelled + self.timed_out
    }
}

/// Aggregate counters only. Completed tasks are not retained.
#[derive(Default)]
pub struct Journal {
    queued: AtomicU64,
    in_flight: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    timed_out: AtomicU64,
}

impl Journal {
    pub fn insert_queued(&self) {
        self.queued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn mark_running(&self, slot: &TaskSlot) {
        if slot.transition(TaskState::Queued, TaskState::Running) {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            self.in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn finish(&self, slot: &TaskSlot, next: TaskState) {
        let prev = slot.swap(next);
        match prev {
            TaskState::Queued => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
            }
            TaskState::Running => {
                self.in_flight.fetch_sub(1, Ordering::Relaxed);
            }
            _ => return,
        }
        match next {
            TaskState::Succeeded => {
                self.succeeded.fetch_add(1, Ordering::Relaxed);
            }
            TaskState::Failed => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            TaskState::Cancelled => {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            TaskState::TimedOut => {
                self.timed_out.fetch_add(1, Ordering::Relaxed);
            }
            TaskState::Unknown | TaskState::Queued | TaskState::Running => {}
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            queued: self.queued.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            succeeded: self.succeeded.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
        }
    }
}
