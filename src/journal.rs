use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
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

#[derive(Default)]
pub struct Journal {
    states: HashMap<u64, TaskState>,
}

impl Journal {
    pub fn insert_queued(&mut self, id: u64) {
        self.states.insert(id, TaskState::Queued);
    }

    pub fn mark_running(&mut self, id: u64) {
        self.states.insert(id, TaskState::Running);
    }

    pub fn finish(&mut self, id: u64, state: TaskState) {
        self.states.insert(id, state);
    }

    pub fn get(&self, id: u64) -> Option<TaskState> {
        self.states.get(&id).copied()
    }

    pub fn stats(&self) -> Stats {
        let mut stats = Stats::default();
        for state in self.states.values() {
            match state {
                TaskState::Queued => stats.queued += 1,
                TaskState::Running => stats.in_flight += 1,
                TaskState::Succeeded => stats.succeeded += 1,
                TaskState::Failed => stats.failed += 1,
                TaskState::Cancelled => stats.cancelled += 1,
                TaskState::TimedOut => stats.timed_out += 1,
            }
        }
        stats
    }
}
