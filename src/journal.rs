use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }
}

#[derive(Default)]
pub struct Journal {
    states: HashMap<u64, TaskState>,
}

impl Journal {
    pub fn insert_running(&mut self, id: u64) {
        self.states.insert(id, TaskState::Running);
    }

    pub fn finish(&mut self, id: u64, state: TaskState) {
        self.states.insert(id, state);
    }

    pub fn get(&self, id: u64) -> Option<TaskState> {
        self.states.get(&id).copied()
    }
}
