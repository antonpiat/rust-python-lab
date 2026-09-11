use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Running,
    Succeeded,
    Failed,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
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

    pub fn finish(&mut self, id: u64, ok: bool) {
        self.states.insert(
            id,
            if ok {
                TaskState::Succeeded
            } else {
                TaskState::Failed
            },
        );
    }

    pub fn get(&self, id: u64) -> Option<TaskState> {
        self.states.get(&id).copied()
    }
}
