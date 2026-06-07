use std::sync::{Mutex, RwLock};

pub struct RouterState {
    pub backends: RwLock<Vec<String>>,
    pub current_index: Mutex<Option<usize>>,
}
