use serde::{Deserialize, Serialize};

pub trait Filter {
    fn processed(&self, packet: &[u8]) -> Vec<FilterTrigger>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterDTO {
    pub pattern_name: String,
    pub filter_type: String,
    pub filter_payload: String,
}

pub struct FilterTrigger {
    pub start_index: usize,
    pub end_index: usize
}
