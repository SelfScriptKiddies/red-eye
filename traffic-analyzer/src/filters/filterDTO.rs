use serde::{Deserialize, Serialize};

pub trait Filter: Send + Sync {
    fn get_id(&self) -> usize;
    fn get_pattern_name(&self) -> String;
    fn processed(&self, packet: &[u8]) -> Vec<FilterTrigger>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterDTO {
    pub id: usize,
    pub pattern_name: String,
    pub filter_type: String,
    pub filter_payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterTrigger {
    pub start_index: usize,
    pub end_index: usize
}
