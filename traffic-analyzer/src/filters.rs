pub mod regex;
pub mod bsubstr;
pub mod substr;
pub mod filterDTO;

use log::error;
use once_cell::sync::Lazy;
use std::sync::RwLock;
use crate::filters::filterDTO::Filter;
use crate::filters::regex::RegexFilter;
use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::substr::SubstrFilter;

// Global storage for filters
pub static PACKET_FILTERS: Lazy<RwLock<Vec<Box<dyn Filter>>>> = Lazy::new(|| {
    RwLock::new(vec![
        Box::new(RegexFilter::new("regex_filter", "GET /\\w*").unwrap()),
        Box::new(BinarySubstrFilter::new("binary_substr_filter", b"1234567890")),
        Box::new(SubstrFilter::new("substr_filter", b"1234567890")),
    ])
});

// Function to add filters
pub fn add_filter(pattern_name: String, pattern_type: String, payload: &[u8]) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    match pattern_type.to_lowercase().as_str() {
        "regex" => {
            let payload_str = match String::from_utf8(payload.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Error converting payload to string: {}", e);
                    return;
                }
            };

            match RegexFilter::new(&pattern_name, &payload_str) {
                Ok(filter) => filters.push(Box::new(filter)),
                Err(e) => error!("Error creating regex filter: {}", e),
            }
        }
        "bsubstr" => {
            filters.push(Box::new(BinarySubstrFilter::new(&pattern_name, &payload)));
        }
        "substr" => {
            filters.push(Box::new(SubstrFilter::new(&pattern_name, &payload)));
        }
        _ => {
            error!("Unsupported filter type: {}", pattern_type);
        }
    }
}

pub fn remove_filter(index: usize) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    // Remove filter by index if it exists
    if index < filters.len() {
        filters.remove(index);
    } else {
        error!("Попытка удалить фильтр с несуществующим индексом: {}", index);
    }
}

pub fn edit_filter(index: usize, pattern_name: String, pattern_type: String, payload: &[u8]) {
    remove_filter(index);
    add_filter(pattern_name, pattern_type, payload);
}

// Function to apply all filters to a packet
pub fn apply_filters(packet: &[u8]) -> Vec<(String, Vec<filterDTO::FilterTrigger>)> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut results: Vec<(String, Vec<filterDTO::FilterTrigger>)> = Vec::new();
    
    for filter in filters.iter() {
        let triggers = filter.processed(packet);
        if !triggers.is_empty() {
            results.push((filter.get_pattern_name(), triggers));
        }
    }
    
    results
}

