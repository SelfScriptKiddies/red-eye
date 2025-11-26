pub mod regex;
pub mod bsubstr;
pub mod substr;
pub mod filterDTO;

use log::{error, debug};
use once_cell::sync::Lazy;
use std::sync::RwLock;
use crate::filters::filterDTO::Filter;
use crate::filters::regex::RegexFilter;
use crate::filters::bsubstr::BinarySubstrFilter;
use crate::filters::substr::SubstrFilter;

// Global storage for filters
pub static PACKET_FILTERS: Lazy<RwLock<Vec<Box<dyn Filter>>>> = Lazy::new(|| {
    RwLock::new(vec![
        // Box::new(RegexFilter::new(1, "regex_filter", "GET /\\w*").unwrap()),
        // Box::new(BinarySubstrFilter::new(2, "binary_substr_filter", b"1234567890")),
        // Box::new(SubstrFilter::new(3, "substr_filter", b"1234567890")),
    ])
});

// Function to add filters
pub fn add_filter(id: usize, pattern_name: String, pattern_type: String, payload: &[u8]) {
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

            match RegexFilter::new(id, &pattern_name, &payload_str) {
                Ok(filter) => filters.push(Box::new(filter)),
                Err(e) => error!("Error creating regex filter: {}", e),
            }
        }
        "bsubstr" => {
            filters.push(Box::new(BinarySubstrFilter::new(id, &pattern_name, &payload)));
        }
        "substr" => {
            filters.push(Box::new(SubstrFilter::new(id, &pattern_name, &payload)));
        }
        _ => {
            error!("Unsupported filter type: {}", pattern_type);
        }
    }
}

pub fn remove_filter(index: usize) {
    let mut filters = PACKET_FILTERS.write().unwrap();
    
    // Find the index of the filter with the specified id
    if let Some(position) = filters.iter().position(|filter| filter.get_id() == index) {
        // Remove the filter by found index
        filters.remove(position);
        debug!("Filter with id {} removed successfully", index);
    } else {
        error!("Filter with id {} not found", index);
    }
   
}

pub fn edit_filter(index: usize, pattern_name: String, pattern_type: String, payload: &[u8]) {
    remove_filter(index);
    add_filter(index, pattern_name, pattern_type, payload);
}

pub fn get_filter_ids() -> Vec<usize> {
    let filters = PACKET_FILTERS.read().unwrap();
    filters.iter().map(|f| f.get_id()).collect()
}

pub fn sync_filters(filters_data: Vec<(usize, String, String, Vec<u8>)>) {
    use std::collections::HashSet;
    
    let incoming_ids: HashSet<usize> = filters_data.iter().map(|(id, _, _, _)| *id).collect();
    let existing_ids: HashSet<usize> = get_filter_ids().into_iter().collect();
    
    // Находим отсутствующие и лишние
    let missing: Vec<_> = filters_data
        .into_iter()
        .filter(|(id, _, _, _)| !existing_ids.contains(id))
        .collect();
    let to_remove: Vec<_> = existing_ids
        .difference(&incoming_ids)
        .cloned()
        .collect();
    
    if missing.is_empty() && to_remove.is_empty() {
        debug!("Filters sync: no changes needed");
        return;
    }
    
    debug!("Filters sync: adding {} missing, removing {} stale", missing.len(), to_remove.len());
    
    // Удаляем лишние
    for id in to_remove {
        remove_filter(id);
    }
    
    // Добавляем отсутствующие
    for (id, pattern_name, pattern_type, payload) in missing {
        add_filter(id, pattern_name, pattern_type, &payload);
    }
}

// Function to apply all filters to a packet
pub fn apply_filters(packet: &[u8]) -> Vec<(usize, Vec<filterDTO::FilterTrigger>)> {
    let filters = PACKET_FILTERS.read().unwrap();
    let mut results: Vec<(usize, Vec<filterDTO::FilterTrigger>)> = Vec::new();
    
    for filter in filters.iter() {
        let triggers = filter.processed(packet);
        if !triggers.is_empty() {
            results.push((filter.get_id(), triggers));
        }
    }
    
    results
}

