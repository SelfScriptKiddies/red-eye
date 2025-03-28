use crate::filters::filterDTO::{Filter, FilterTrigger};
use log::debug;

pub struct BinarySubstrFilter {
    id: usize,
    pattern_name: String,
    binary_pattern: Vec<u8>,
}

impl BinarySubstrFilter {
    pub fn new(id: usize, pattern_name: &str, binary_pattern: &[u8]) -> Self {
        BinarySubstrFilter {
            id,
            pattern_name: pattern_name.to_string(),
            binary_pattern: binary_pattern.to_vec(),
        }
    }
}

impl Filter for BinarySubstrFilter {
    fn get_id(&self) -> usize {
        self.id
    }

    fn get_pattern_name(&self) -> String {
        self.pattern_name.clone()
    }

    fn processed(&self, packet: &[u8]) -> Vec<FilterTrigger> {
        let mut triggers = Vec::new();
        
        for position in find_binary_pattern(packet, &self.binary_pattern) {
            triggers.push(FilterTrigger {
                start_index: position,
                end_index: position + self.binary_pattern.len(),
            });
        }
        
        debug!("Checking packet for binary pattern '{}': found {} matches", 
               self.pattern_name, 
               triggers.len());
               
        triggers
    }
}

fn find_binary_pattern(haystack: &[u8], needle: &[u8]) -> Vec<usize> {
    let mut positions = Vec::new();
    
    if needle.is_empty() {
        return vec![0];
    }
    if needle.len() > haystack.len() {
        return positions;
    }
    
    // KMP algorithm
    let lps = compute_lps_array(needle);
    
    let mut i = 0;
    let mut j = 0;
    
    while i < haystack.len() {
        if needle[j] == haystack[i] {
            i += 1;
            j += 1;
        }
        
        if j == needle.len() {
            positions.push(i - j); // Add position where pattern was found
            j = lps[j - 1]; // Continue searching from the last partial match
        } else if i < haystack.len() && needle[j] != haystack[i] {
            if j != 0 {
                j = lps[j - 1];
            } else {
                i += 1;
            }
        }
    }
    
    positions
}

fn compute_lps_array(pattern: &[u8]) -> Vec<usize> {
    let mut lps = vec![0; pattern.len()];
    let mut len = 0;
    let mut i = 1;
    
    while i < pattern.len() {
        if pattern[i] == pattern[len] {
            len += 1;
            lps[i] = len;
            i += 1;
        } else {
            if len != 0 {
                len = lps[len - 1];
            } else {
                lps[i] = 0;
                i += 1;
            }
        }
    }
    
    lps 
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_substr_filter_match() {
        let filter = BinarySubstrFilter::new(1, "test_pattern", &[0x01, 0x02, 0x03]);
        let packet = &[0x00, 0x01, 0x02, 0x03, 0x04];
        let result = filter.processed(packet);
        assert!(!result.is_empty());
        assert_eq!(result[0].start_index, 1);
        assert_eq!(result[0].end_index, 4);
    }

    #[test]
    fn test_binary_substr_filter_no_match() {
        let filter = BinarySubstrFilter::new(1, "test_pattern", &[0x05, 0x06]);
        let packet = &[0x00, 0x01, 0x02, 0x03, 0x04];
        let result = filter.processed(packet);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_binary_pattern() {
        let filter = BinarySubstrFilter::new(1, "test_pattern", &[]);
        let packet = &[0x00, 0x01, 0x02];
        let result = filter.processed(packet);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_binary_pattern_longer_than_packet() {
        let filter = BinarySubstrFilter::new(1, "test_pattern", &[0x01, 0x02, 0x03, 0x04, 0x05]);
        let packet = &[0x01, 0x02, 0x03];
        let result = filter.processed(packet);
        assert!(result.is_empty());
    }
}
