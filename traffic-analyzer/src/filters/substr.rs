use crate::filters::filterDTO::{Filter, FilterTrigger};
use log::debug;

pub struct SubstrFilter {
    pattern_name: String,
    substring: Vec<u8>,
}

impl SubstrFilter {
    pub fn new(pattern_name: &str, substring: &[u8]) -> Self {
        SubstrFilter {
            pattern_name: pattern_name.to_string(),
            substring: substring.to_vec(),
        }
    }
    
    pub fn get_pattern_name(&self) -> &str {
        &self.pattern_name
    }
}

impl Filter for SubstrFilter {
    fn processed(&self, packet: &[u8]) -> Vec<FilterTrigger> {
        let mut triggers = Vec::new();
        
        if let Some(positions) = find_all_subsequences(packet, &self.substring) {
            for (start, end) in positions {
                triggers.push(FilterTrigger {
                    start_index: start,
                    end_index: end,
                });
            }
        }
        
        debug!("Checking packet for substring '{}': {} matches found", 
               self.pattern_name, triggers.len());
        
        triggers
    }
}

fn find_all_subsequences(haystack: &[u8], needle: &[u8]) -> Option<Vec<(usize, usize)>> {
    if needle.is_empty() {
        return Some(vec![(0, 0)]);
    }
    if needle.len() > haystack.len() {
        return None;
    }
    
    let lps = compute_lps_array(needle);
    let mut matches = Vec::new();
    
    let mut i = 0;
    let mut j = 0;
    
    while i < haystack.len() {
        if needle[j] == haystack[i] {
            i += 1;
            j += 1;
        }
        
        if j == needle.len() {
            let start = i - j;
            let end = i;
            matches.push((start, end));
            j = lps[j - 1];
        } else if i < haystack.len() && needle[j] != haystack[i] {
            if j != 0 {
                j = lps[j - 1];
            } else {
                i += 1;
            }
        }
    }
    
    if matches.is_empty() {
        None
    } else {
        Some(matches)
    }
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
    fn test_substr_filter_match() {
        let filter = SubstrFilter::new("test_pattern", b"hello");
        let packet = b"hello world";
        assert!(filter.processed(packet).len() > 0);
    }

    #[test]
    fn test_substr_filter_no_match() {
        let filter = SubstrFilter::new("test_pattern", b"goodbye");
        let packet = b"hello world";
        assert!(filter.processed(packet).is_empty());
    }

    #[test]
    fn test_empty_substring() {
        let filter = SubstrFilter::new("test_pattern", b"");
        let packet = b"hello world";
        assert!(filter.processed(packet).len() > 0);
    }

    #[test]
    fn test_substring_longer_than_packet() {
        let filter = SubstrFilter::new("test_pattern", b"hello world and more");
        let packet = b"hello world";
        assert!(filter.processed(packet).is_empty());
    }
}
