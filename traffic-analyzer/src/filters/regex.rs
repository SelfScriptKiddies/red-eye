use crate::filters::filterDTO::Filter;
use log::{error, trace};
use regex::bytes::Regex;

pub struct RegexFilter {
    id: usize,
    pattern_name: String,
    regex: Regex,
}

impl RegexFilter {
    pub fn new(id: usize, pattern_name: &str, pattern: &str) -> Result<Self, regex::Error> {
        match Regex::new(pattern) {
            Ok(regex) => Ok(RegexFilter {
                id,
                pattern_name: pattern_name.to_string(),
                regex,
            }),
            Err(e) => {
                error!("Error compiling regex '{}': {}", pattern, e);
                Err(e)
            }
        }
    }
}

impl Filter for RegexFilter {
    fn get_id(&self) -> usize {
        self.id
    }

    fn get_pattern_name(&self) -> String {
        self.pattern_name.clone()
    }

    fn processed(&self, packet: &[u8]) -> Vec<crate::filters::filterDTO::FilterTrigger> {
        // Match directly on raw bytes (regex::bytes): a single non-UTF-8
        // byte anywhere in the packet must NOT hide matches in other
        // regions (length-prefixed framing, compressed bodies, etc.).
        // Byte offsets == payload offsets, same semantics as substr/bsubstr.
        // Patterns are user-supplied UTF-8 strings; with the default Unicode
        // mode `.` matches UTF-8 sequences and will not span invalid bytes —
        // ASCII patterns (flags, markers) match fine amid binary garbage.
        let mut triggers = Vec::new();
        for mat in self.regex.find_iter(packet) {
            triggers.push(crate::filters::filterDTO::FilterTrigger {
                start_index: mat.start(),
                end_index: mat.end(),
            });
        }

        trace!(
            "Checking packet with regex '{}': {} matches found",
            self.pattern_name,
            triggers.len()
        );

        triggers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regex_filter_match() {
        let filter = RegexFilter::new(1, "test_pattern", r"hello\s+world").unwrap();
        let packet = b"hello world";
        assert!(filter.processed(packet).len() > 0);
    }

    #[test]
    fn test_regex_filter_no_match() {
        let filter = RegexFilter::new(1, "test_pattern", r"hello\s+world").unwrap();
        let packet = b"goodbye world";
        assert!(filter.processed(packet).is_empty());
    }

    #[test]
    fn test_binary_garbage_with_embedded_ascii_match() {
        // Binary framing bytes + an embedded ASCII flag string: the match
        // MUST still be found. One invalid byte used to hide ALL matches.
        let filter = RegexFilter::new(1, "flag_pattern", r"flag\{[a-z0-9_]+\}").unwrap();
        let packet = b"\x00\x02\x00\x01\xFF\xFEflag{stolen_secret}\x00\xFF\x10";
        let triggers = filter.processed(packet);
        assert_eq!(triggers.len(), 1);
        assert_eq!(
            &packet[triggers[0].start_index..triggers[0].end_index],
            b"flag{stolen_secret}"
        );
        // Offsets must address the raw payload (binary prefix counts).
        assert_eq!(triggers[0].start_index, 6);
        assert_eq!(triggers[0].end_index, 25);
    }

    #[test]
    fn test_binary_packet_without_match_stays_empty() {
        let filter = RegexFilter::new(1, "test_pattern", r"hello").unwrap();
        let packet = &[0xFF, 0xFE, 0xFD]; // binary garbage, no match
        assert!(filter.processed(packet).is_empty());
    }
}
