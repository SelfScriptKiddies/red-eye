use regex::Regex;
use crate::filters::filterDTO::Filter;
use log::{debug, error};

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
        let mut triggers = Vec::new();
        
        match std::str::from_utf8(packet) {
            Ok(text) => {
                // Находим все совпадения и сохраняем их позиции
                for mat in self.regex.find_iter(text) {
                    triggers.push(crate::filters::filterDTO::FilterTrigger {
                        start_index: mat.start(),
                        end_index: mat.end(),
                    });
                }
                
                debug!("Checking packet with regex '{}': {} matches found", 
                       self.pattern_name, triggers.len());
            },
            Err(_) => {
                debug!("Packet is not valid UTF-8, does not match regex '{}'", self.pattern_name);
            }
        }
        
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
    fn test_invalid_utf8() {
        let filter = RegexFilter::new(1, "test_pattern", r"hello").unwrap();
        let packet = &[0xFF, 0xFE, 0xFD]; // Недопустимый UTF-8
        assert!(filter.processed(packet).is_empty());
    }
}
