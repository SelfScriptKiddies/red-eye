//! Minimal IPv4/IPv6 address and CIDR parsing plus containment.
//!
//! Families never mix: an IPv4 address matches only IPv4 networks, IPv6
//! only IPv6 networks. No implicit v4<->v6 conversion (no IPv4-mapped
//! tunneling), so a rule for 10.0.0.0/8 cannot silently match ::ffff:10.0.0.1.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFamily {
    V4,
    V6,
}

#[derive(Debug, Clone, Copy)]
pub struct ParsedIp {
    pub family: IpFamily,
    /// Address bits: for V4 the low 32 bits are the address, the rest zero.
    pub bits: u128,
}

#[derive(Debug, Clone, Copy)]
pub struct ParsedCidr {
    pub ip: ParsedIp,
    pub prefix: u8,
    /// Already-masked network bits.
    pub network: u128,
}

/// Strict IPv4 dotted-quad parse: exactly four decimal octets, each 0-255.
pub fn parse_ipv4(s: &str) -> Option<ParsedIp> {
    let mut parts = s.split('.');
    let mut value = 0u128;
    for n in 0..4 {
        let part = parts.next()?;
        if part.is_empty() || part.len() > 3 {
            return None;
        }
        let octet: u16 = part.parse().ok()?;
        // Reject leading zeros ("010" is not an octet spelling here) and
        // non-digit characters ("+1", " 1").
        if octet > 255 || (part.len() > 1 && part.starts_with('0')) {
            return None;
        }
        if n == 3 && parts.next().is_some() {
            return None;
        }
        value = (value << 8) | u128::from(octet);
    }
    Some(ParsedIp {
        family: IpFamily::V4,
        bits: value,
    })
}

/// IPv6 parse: full form, :: compression (at most one), and an optional
/// embedded IPv4 tail. No scope/zone ("::%eth0") or v4-mapped conversions.
pub fn parse_ipv6(s: &str) -> Option<ParsedIp> {
    if s.contains('%') || !s.is_ascii() {
        return None;
    }
    let (head, tail) = match s.split_once("::") {
        Some((h, t)) => {
            // At most one "::".
            if t.contains("::") {
                return None;
            }
            (h, Some(t))
        }
        None => (s, None),
    };

    let mut groups = [0u16; 8];
    let mut count = 0usize;

    let parse_groups = |text: &str, groups: &mut [u16; 8], count: &mut usize| -> Option<()> {
        if text.is_empty() {
            return Some(());
        }
        let parts: Vec<&str> = text.split(':').collect();
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                // Leading or doubled ':' around "::".
                return None;
            }
            if part.contains('.') {
                // Embedded IPv4 tail is legal only as the last group(s).
                if i != parts.len() - 1 {
                    return None;
                }
                let ip = parse_ipv4(part)?;
                let hi = ((ip.bits >> 16) & 0xffff) as u16;
                let lo = (ip.bits & 0xffff) as u16;
                *groups.get_mut(*count)? = hi;
                *count += 1;
                *groups.get_mut(*count)? = lo;
                *count += 1;
                return Some(());
            }
            if part.len() > 4 {
                return None;
            }
            let v = u16::from_str_radix(part, 16).ok()?;
            *groups.get_mut(*count)? = v;
            *count += 1;
        }
        Some(())
    };

    parse_groups(head, &mut groups, &mut count)?;
    match tail {
        Some(t) => {
            let mut tail_groups = [0u16; 8];
            let mut tail_count = 0usize;
            parse_groups(t, &mut tail_groups, &mut tail_count)?;
            if count + tail_count > 8 {
                return None;
            }
            let mut out = [0u16; 8];
            out[..count].copy_from_slice(&groups[..count]);
            out[8 - tail_count..].copy_from_slice(&tail_groups[..tail_count]);
            groups = out;
        }
        None => {
            if count != 8 {
                return None;
            }
        }
    }

    let bits = groups
        .iter()
        .fold(0u128, |acc, g| (acc << 16) | u128::from(*g));
    Some(ParsedIp {
        family: IpFamily::V6,
        bits,
    })
}

pub fn parse_ip(s: &str) -> Option<ParsedIp> {
    parse_ipv4(s).or_else(|| parse_ipv6(s))
}

/// Prefix mask bounded to the family's address width.
fn family_mask(family: IpFamily, prefix: u8) -> u128 {
    let max = match family {
        IpFamily::V4 => 32u8,
        IpFamily::V6 => 128u8,
    };
    let shift = u32::from(max - prefix);
    let mask = if shift >= 128 { 0 } else { u128::MAX << shift };
    if family == IpFamily::V4 {
        mask & 0xffff_ffff
    } else {
        mask
    }
}

/// Parse "addr/prefix" or a bare address (host prefix).
pub fn parse_cidr(s: &str) -> Option<ParsedCidr> {
    let (addr_text, prefix) = match s.rsplit_once('/') {
        Some((a, p)) => {
            let prefix: u8 = p.parse().ok()?;
            (a, prefix)
        }
        None => (s, 0),
    };
    let ip = parse_ip(addr_text)?;
    let max = match ip.family {
        IpFamily::V4 => 32u8,
        IpFamily::V6 => 128u8,
    };
    let prefix = if s.contains('/') { prefix } else { max };
    if prefix > max {
        return None;
    }
    let network = ip.bits & family_mask(ip.family, prefix);
    Some(ParsedCidr {
        ip,
        prefix,
        network,
    })
}

/// Family-aware containment: mismatched families are never equal.
pub fn cidr_contains(cidr: &ParsedCidr, ip: &ParsedIp) -> bool {
    if cidr.ip.family != ip.family {
        return false;
    }
    (ip.bits & family_mask(ip.family, cidr.prefix)) == cidr.network
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv4_parses_strictly() {
        assert!(parse_ipv4("10.0.0.1").is_some());
        assert!(parse_ipv4("255.255.255.255").is_some());
        assert!(parse_ipv4("256.0.0.1").is_none());
        assert!(parse_ipv4("1.2.3").is_none());
        assert!(parse_ipv4("1.2.3.4.5").is_none());
        assert!(parse_ipv4("010.0.0.1").is_none());
        assert!(parse_ipv4("a.b.c.d").is_none());
        assert!(parse_ipv4("").is_none());
    }

    #[test]
    fn ipv6_parses_compressed_and_embedded_v4() {
        assert!(parse_ipv6("::1").is_some());
        assert!(parse_ipv6("fd00::5").is_some());
        assert!(parse_ipv6("fd00:0:0:0:0:0:0:5").is_some());
        assert!(parse_ipv6("::ffff:1.2.3.4").is_some());
        assert!(parse_ipv6("1:2:3:4:5:6:7:8").is_some());
        assert!(parse_ipv6("1:2:3:4:5:6:7:8:9").is_none());
        assert!(parse_ipv6("1::2::3").is_none());
        assert!(parse_ipv6("fd00::5%eth0").is_none());
        assert!(parse_ipv6("zz::1").is_none());
    }

    #[test]
    fn cidr_containment_v4() {
        let net = parse_cidr("10.0.0.0/8").unwrap();
        assert!(cidr_contains(&net, &parse_ip("10.255.1.2").unwrap()));
        assert!(!cidr_contains(&net, &parse_ip("11.0.0.1").unwrap()));
        let host = parse_cidr("10.0.0.1").unwrap();
        assert_eq!(host.prefix, 32);
        assert!(cidr_contains(&host, &parse_ip("10.0.0.1").unwrap()));
        assert!(!cidr_contains(&host, &parse_ip("10.0.0.2").unwrap()));
        assert!(parse_cidr("10.0.0.0/33").is_none());
        assert!(parse_cidr("10.0.0.0/-1").is_none());
    }

    #[test]
    fn cidr_containment_v6_and_no_family_mixing() {
        let net = parse_cidr("fd00::/16").unwrap();
        assert!(cidr_contains(&net, &parse_ip("fd00::5").unwrap()));
        assert!(!cidr_contains(&net, &parse_ip("fd01::5").unwrap()));
        // v4 address never matches a v6 network and vice versa.
        assert!(!cidr_contains(&net, &parse_ip("10.0.0.1").unwrap()));
        let v4net = parse_cidr("10.0.0.0/8").unwrap();
        assert!(!cidr_contains(
            &v4net,
            &parse_ip("::ffff:10.0.0.1").unwrap()
        ));
    }

    #[test]
    fn cidr_masks_host_bits() {
        let net = parse_cidr("10.1.2.3/8").unwrap();
        assert_eq!(net.network, u128::from(10u32) << 24);
    }
}
