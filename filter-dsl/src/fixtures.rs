//! Shared analyzer/agent contract fixtures.
//!
//! Both the traffic-analyzer and the agent run these cases through their
//! own matcher paths and must produce the identical `expected` results
//! (spec: "Shared analyzer/agent matcher fixtures produce identical
//! results"). The fixture set covers every expression field, operator
//! precedence, ip_in_cidr family separation, byte containment, and the
//! non-match-on-error rule.

/// Owned descriptor of one packet; converts to a borrowed EvalInput.
pub struct FixturePacket {
    pub payload: &'static [u8],
    pub src_ip: &'static str,
    pub dst_ip: &'static str,
    pub src_port: u64,
    pub dst_port: u64,
    pub local_ip: &'static str,
    pub remote_ip: &'static str,
    pub local_port: u64,
    pub remote_port: u64,
    pub protocol: &'static str,
    pub direction: &'static str,
    pub service_id: u64,
    pub agent_id: &'static str,
    pub timestamp: u64,
}

pub struct FixtureCase {
    pub name: &'static str,
    pub source: &'static str,
    pub packet: FixturePacket,
    /// Expected boolean result; `expect_error` cases must yield Err and are
    /// treated as non-match by the caller.
    pub expected: bool,
    /// The expression must FAIL TO COMPILE (rejected before persistence).
    pub expect_compile_error: bool,
    /// The expression compiles but must fail AT EVALUATION (non-match).
    pub expect_eval_error: bool,
}

impl FixturePacket {
    pub fn to_eval_input(&self) -> crate::EvalInput<'_> {
        crate::EvalInput {
            payload: self.payload,
            src_ip: self.src_ip,
            dst_ip: self.dst_ip,
            src_port: self.src_port,
            dst_port: self.dst_port,
            local_ip: self.local_ip,
            remote_ip: self.remote_ip,
            local_port: self.local_port,
            remote_port: self.remote_port,
            protocol: self.protocol,
            direction: self.direction,
            service_id: self.service_id,
            agent_id: self.agent_id,
            timestamp: self.timestamp,
        }
    }
}

/// Baseline packet used by most cases.
const BASE: FixturePacket = FixturePacket {
    payload: b"GET /flag{abc123} HTTP/1.1\r\nHost: 10.10.2.4\r\n",
    src_ip: "10.10.9.7",
    dst_ip: "10.10.2.4",
    src_port: 40000,
    dst_port: 80,
    local_ip: "10.10.2.4",
    remote_ip: "10.10.9.7",
    local_port: 80,
    remote_port: 40000,
    protocol: "TCP",
    direction: "in",
    service_id: 7,
    agent_id: "agent-alpha",
    timestamp: 1_700_000_000,
};

macro_rules! packet_like {
    ($base:expr, $($field:ident : $value:expr),* $(,)?) => {{
        let mut pkt = $base;
        $(pkt.$field = $value;)*
        pkt
    }};
}

const PAYLOAD_MATCH: &str = "payload.contains(b\"flag{\")";
const PAYLOAD_NO_MATCH: &str = "payload.contains(b\"nothere\")";
const INBOUND: &str = "direction == \"in\" && protocol == \"TCP\"";

/// Build the fixture set. Used by the DSL crate's own tests; the analyzer
/// and agent re-run the same cases through their matcher layers.
pub fn fixtures() -> Vec<FixtureCase> {
    vec![
        // Payload containment, both orientations.
        FixtureCase {
            name: "payload_contains_hit",
            source: PAYLOAD_MATCH,
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "payload_contains_miss",
            source: PAYLOAD_NO_MATCH,
            packet: BASE,
            expected: false,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Field coverage.
        FixtureCase {
            name: "src_ip_exact",
            source: "src_ip == \"10.10.9.7\"",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "dst_ip_exact",
            source: "dst_ip == \"10.10.2.4\"",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "ports_uint_compare",
            source: "src_port >= 40000 && dst_port < 443",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "local_remote_fields",
            source: "local_ip == dst_ip && remote_ip == src_ip && local_port == dst_port && remote_port == src_port",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "service_and_agent",
            source: "service_id == 7 && agent_id == \"agent-alpha\"",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "timestamp_bound",
            source: "timestamp > 1699999999",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Precedence: && binds tighter than ||.
        FixtureCase {
            name: "precedence_and_over_or",
            source: "direction == \"out\" || direction == \"in\" && protocol == \"TCP\"",
            packet: packet_like! {
                BASE,
                direction: "out",
                protocol: "UDP"
            },
            // Parsed as: out || (in && TCP) - the left disjunct alone wins.
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "negation_in_group",
            source: "!(direction == \"out\") && !(protocol == \"UDP\")",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Direction/protocol negatives on a real grammar.
        FixtureCase {
            name: "inbound_tcp_true",
            source: INBOUND,
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "inbound_tcp_false_for_outbound_udp",
            source: INBOUND,
            packet: packet_like! {
                BASE,
                direction: "out",
                protocol: "UDP",
                src_ip: "10.10.2.4",
                dst_ip: "10.10.9.7",
                local_ip: "10.10.2.4",
                remote_ip: "10.10.9.7",
                local_port: 40000,
                remote_port: 80,
                src_port: 40000,
                dst_port: 80
            },
            expected: false,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // ip_in_cidr: v4 hit and miss.
        FixtureCase {
            name: "cidr_v4_hit",
            source: "ip_in_cidr(remote_ip, \"10.10.0.0/16\")",
            packet: BASE,
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "cidr_v4_miss",
            source: "ip_in_cidr(remote_ip, \"10.11.0.0/16\")",
            packet: BASE,
            expected: false,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // No implicit v4<->v6 matching.
        FixtureCase {
            name: "cidr_family_never_mixes",
            source: "ip_in_cidr(remote_ip, \"::ffff:10.10.0.0/104\")",
            packet: BASE,
            expected: false,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "cidr_v6_hit",
            source: "ip_in_cidr(remote_ip, \"fd00::/16\")",
            packet: packet_like! {
                BASE,
                src_ip: "fd00::9",
                dst_ip: "fd00::4",
                local_ip: "fd00::4",
                remote_ip: "fd00::9"
            },
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Compound example straight from the spec.
        FixtureCase {
            name: "spec_example_compound",
            source: "protocol == \"TCP\" && direction == \"in\" && ip_in_cidr(remote_ip, \"10.0.0.0/8\") && payload.contains(b\"token\")",
            packet: packet_like! {
                BASE,
                payload: b"POST /x token=y HTTP/1.1\r\n"
            },
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Byte-literal containment with a NUL byte.
        FixtureCase {
            name: "binary_contains_nul",
            source: "payload.contains(b\"\\x00\\x01binary\")",
            packet: packet_like! {
                BASE,
                payload: &[0u8, 1, b'b', b'i', b'n', b'a', b'r', b'y', 0xff]
            },
            expected: true,
            expect_compile_error: false,
            expect_eval_error: false,
        },
        // Evaluation error => non-match: dynamic IP argument is garbage.
        FixtureCase {
            name: "eval_error_invalid_ip_is_non_match",
            source: "ip_in_cidr(agent_id, \"10.0.0.0/8\")",
            packet: BASE,
            expected: false,
            expect_compile_error: false,
            expect_eval_error: true,
        },
        // Compile-time rejections (must never persist).
        FixtureCase {
            name: "reject_string_uint_compare",
            source: "src_port == \"80\"",
            packet: BASE,
            expected: false,
            expect_compile_error: true,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "reject_mixed_contains",
            source: "src_ip.contains(b\"a\")",
            packet: BASE,
            expected: false,
            expect_compile_error: true,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "reject_non_boolean_root",
            source: "src_port + 1",
            packet: BASE,
            expected: false,
            expect_compile_error: true,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "reject_unknown_field",
            source: "http_host == \"x\"",
            packet: BASE,
            expected: false,
            expect_compile_error: true,
            expect_eval_error: false,
        },
        FixtureCase {
            name: "reject_assignment",
            source: "src_port = 80",
            packet: BASE,
            expected: false,
            expect_compile_error: true,
            expect_eval_error: false,
        },
    ]
}
/// Run the fixture set against this crate's compile+eval. Used by the DSL
/// crate's own tests; the analyzer and agent re-run the same cases through
/// their matcher layers.
pub fn run_fixtures() -> Result<(), String> {
    for case in fixtures() {
        let compiled = crate::compile(case.source);
        if case.expect_compile_error {
            if compiled.is_ok() {
                return Err(format!(
                    "fixture '{}': expected compile error, compiled ok",
                    case.name
                ));
            }
            continue;
        }
        let expr =
            compiled.map_err(|e| format!("fixture '{}': compile failed: {}", case.name, e))?;
        let outcome = crate::eval(&expr, &case.packet.to_eval_input());
        if case.expect_eval_error {
            if outcome.is_ok() {
                return Err(format!(
                    "fixture '{}': expected eval error, evaluated ok",
                    case.name
                ));
            }
            continue;
        }
        match outcome {
            Ok(v) if v == case.expected => {}
            Ok(v) => {
                return Err(format!(
                    "fixture '{}': expected {}, got {}",
                    case.name, case.expected, v
                ));
            }
            Err(e) => return Err(format!("fixture '{}': eval error {}", case.name, e)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_fixtures_pass_in_the_dsl_itself() {
        super::run_fixtures().unwrap();
    }
}
