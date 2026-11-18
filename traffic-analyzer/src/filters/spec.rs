//! Unified filter specification shared by every matcher variant: targets,
//! action, threshold, expiry and revision. This is the analyzer-side image
//! of the backend's unified filter entity.

use std::collections::HashSet;
use std::net::IpAddr;

/// Action a matching filter can produce. Values mirror the backend
/// (tag_name.action) one to one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Tag,
    Flag,
    Alert,
    Block,
    BlockConnection,
}

impl Action {
    pub fn from_wire_str(value: &str) -> Option<Self> {
        Some(match value {
            "tag" => Action::Tag,
            "flag" => Action::Flag,
            "alert" => Action::Alert,
            "block" => Action::Block,
            "block_connection" => Action::BlockConnection,
            _ => return None,
        })
    }
}

/// Which packet endpoint an IP/CIDR matcher inspects: the ORIGINAL packet
/// before stream tuple normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpEndpointSel {
    Src,
    Dst,
}

/// Threshold counter grouping value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupBy {
    /// Normalized remote peer address string.
    RemoteIp,
    Stream,
    Service,
}

/// Threshold configuration; valid only for alert and block actions.
#[derive(Debug, Clone)]
pub struct ThresholdCfg {
    pub count: u32,
    pub window_secs: u32,
    pub group_by: GroupBy,
    pub cooldown_secs: u32,
    /// Block length for threshold block actions.
    pub block_secs: u32,
}

/// Independent service and agent targets. Global includes future entities.
#[derive(Debug, Clone, Default)]
pub struct Targets {
    pub services_all: bool,
    pub service_ids: HashSet<u64>,
    pub agents_all: bool,
    pub agent_ids: HashSet<String>,
}

impl Targets {
    pub fn all() -> Self {
        Targets {
            services_all: true,
            service_ids: HashSet::new(),
            agents_all: true,
            agent_ids: HashSet::new(),
        }
    }

    pub fn service_applies(&self, service_id: u64) -> bool {
        self.services_all || self.service_ids.contains(&service_id)
    }

    pub fn agent_applies(&self, agent_id: &str) -> bool {
        self.agents_all || self.agent_ids.contains(agent_id)
    }
}

/// Complete filter definition as evaluated by the analyzer.
#[derive(Debug, Clone)]
pub struct FilterSpec {
    pub id: u64,
    pub name: String,
    pub direction: crate::filters::filterDTO::FilterDirection,
    pub action: Action,
    pub enabled: bool,
    pub revision: u64,
    pub targets: Targets,
    pub threshold: Option<ThresholdCfg>,
    /// Epoch millis; 0 = never expires.
    pub expires_at_ms: u64,
    /// Matcher: (kind, payload) for payload kinds; CIDR for ip_cidr;
    /// CEL source for expression.
    pub matcher: MatcherSpec,
}

#[derive(Debug, Clone)]
pub enum MatcherSpec {
    /// regex | substr | bsubstr; payload is the matcher input exactly as
    /// delivered (bsubstr: hex string; the matcher decodes).
    Payload {
        kind: String,
        payload: Vec<u8>,
    },
    IpCidr {
        /// Host address with prefix length (normalized upstream).
        network: IpAddr,
        prefix: u8,
        endpoint: IpEndpointSel,
    },
    Expression {
        source: String,
    },
}
