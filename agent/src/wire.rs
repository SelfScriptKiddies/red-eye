//! Wire-format types generated from the top-level `proto/wire.proto`
//! (symlinked into this crate as `./proto`, compiled by `build.rs`).

pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::settings_envelope;
pub use red_eye::wire::PacketBatch;
// NetworkSettings/Service re-exports are consumed by snapshot tests.
#[allow(unused_imports)]
pub use red_eye::wire::{
    ActiveBlock as WireActiveBlock, AgentConfigSnapshot as WireAgentConfigSnapshot,
    Filter as WireFilter, Protocol as WireProtocol, ProxyRule as WireProxyRule,
    RawPacket as WireRawPacket, SettingsEnvelope,
};
// Snapshot-test-only shapes (validation helpers build wire values).
#[cfg(test)]
pub use red_eye::wire::{AgentNetworkSettings as WireAgentNetworkSettings, Service as WireService};
