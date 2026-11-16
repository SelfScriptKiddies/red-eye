//! Wire-format types generated from the top-level `proto/wire.proto`
//! (symlinked into this crate as `./proto`, compiled by `build.rs`).

pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::settings_envelope;
pub use red_eye::wire::{
    AgentConfigSnapshot as WireAgentConfigSnapshot,
    AgentNetworkSettings as WireAgentNetworkSettings, Service as WireService, SettingsEnvelope,
};
pub use red_eye::wire::{PacketBatch, Protocol as WireProtocol, RawPacket as WireRawPacket};
