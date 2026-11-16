pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::settings_envelope;
pub use red_eye::wire::{
    ActiveBlock as WireActiveBlock, AgentConfigSnapshot as WireAgentConfigSnapshot,
    AgentNetworkSettings as WireAgentNetworkSettings, ConfigSync as WireConfigSync,
    Filter as WireFilter, FilterDirection as WireFilterDirection, FilterOp as WireFilterOp,
    IpEndpoint as WireIpEndpoint, Protocol as WireProtocol, Service as WireService,
    ServiceOp as WireServiceOp, SettingsEnvelope, Threshold as WireThreshold,
};
