pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::settings_envelope;
pub use red_eye::wire::{
    AgentConfigSnapshot as WireAgentConfigSnapshot,
    AgentNetworkSettings as WireAgentNetworkSettings, ConfigSync as WireConfigSync,
    Filter as WireFilter, FilterDirection as WireFilterDirection, FilterOp as WireFilterOp,
    Protocol as WireProtocol, ProxyRule as WireProxyRule, Service as WireService,
    ServiceOp as WireServiceOp, SettingsEnvelope,
};
