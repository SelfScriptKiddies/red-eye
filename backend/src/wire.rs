pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::{
    ConfigSync as WireConfigSync, Filter as WireFilter, FilterDirection as WireFilterDirection,
    FilterOp as WireFilterOp, Service as WireService, ServiceOp as WireServiceOp,
    SettingsEnvelope,
};
pub use red_eye::wire::settings_envelope;
