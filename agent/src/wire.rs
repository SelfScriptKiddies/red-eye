//! Wire-format types generated from the top-level `proto/wire.proto`
//! (symlinked into this crate as `./proto`, compiled by `build.rs`).

pub mod red_eye {
    pub mod wire {
        include!(concat!(env!("OUT_DIR"), "/red_eye.wire.rs"));
    }
}

pub use red_eye::wire::{PacketBatch, RawPacket as WireRawPacket};
pub use red_eye::wire::{Service as WireService, SettingsEnvelope};
pub use red_eye::wire::settings_envelope;
