pub mod api;
pub mod decision;
pub mod openrouter;
pub mod repository;
pub mod scheduler;
pub mod settings;
pub mod transcript;

pub use settings::validate_setting;

pub fn config(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.configure(api::config);
}
