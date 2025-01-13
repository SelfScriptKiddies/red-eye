use config_manager::{config, ConfigInit};

const VERSION: &str = "0.1.1";
const AUTHOR: &str = "TonySD";

#[derive(Debug)]
#[config(
    clap(version, author),
    file(
        format = "toml",
        clap(long = "config", short = 'c', help = "path to configuration file"),
        env = "demo_config",
        default = "./config.toml"
    )
)]
pub struct ApplicationConfig {
    #[source(
        clap(long, short = 'p'),
        env = "MODEL_PATH",
        config = "general.model_path",
        default = "\"руруру\".to_string()")
    ]
    model_path: String,
    #[source(env, config, default = 0)]
    prediction_delay: u64,
}