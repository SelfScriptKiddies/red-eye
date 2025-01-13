use crate::configuration::init_config;

mod configuration;

fn main() {
    let config = init_config();
    println!("{config:?}");
}
