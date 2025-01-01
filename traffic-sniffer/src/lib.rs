mod configuration;
mod packet_parser;

fn init() {
    let mut clog = colog::default_builder();
    clog.filter(None, log::LevelFilter::Trace);
    clog.init();
}

pub fn run() {
    init();
    let config = configuration::init_config();
    println!("{config:?}");


}