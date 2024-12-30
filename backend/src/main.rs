use clap::Parser;

mod pcap;
mod api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    interface: String,

    #[arg(short, long)]
    ip: String,

    #[arg(short, long)]
    port: u16,

    #[arg(short, long, default_value_t = 100)]
    max_packets: usize,
}

fn main() {
    println!("Hello, world!");
}
