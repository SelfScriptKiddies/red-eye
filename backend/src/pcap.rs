use serde::Serialize;

#[derive(Serialize, Clone)]
struct PacketInfo {
    protocol: String,
    src_ip: String,
    src_port: u16,
    dst_ip: String,
    dst_port: u16,
    length: usize,
    data: Vec<u8>,
}

fn process_packet() {

}