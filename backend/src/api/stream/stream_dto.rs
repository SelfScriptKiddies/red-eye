use std::net::IpAddr;
use serde::{Deserialize, Serialize};
use strum_macros::IntoStaticStr;

#[derive(Debug, Serialize, Deserialize, IntoStaticStr)]
pub enum Protocol {
    TCP,
    UDP
}
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Stream {
    pub id: i32,
    pub src_ip: IpAddr,
    pub src_port: i32,
    pub dst_ip: IpAddr,
    pub dst_port: i32,
    pub protocol: Protocol,
    pub active: bool,
    pub packet_count: i32
}