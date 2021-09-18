use std::net::{SocketAddr, AddrParseError};
use std::str::FromStr;
use serde::{Serialize,Deserialize};
pub fn parse_addr(addr: &str) -> std::result::Result<SocketAddr, AddrParseError> {
    SocketAddr::from_str(addr)
}

///message between client and server
#[derive(Serialize,Deserialize,Debug)]
pub enum Command{
    Set(String,String),
    Get(String),
    Remove(String),
    Ping,
    Pong,
    Ok(Option<String>),
    Err
}

