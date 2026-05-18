use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String
}