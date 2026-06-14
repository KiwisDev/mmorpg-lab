use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityState {
    Owned,           
    PendingHandoff,  
    Ghost,           
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HandoffRequest {
    pub entity_id: u32,
    pub pos_x: f32,
    pub pos_y: f32,
    pub vel_x: f32,
    pub vel_y: f32,
    pub state: Vec<u8>, 
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HandoffAccept {
    pub entity_id: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HandoffReject {
    pub entity_id: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GhostUpdate {
    pub entity_id: u32,
    pub pos_x: f32,
    pub pos_y: f32,
    pub vel_x: f32,
    pub vel_y: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HandoffComplete {
    pub entity_id: u32,
}

