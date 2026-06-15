use bevy::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;
use game_sockets::GameConnection;
use shared::EntityState;

/// Information sur un joueur connecté
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub id: String,
    pub username: String,
    /// Position 2D du joueur
    pub pos_x: f32,
    pub pos_y: f32,
    /// Vélocité du joueur
    pub vel_x: f32,
    pub vel_y: f32,
    /// État de l'entité (Owned, PendingHandoff, Ghost)
    pub state: EntityState,
    /// ID du shard propriétaire (si Ghost, c'est le shard distant qui own)
    pub owner_shard_id: Option<u32>,
    #[serde(skip)]
    pub conn: Option<GameConnection>,
}

/// Configuration du serveur de jeu dédié
#[derive(Resource, Debug, Clone)]
pub struct ServerConfig {
    pub id: String,
    pub shard_id: u32,
    pub port: u16,
    pub zone: String,
    pub max_players: usize,
    pub orchestrator_addr: SocketAddr,
}

impl ServerConfig {
    /// Charge la configuration depuis les variables d'environnement
    pub fn from_env() -> Self {
        let port = std::env::var("DS_PORT")
            .unwrap_or_else(|_| "9000".to_string())
            .parse::<u16>()
            .expect("DS_PORT doit être un numéro de port valide");

        let zone = std::env::var("DS_ZONE").unwrap_or_else(|_| "zone_A".to_string());

        let max_players = std::env::var("DS_MAX_PLAYERS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<usize>()
            .expect("DS_MAX_PLAYERS doit être un nombre valide");

        let orchestrator_addr = std::env::var("ORCHESTRATOR_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9000".to_string())
            .parse::<SocketAddr>()
            .expect("ORCHESTRATOR_ADDR doit être une adresse valide");

        let shard_id = std::env::var("DS_SHARD_ID")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<u32>()
            .unwrap_or(0);

        Self {
            id: Uuid::new_v4().to_string(),
            shard_id,
            port,
            zone,
            max_players,
            orchestrator_addr,
        }
    }
}

/// Registre des joueurs connectés
#[derive(Resource, Default, Debug)]
pub struct PlayerRegistry {
    pub players: HashMap<GameConnection, PlayerInfo>,
}

impl PlayerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_player(&mut self, conn: GameConnection, username: String) -> PlayerInfo {
        let player = PlayerInfo {
            id: Uuid::new_v4().to_string(),
            username,
            pos_x: 0.0,
            pos_y: 0.0,
            vel_x: 0.0,
            vel_y: 0.0,
            state: EntityState::Owned,
            owner_shard_id: None,
            conn: Some(conn),
        };
        self.players.insert(conn, player.clone());
        player
    }

    pub fn remove_player(&mut self, conn: &GameConnection) -> Option<PlayerInfo> {
        self.players.remove(conn)
    }

    pub fn get_player_count(&self) -> usize {
        self.players.len()
    }

    pub fn is_full(&self, max_players: usize) -> bool {
        self.players.len() >= max_players
    }

    pub fn update_position(&mut self, conn: &GameConnection, x: f32, y: f32) {
        if let Some(player) = self.players.get_mut(conn) {
            player.pos_x = x;
            player.pos_y = y;
        }
    }

    pub fn update_velocity(&mut self, conn: &GameConnection, vx: f32, vy: f32) {
        if let Some(player) = self.players.get_mut(conn) {
            player.vel_x = vx;
            player.vel_y = vy;
        }
    }

    pub fn set_entity_state(&mut self, conn: &GameConnection, state: EntityState) {
        if let Some(player) = self.players.get_mut(conn) {
            player.state = state;
        }
    }

    pub fn get_player_by_id(&self, entity_id: &str) -> Option<&PlayerInfo> {
        self.players.values().find(|p| p.id == entity_id)
    }

    pub fn get_player_mut_by_id(&mut self, entity_id: &str) -> Option<&mut PlayerInfo> {
        self.players.values_mut().find(|p| p.id == entity_id)
    }
}

#[derive(Resource, Default)]
pub struct Orchestrator {
    pub connection: Option<GameConnection>,
}

#[derive(Resource, Default)]
pub struct SpatialService {
    pub connection: Option<GameConnection>,
}

#[derive(Resource)]
pub struct HeartbeatTimer(pub Timer);

impl Default for HeartbeatTimer {
    fn default() -> Self {
        HeartbeatTimer(Timer::from_seconds(5.0, TimerMode::Repeating))
    }
}

/// Suivi d'un transfert d'autorité en cours
#[derive(Debug, Clone)]
pub struct HandoffInProgress {
    pub entity_id: String,
    pub source_shard_id: u32,
    pub dest_shard_id: u32,
    pub entity_state: Vec<u8>,
    pub timer: Timer, // Timeout pour le handoff
}

/// Gère les transferts d'autorité en cours
#[derive(Resource, Default)]
pub struct HandoffManager {
    pub pending: HashMap<String, HandoffInProgress>, // entity_id -> handoff info
    pub ghost_entities: HashMap<String, PlayerInfo>,  // entity_id -> ghost copy
    pub shard_connections: HashMap<u32, GameConnection>, // shard_id -> connection
}

impl HandoffManager {
    pub fn is_handoff_in_progress(&self, entity_id: &str) -> bool {
        self.pending.contains_key(entity_id)
    }

    pub fn start_handoff(
        &mut self,
        entity_id: String,
        source_shard_id: u32,
        dest_shard_id: u32,
        entity_state: Vec<u8>,
    ) {
        let handoff = HandoffInProgress {
            entity_id: entity_id.clone(),
            source_shard_id,
            dest_shard_id,
            entity_state,
            timer: Timer::from_seconds(5.0, TimerMode::Once),
        };
        self.pending.insert(entity_id, handoff);
    }

    pub fn complete_handoff(&mut self, entity_id: &str) -> Option<HandoffInProgress> {
        self.pending.remove(entity_id)
    }

    pub fn add_ghost(&mut self, entity_id: String, player_info: PlayerInfo) {
        self.ghost_entities.insert(entity_id, player_info);
    }

    pub fn remove_ghost(&mut self, entity_id: &str) -> Option<PlayerInfo> {
        self.ghost_entities.remove(entity_id)
    }

    pub fn register_shard(&mut self, shard_id: u32, conn: GameConnection) {
        self.shard_connections.insert(shard_id, conn);
    }

    pub fn get_shard_connection(&self, shard_id: u32) -> Option<&GameConnection> {
        self.shard_connections.get(&shard_id)
    }
}

