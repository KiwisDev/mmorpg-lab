use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;
use game_sockets::GameConnection;

/// Information sur un joueur connecté
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub id: String,
    pub username: String,
    #[serde(skip)]
    pub conn: Option<GameConnection>,
}

/// Configuration du serveur de jeu dédié
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub id: String,
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

        Self {
            id: Uuid::new_v4().to_string(),
            port,
            zone,
            max_players,
            orchestrator_addr,
        }
    }
}

/// Registre des joueurs connectés
#[derive(Default, Debug)]
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
}