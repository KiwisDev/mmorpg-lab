use bevy::prelude::*;
use bevy::log::LogPlugin;
use bevy::app::ScheduleRunnerPlugin;
use std::time::Duration;
use game_sockets::{GamePeer, GameNetworkEvent, GameStreamReliability, GameConnection, GameStream};
use game_sockets::protocols::QuicBackend;
use crate::resources::{ServerConfig, PlayerRegistry, HeartbeatTimer, Orchestrator, SpatialService, HandoffManager};
use shared::{Heartbeat, EntityState, HandoffRequest, HandoffAccept, HandoffReject, HandoffComplete, GhostUpdate};
use bytes::Bytes;

mod resources;

#[derive(Resource)]
struct GamePeerResource(GamePeer);

fn main() {
    println!("[INFO] Dedicated Game Server starting...");
    App::new()
        .add_plugins(
            MinimalPlugins.set(ScheduleRunnerPlugin::run_loop(Duration::from_secs_f64(
                1.0 / 60.0, // 60 "ticks" per second
            ))),
        )
        .add_plugins(LogPlugin::default())
        .insert_resource(ServerConfig::from_env())
        .init_resource::<PlayerRegistry>()
        .init_resource::<Orchestrator>()
        .init_resource::<SpatialService>()
        .init_resource::<HandoffManager>()
        .init_resource::<HeartbeatTimer>()
        .add_systems(Startup, (setup_networking, connect_to_orchestrator, connect_to_spatial_service).chain())
        .add_systems(Update, (
            handle_network_events,
            send_heartbeat,
            process_handoff_timeouts,
        ).chain())
        .run();
}

fn setup_networking(mut commands: Commands, config: Res<ServerConfig>) {
    let peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = peer.listen("0.0.0.0", config.port) {
        error!("[ERROR] Failed to bind Quic: {}", e);
    } else {
        info!("[INFO] Listening on: 0.0.0.0:{}", config.port);
    }
    commands.insert_resource(GamePeerResource(peer));
}

fn connect_to_orchestrator(peer: ResMut<GamePeerResource>, config: Res<ServerConfig>) {
    info!("[INFO] Connecting to orchestrator at {}", config.orchestrator_addr);
    if let Err(e) = peer.0.connect(&config.orchestrator_addr.ip().to_string(), config.orchestrator_addr.port()) {
        error!("[ERROR] Failed to connect to orchestrator: {}", e);
    }
}

fn connect_to_spatial_service(peer: ResMut<GamePeerResource>) {
    let addr = std::env::var("SPATIAL_ADDR").unwrap_or("127.0.0.1:9001".to_string());
    let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
    let port: u16 = parts[0].parse().unwrap_or(9001);
    let host = parts[1];
    info!("[INFO] Connecting to spatial service at {}", addr);
    if let Err(e) = peer.0.connect(host, port) {
        error!("[ERROR] Failed to connect to spatial service: {}", e);
    }
}

fn handle_network_events(
    mut peer: ResMut<GamePeerResource>,
    mut registry: ResMut<PlayerRegistry>,
    mut orchestrator: ResMut<Orchestrator>,
    mut spatial: ResMut<SpatialService>,
    mut handoff_mgr: ResMut<HandoffManager>,
    config: Res<ServerConfig>,
) {
    while let Ok(Some(event)) = peer.0.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                if orchestrator.connection.is_none() {
                    info!("[INFO] Orchestrator connected: {:?}", conn);
                    orchestrator.connection = Some(conn);
                } else if spatial.connection.is_none() {
                    info!("[INFO] Spatial service connected: {:?}", conn);
                    spatial.connection = Some(conn);
                } else {
                    info!("[INFO] Peer connected: {:?}", conn);
                }
            }
            GameNetworkEvent::Disconnected(conn) => {
                if Some(conn) == orchestrator.connection {
                    orchestrator.connection = None;
                    warn!("[WARN] Orchestrator disconnected.");
                } else if Some(conn) == spatial.connection {
                    spatial.connection = None;
                    warn!("[WARN] Spatial service disconnected.");
                } else {
                    info!("[INFO] Peer disconnected: {:?}", conn);
                    registry.remove_player(&conn);
                }
            }
            GameNetworkEvent::Message { connection, stream, data } => {
                if data.is_empty() { continue; }

                // Binary protocol messages come first.
                match data[0] {
                    0x11 => {
                        handle_crossing_alert(&data, &registry, &mut handoff_mgr);
                        continue;
                    }
                    0x20 => { handle_handoff_request(&data, connection, &mut registry, &mut handoff_mgr, &mut peer.0); continue; }
                    0x21 => { handle_handoff_accept(&data, &mut registry, &mut handoff_mgr); continue; }
                    0x22 => { handle_handoff_reject(&data, &mut registry, &mut handoff_mgr); continue; }
                    0x23 => { handle_ghost_update(&data, &mut handoff_mgr); continue; }
                    0x24 => { handle_handoff_complete(&data, &mut registry, &mut handoff_mgr); continue; }
                    _ => {}
                }

                let message = String::from_utf8_lossy(&data);
                info!("[DEBUG] Received message from {:?} on stream {:?}: {}", connection, stream, message);

                if let Some(username) = message.strip_prefix("JOIN ") {
                    handle_join(username, connection, stream, &mut peer.0, &mut registry, config.max_players);
                } else if message.starts_with("UPDATE_POS ") {
                    let parts: Vec<&str> = message.strip_prefix("UPDATE_POS ").unwrap_or("").split_whitespace().collect();
                    if parts.len() >= 4 {
                        if let (Ok(x), Ok(y), Ok(vx), Ok(vy)) = (
                            parts[0].parse::<f32>(),
                            parts[1].parse::<f32>(),
                            parts[2].parse::<f32>(),
                            parts[3].parse::<f32>(),
                        ) {
                            registry.update_position(&connection, x, y);
                            registry.update_velocity(&connection, vx, vy);
                            send_position_update(&peer.0, &spatial, &registry, &connection, &config, x, y);
                        }
                    }
                }
            }
            GameNetworkEvent::Error { connection, inner } => {
                error!("[ERROR] Network error for {:?}: {}", connection, inner);
            }
            _ => {}
        }
    }
}

fn handle_join(
    username: &str,
    connection: GameConnection,
    stream: GameStream,
    peer: &mut GamePeer,
    registry: &mut PlayerRegistry,
    max_players: usize,
) {
    let username = username.trim().to_string();

    if !registry.is_full(max_players) {
        let player = registry.add_player(connection, username.clone());
        info!("[INFO] Player {} connected with ID: {}", username, player.id);

        let response = format!("WELCOME {}", player.id);
        let _ = peer.send(&connection, &stream, Bytes::from(response));
        info!("[DEBUG] Sent WELCOME response");
    } else {
        warn!("[WARN] Server is full, rejected connection ({})", username);
        let response = "FULL";
        let _ = peer.send(&connection, &stream, Bytes::from(response));
    }
}

fn send_position_update(
    peer: &GamePeer,
    spatial: &SpatialService,
    registry: &PlayerRegistry,
    conn: &GameConnection,
    config: &ServerConfig,
    x: f32,
    y: f32,
) {
    let spatial_conn = match spatial.connection {
        Some(c) => c,
        None => return,
    };
    let player = match registry.players.get(conn) {
        Some(p) => p,
        None => return,
    };

    // Parse the player's UUID string to raw bytes for the binary protocol.
    let uuid: uuid::Uuid = match player.id.parse() {
        Ok(u) => u,
        Err(_) => return,
    };

    // TAG(1) + shard_id(4) + client_uuid(16) + x(4) + y(4)
    let mut buf = Vec::with_capacity(29);
    buf.push(0x10u8);
    buf.extend_from_slice(&config.shard_id.to_le_bytes());
    buf.extend_from_slice(uuid.as_bytes());
    buf.extend_from_slice(&x.to_le_bytes());
    buf.extend_from_slice(&y.to_le_bytes());

    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    let _ = peer.send(&spatial_conn, &stream, Bytes::from(buf));
}

fn handle_crossing_alert(
    data: &[u8],
    registry: &PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
) {
    // TAG(1) + client_uuid(16) + dest_shard_id(4) = 21 bytes
    if data.len() < 21 { return; }

    let client_uuid = &data[1..17];
    let dest_shard_id = u32::from_le_bytes(data[17..21].try_into().unwrap());

    // Find the player with this UUID.
    let uuid_str = uuid::Uuid::from_bytes(client_uuid.try_into().unwrap()).to_string();
    let player = match registry.get_player_by_id(&uuid_str) {
        Some(p) => p,
        None => return,
    };

    // Skip if a handoff is already in progress for this entity.
    if handoff_mgr.is_handoff_in_progress(&player.id) { return; }

    info!("[INFO] CrossingAlert: entity {} should hand off to shard {}", player.id, dest_shard_id);
    // Actual HandoffRequest to the destination shard is Part 3 (inter-shard connections).
}

fn send_heartbeat(
    time: Res<Time>,
    mut timer: ResMut<HeartbeatTimer>,
    peer: Res<GamePeerResource>,
    config: Res<ServerConfig>,
    registry: Res<PlayerRegistry>,
    orchestrator: Res<Orchestrator>,
) {
    if timer.0.tick(time.delta()).just_finished() {
        if let Some(conn) = &orchestrator.connection {
            let heartbeat = Heartbeat {
                id: config.id.clone(),
                ip: "127.0.0.1".to_string(),
                port: config.port,
                zone: config.zone.clone(),
                player_count: registry.get_player_count(),
                max_players: config.max_players,
            };

            if let Ok(json) = serde_json::to_string(&heartbeat) {
                let stream = GameStream::new(0, GameStreamReliability::Unreliable);
                match peer.0.send(conn, &stream, Bytes::from(json)) {
                    Ok(_) => {
                        let status = if registry.is_full(config.max_players) { "full" } else { "available" };
                        info!("[INFO] Heartbeat sent - Players: {}/{}, Status: {}",
                                 heartbeat.player_count, heartbeat.max_players, status);
                    }
                    Err(e) => {
                        warn!("[WARN] Failed to send heartbeat: {}", e);
                    }
                }
            }
        } else {
            warn!("[WARN] No orchestrator connection to send heartbeat.");
        }
    }
}

fn send_handoff_accept(peer: &mut GamePeer, target_shard_conn: &GameConnection, entity_id: u32) {
    let response = HandoffAccept { entity_id };
    if let Ok(json) = serde_json::to_string(&response) {
        let mut msg = vec![0x21]; // HandoffAccept tag
        msg.extend_from_slice(json.as_bytes());
        let stream = GameStream::new(1, GameStreamReliability::Reliable);
        let _ = peer.send(target_shard_conn, &stream, Bytes::from(msg));
        info!("[INFO] HandoffAccept sent for entity {}", entity_id);
    }
}

fn send_handoff_reject(peer: &mut GamePeer, target_shard_conn: &GameConnection, entity_id: u32) {
    let response = HandoffReject { entity_id };
    if let Ok(json) = serde_json::to_string(&response) {
        let mut msg = vec![0x22]; // HandoffReject tag
        msg.extend_from_slice(json.as_bytes());
        let stream = GameStream::new(1, GameStreamReliability::Reliable);
        let _ = peer.send(target_shard_conn, &stream, Bytes::from(msg));
        info!("[INFO] HandoffReject sent for entity {}", entity_id);
    }
}

fn send_ghost_update(peer: &mut GamePeer, target_shard_conn: &GameConnection, entity_id: u32, pos_x: f32, pos_y: f32, vel_x: f32, vel_y: f32) {
    let update = GhostUpdate {
        entity_id,
        pos_x,
        pos_y,
        vel_x,
        vel_y,
    };
    if let Ok(json) = serde_json::to_string(&update) {
        let mut msg = vec![0x23]; // GhostUpdate tag
        msg.extend_from_slice(json.as_bytes());
        let stream = GameStream::new(1, GameStreamReliability::Unreliable);
        let _ = peer.send(target_shard_conn, &stream, Bytes::from(msg));
    }
}

fn send_handoff_complete(peer: &mut GamePeer, target_shard_conn: &GameConnection, entity_id: u32) {
    let response = HandoffComplete { entity_id };
    if let Ok(json) = serde_json::to_string(&response) {
        let mut msg = vec![0x24]; // HandoffComplete tag
        msg.extend_from_slice(json.as_bytes());
        let stream = GameStream::new(1, GameStreamReliability::Reliable);
        let _ = peer.send(target_shard_conn, &stream, Bytes::from(msg));
        info!("[INFO] HandoffComplete sent for entity {}", entity_id);
    }
}

fn handle_handoff_request(
    data: &[u8],
    conn: GameConnection,
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
    peer: &mut GamePeer,
) {
    if data.len() <= 1 {
        return;
    }

    if let Ok(req) = serde_json::from_slice::<HandoffRequest>(&data[1..]) {
        info!("[INFO] HandoffRequest received for entity {} at ({}, {})",
              req.entity_id, req.pos_x, req.pos_y);

        // Créer une copie Ghost localement
        let ghost_player = resources::PlayerInfo {
            id: req.entity_id.to_string(),
            username: format!("ghost_{}", req.entity_id),
            pos_x: req.pos_x,
            pos_y: req.pos_y,
            vel_x: req.vel_x,
            vel_y: req.vel_y,
            state: EntityState::Ghost,
            owner_shard_id: None,
            conn: Some(conn),
        };

        handoff_mgr.add_ghost(req.entity_id.to_string(), ghost_player);

        // Accepter le handoff
        send_handoff_accept(peer, &conn, req.entity_id);
    }
}

fn handle_handoff_accept(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
) {
    if data.len() <= 1 {
        return;
    }

    if let Ok(msg) = serde_json::from_slice::<HandoffAccept>(&data[1..]) {
        info!("[INFO] HandoffAccept received for entity {}", msg.entity_id);

        // L'entité est maintenant acceptée au shard destination
        // Elle reste en état PendingHandoff localement jusqu'à HandoffComplete
    }
}

fn handle_handoff_reject(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
) {
    if data.len() <= 1 {
        return;
    }

    if let Ok(msg) = serde_json::from_slice::<HandoffReject>(&data[1..]) {
        info!("[INFO] HandoffReject received for entity {}", msg.entity_id);

        // Mettre à jour l'état de l'entité si elle existe localement
        // TODO: implémenter la logique de rejet (rebond sur la frontière)
    }
}

fn handle_ghost_update(
    data: &[u8],
    handoff_mgr: &mut HandoffManager,
) {
    if data.len() <= 1 {
        return;
    }

    if let Ok(msg) = serde_json::from_slice::<GhostUpdate>(&data[1..]) {
        info!("[INFO] GhostUpdate received for entity {}: ({}, {})",
              msg.entity_id, msg.pos_x, msg.pos_y);

        // Mettre à jour la position du ghost
        if let Some(ghost) = handoff_mgr.ghost_entities.get_mut(&msg.entity_id.to_string()) {
            ghost.pos_x = msg.pos_x;
            ghost.pos_y = msg.pos_y;
            ghost.vel_x = msg.vel_x;
            ghost.vel_y = msg.vel_y;
        }
    }
}

fn handle_handoff_complete(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
) {
    if data.len() <= 1 {
        return;
    }

    if let Ok(msg) = serde_json::from_slice::<HandoffComplete>(&data[1..]) {
        info!("[INFO] HandoffComplete received for entity {}", msg.entity_id);

        // Supprimer le ghost local et terminer le handoff
        handoff_mgr.remove_ghost(&msg.entity_id.to_string());
        handoff_mgr.complete_handoff(&msg.entity_id.to_string());
    }
}

fn process_handoff_timeouts(
    time: Res<Time>,
    mut handoff_mgr: ResMut<HandoffManager>,
) {
    let mut expired = Vec::new();
    for (entity_id, handoff) in handoff_mgr.pending.iter_mut() {
        handoff.timer.tick(time.delta());
        if handoff.timer.finished() {
            expired.push(entity_id.clone());
        }
    }

    for entity_id in expired {
        handoff_mgr.complete_handoff(&entity_id);
        warn!("[WARN] Handoff timeout for entity {}", entity_id);
    }
}
