use bevy::prelude::*;
use bevy::log::LogPlugin;
use bevy::app::ScheduleRunnerPlugin;
use std::time::Duration;
use game_sockets::{GamePeer, GameNetworkEvent, GameStreamReliability, GameConnection, GameStream};
use game_sockets::protocols::QuicBackend;
use crate::resources::{ServerConfig, PlayerRegistry, HeartbeatTimer, GameStateTimer, SpatialUpdateTimer, FarPublishTimer, Orchestrator, SpatialService, BrokerConnection, PendingConnections, PendingConnType, HandoffManager};
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
        .init_resource::<BrokerConnection>()
        .init_resource::<PendingConnections>()
        .init_resource::<HandoffManager>()
        .init_resource::<HeartbeatTimer>()
        .init_resource::<GameStateTimer>()
        .init_resource::<SpatialUpdateTimer>()
        .init_resource::<FarPublishTimer>()
        .add_systems(Startup, (setup_networking, connect_to_orchestrator, connect_to_spatial_service, connect_to_broker).chain())
        .add_systems(Update, (
            handle_network_events,
            update_positions,
            send_heartbeat,
            send_game_state,
            send_game_state_far,
            send_spatial_updates,
            drive_handoffs,
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

fn connect_to_orchestrator(peer: ResMut<GamePeerResource>, config: Res<ServerConfig>, mut pending: ResMut<PendingConnections>) {
    info!("[INFO] Connecting to orchestrator at {}", config.orchestrator_addr);
    if peer.0.connect(&config.orchestrator_addr.ip().to_string(), config.orchestrator_addr.port()).is_ok() {
        pending.0.push_back(PendingConnType::Orchestrator);
    }
}

fn connect_to_spatial_service(peer: ResMut<GamePeerResource>, mut pending: ResMut<PendingConnections>) {
    let addr = std::env::var("SPATIAL_ADDR").unwrap_or("127.0.0.1:9001".to_string());
    let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
    let port: u16 = parts[0].parse().unwrap_or(9001);
    let host = parts[1];
    info!("[INFO] Connecting to spatial service at {}", addr);
    if peer.0.connect(host, port).is_ok() {
        pending.0.push_back(PendingConnType::Spatial);
    }
}

fn connect_to_broker(peer: ResMut<GamePeerResource>, mut pending: ResMut<PendingConnections>) {
    let addr = std::env::var("BROKER_ADDR").unwrap_or("127.0.0.1:9010".to_string());
    let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
    let port: u16 = parts[0].parse().unwrap_or(9010);
    let host = parts[1];
    info!("[INFO] Connecting to broker at {}", addr);
    if peer.0.connect(host, port).is_ok() {
        pending.0.push_back(PendingConnType::Broker);
    }
}

fn handle_network_events(
    mut peer: ResMut<GamePeerResource>,
    mut registry: ResMut<PlayerRegistry>,
    mut orchestrator: ResMut<Orchestrator>,
    mut spatial: ResMut<SpatialService>,
    mut broker: ResMut<BrokerConnection>,
    mut pending: ResMut<PendingConnections>,
    mut handoff_mgr: ResMut<HandoffManager>,
    config: Res<ServerConfig>,
) {
    while let Ok(Some(event)) = peer.0.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                match pending.0.pop_front() {
                    Some(PendingConnType::Orchestrator) => {
                        info!("[INFO] Orchestrator connected");
                        orchestrator.connection = Some(conn);
                    }
                    Some(PendingConnType::Spatial) => {
                        info!("[INFO] Spatial service connected");
                        spatial.connection = Some(conn);
                    }
                    Some(PendingConnType::Broker) => {
                        info!("[INFO] Broker connected");
                        broker.connection = Some(conn);
                    }
                    None => {
                        // Incoming connection from a player (TP1 direct mode)
                        info!("[INFO] Player connected: {:?}", conn);
                    }
                }
            }
            GameNetworkEvent::Disconnected(conn) => {
                if Some(conn) == orchestrator.connection {
                    orchestrator.connection = None;
                    warn!("[WARN] Orchestrator disconnected.");
                } else if Some(conn) == spatial.connection {
                    spatial.connection = None;
                    warn!("[WARN] Spatial service disconnected.");
                } else if Some(conn) == broker.connection {
                    broker.connection = None;
                    warn!("[WARN] Broker disconnected.");
                } else {
                    info!("[INFO] Peer disconnected: {:?}", conn);
                    registry.remove_player(&conn);
                }
            }
            GameNetworkEvent::Message { connection, stream, data } => {
                if data.is_empty() { continue; }

                match data[0] {
                    // ClientInput forwarded by broker: TAG(1) + client_uuid(16) + input(16)
                    0x05 => {
                        if data.len() >= 33 {
                            let uuid: [u8; 16] = data[1..17].try_into().unwrap();
                            let input = &data[17..33];
                            handle_client_input(uuid, input, &mut registry, &config);
                        }
                        continue;
                    }
                    0x11 => { handle_crossing_alert(&data, &mut registry, &mut handoff_mgr, &peer.0, &broker, &config); continue; }
                    0x20 => { handle_handoff_request(&data, &mut handoff_mgr, &peer.0, &broker); continue; }
                    0x21 => { handle_handoff_accept(&data, &mut handoff_mgr); continue; }
                    0x22 => { handle_handoff_reject(&data, &mut handoff_mgr); continue; }
                    0x23 => { handle_ghost_update(&data, &mut registry, &mut handoff_mgr, &config); continue; }
                    0x24 => { handle_handoff_complete(&data, &mut registry, &mut handoff_mgr); continue; }
                    _ => {}
                }

                // Legacy text protocol (TP1 direct connections)
                let message = String::from_utf8_lossy(&data);
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

// World layout: 1000x1000 split into a 2x2 grid (shards 0-3).
//   0: top-left   1: top-right   2: bottom-left   3: bottom-right
// Which shard owns a position, mirroring the spatial service's quad tree.
// Lets a shard decide locally when an entity has actually crossed its border.
fn shard_for_2x2(x: f32, y: f32) -> Option<u32> {
    if !(0.0..1000.0).contains(&x) || !(0.0..1000.0).contains(&y) {
        return None;
    }
    let col = if x >= 500.0 { 1 } else { 0 };
    let row = if y >= 500.0 { 1 } else { 0 };
    Some(row * 2 + col)
}

fn shard_topic(shard_id: u32) -> [u8; 32] {
    topic_from_label(&format!("shard:{}", shard_id))
}

fn shard_topic_far(shard_id: u32) -> [u8; 32] {
    topic_from_label(&format!("shard:{}:far", shard_id))
}

fn topic_from_label(label: &str) -> [u8; 32] {
    let mut topic = [0u8; 32];
    let bytes = label.as_bytes();
    let len = bytes.len().min(32);
    topic[..len].copy_from_slice(&bytes[..len]);
    topic
}

// Send an inter-shard message to `dest_shard` via the broker (tag 0x06).
// `inner` already starts with the handoff tag (0x20-0x24).
fn route_to_shard(peer: &GamePeer, broker_conn: &GameConnection, dest_shard: u32, inner: &[u8]) {
    let mut buf = Vec::with_capacity(1 + 32 + inner.len());
    buf.push(0x06u8); // TAG_ROUTE_TO_SHARD
    buf.extend_from_slice(&shard_topic(dest_shard));
    buf.extend_from_slice(inner);
    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    let _ = peer.send(broker_conn, &stream, Bytes::from(buf));
}

fn handle_client_input(
    uuid: [u8; 16],
    input: &[u8],
    registry: &mut PlayerRegistry,
    config: &ServerConfig,
) {
    if input.is_empty() { return; }
    match input[0] {
        0x01 => {
            // JOIN: input[1..5] = spawn x, input[5..9] = spawn y.
            // Only the shard owning the spawn position creates the player, so a
            // JOIN relayed to several AOI shards never produces duplicates.
            if input.len() < 9 { return; }
            let sx = f32::from_le_bytes(input[1..5].try_into().unwrap());
            let sy = f32::from_le_bytes(input[5..9].try_into().unwrap());
            if shard_for_2x2(sx, sy) != Some(config.shard_id) { return; }
            if !registry.is_full(config.max_players) {
                let player = registry.add_player_with_uuid(uuid, "player".to_string(), sx, sy);
                info!("[INFO] Player joined via broker: {}", player.id);
            }
        }
        0x02 => {
            // MOVE: input[1..5] = vx f32, input[5..9] = vy f32
            if input.len() >= 9 {
                let vx = f32::from_le_bytes(input[1..5].try_into().unwrap());
                let vy = f32::from_le_bytes(input[5..9].try_into().unwrap());
                registry.update_velocity_by_uuid(uuid, vx, vy);
            }
        }
        0x03 => {
            // LEAVE: broker reports the client disconnected
            if registry.remove_player_by_uuid(uuid).is_some() {
                info!("[INFO] Player left: {}", uuid::Uuid::from_bytes(uuid));
            }
        }
        _ => {}
    }
}

fn update_positions(time: Res<Time>, mut registry: ResMut<PlayerRegistry>) {
    let dt = time.delta_seconds();
    for player in registry.players.values_mut() {
        player.pos_x += player.vel_x * dt;
        player.pos_y += player.vel_y * dt;
    }
}

// Game state payload: count(2) + per entity uuid(16) + x(4) + y(4).
// Includes owned/pending players AND local ghosts, so the client sees a
// continuous position during a handoff (both shards publish the entity).
fn build_state_payload(registry: &PlayerRegistry, handoff_mgr: &HandoffManager) -> Vec<u8> {
    let entities = registry.players.values().chain(handoff_mgr.ghost_entities.values());
    let mut count: u16 = 0;
    let mut payload = Vec::new();
    payload.extend_from_slice(&[0u8, 0u8]); // placeholder for count
    for player in entities {
        if let Ok(uuid) = player.id.parse::<uuid::Uuid>() {
            payload.extend_from_slice(uuid.as_bytes());
            payload.extend_from_slice(&player.pos_x.to_le_bytes());
            payload.extend_from_slice(&player.pos_y.to_le_bytes());
            count += 1;
        }
    }
    payload[0..2].copy_from_slice(&count.to_le_bytes());
    payload
}

fn publish_state(peer: &GamePeer, broker_conn: &GameConnection, topic: &[u8; 32], payload: &[u8]) {
    let mut msg = Vec::with_capacity(1 + 32 + 2 + payload.len());
    msg.push(0x03u8); // TAG_PUBLISH
    msg.extend_from_slice(topic);
    msg.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    msg.extend_from_slice(payload);
    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    let _ = peer.send(broker_conn, &stream, Bytes::from(msg));
}

// Full-rate publish (20Hz) on topic "shard:N" — for clients with this shard in their near AOI.
fn send_game_state(
    time: Res<Time>,
    mut timer: ResMut<GameStateTimer>,
    peer: Res<GamePeerResource>,
    registry: Res<PlayerRegistry>,
    handoff_mgr: Res<HandoffManager>,
    config: Res<ServerConfig>,
    broker: Res<BrokerConnection>,
) {
    if !timer.0.tick(time.delta()).just_finished() { return; }
    let broker_conn = match broker.connection { Some(c) => c, None => return };
    let payload = build_state_payload(&registry, &handoff_mgr);
    publish_state(&peer.0, &broker_conn, &shard_topic(config.shard_id), &payload);
}

// Low-rate publish (5Hz) on topic "shard:N:far" — for clients with this shard in their far AOI.
fn send_game_state_far(
    time: Res<Time>,
    mut timer: ResMut<FarPublishTimer>,
    peer: Res<GamePeerResource>,
    registry: Res<PlayerRegistry>,
    handoff_mgr: Res<HandoffManager>,
    config: Res<ServerConfig>,
    broker: Res<BrokerConnection>,
) {
    if !timer.0.tick(time.delta()).just_finished() { return; }
    let broker_conn = match broker.connection { Some(c) => c, None => return };
    let payload = build_state_payload(&registry, &handoff_mgr);
    publish_state(&peer.0, &broker_conn, &shard_topic_far(config.shard_id), &payload);
}

// Report each owned entity's position to the spatial service, which decides
// subscription changes and crossing alerts. This is the TP2 spatial pipeline.
fn send_spatial_updates(
    time: Res<Time>,
    mut timer: ResMut<SpatialUpdateTimer>,
    peer: Res<GamePeerResource>,
    registry: Res<PlayerRegistry>,
    config: Res<ServerConfig>,
    spatial: Res<SpatialService>,
) {
    if !timer.0.tick(time.delta()).just_finished() { return; }
    let spatial_conn = match spatial.connection {
        Some(c) => c,
        None => return,
    };

    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    for player in registry.players.values() {
        if player.state != EntityState::Owned { continue; }
        let uuid: uuid::Uuid = match player.id.parse() { Ok(u) => u, Err(_) => continue };

        // TAG(1) + shard_id(4) + client_uuid(16) + x(4) + y(4)
        let mut buf = Vec::with_capacity(29);
        buf.push(0x10u8);
        buf.extend_from_slice(&config.shard_id.to_le_bytes());
        buf.extend_from_slice(uuid.as_bytes());
        buf.extend_from_slice(&player.pos_x.to_le_bytes());
        buf.extend_from_slice(&player.pos_y.to_le_bytes());
        let _ = peer.0.send(&spatial_conn, &stream, Bytes::from(buf));
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

// Source shard: an owned entity is nearing a border → start its handoff.
fn handle_crossing_alert(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
    peer: &GamePeer,
    broker: &BrokerConnection,
    config: &ServerConfig,
) {
    // TAG(1) + client_uuid(16) + dest_shard_id(4) = 21 bytes
    if data.len() < 21 { return; }

    let uuid: [u8; 16] = data[1..17].try_into().unwrap();
    let dest_shard_id = u32::from_le_bytes(data[17..21].try_into().unwrap());

    let uuid_str = uuid::Uuid::from_bytes(uuid).to_string();
    let player = match registry.get_player_by_id(&uuid_str) {
        Some(p) => p.clone(),
        None => return,
    };

    // Only the owning shard starts a handoff, and only once per entity.
    if player.state != EntityState::Owned { return; }
    if handoff_mgr.is_handoff_in_progress(&player.id) { return; }

    let broker_conn = match broker.connection {
        Some(c) => c,
        None => return,
    };

    info!("[INFO] CrossingAlert: entity {} → handoff to shard {}", player.id, dest_shard_id);

    registry.set_state_by_uuid(uuid, EntityState::PendingHandoff);
    handoff_mgr.start_handoff(player.id.clone(), config.shard_id, dest_shard_id);

    // Ask the destination shard to spawn a ghost.
    let req = HandoffRequest {
        entity_id: player.id.clone(),
        from_shard: config.shard_id,
        pos_x: player.pos_x,
        pos_y: player.pos_y,
        vel_x: player.vel_x,
        vel_y: player.vel_y,
    };
    if let Ok(json) = serde_json::to_string(&req) {
        let mut inner = vec![0x20u8]; // HandoffRequest tag
        inner.extend_from_slice(json.as_bytes());
        route_to_shard(peer, &broker_conn, dest_shard_id, &inner);
    }
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
                shard_id: config.shard_id,
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

// Destination shard: spawn a ghost and accept the transfer.
fn handle_handoff_request(
    data: &[u8],
    handoff_mgr: &mut HandoffManager,
    peer: &GamePeer,
    broker: &BrokerConnection,
) {
    if data.len() <= 1 { return; }
    let req = match serde_json::from_slice::<HandoffRequest>(&data[1..]) {
        Ok(r) => r,
        Err(_) => return,
    };
    let broker_conn = match broker.connection { Some(c) => c, None => return };

    info!("[INFO] HandoffRequest for entity {} from shard {} at ({:.0},{:.0})",
          req.entity_id, req.from_shard, req.pos_x, req.pos_y);

    let ghost = resources::PlayerInfo {
        id: req.entity_id.clone(),
        username: "ghost".to_string(),
        pos_x: req.pos_x,
        pos_y: req.pos_y,
        vel_x: req.vel_x,
        vel_y: req.vel_y,
        state: EntityState::Ghost,
        owner_shard_id: Some(req.from_shard),
        conn: None,
    };
    handoff_mgr.add_ghost(req.entity_id.clone(), ghost);

    // Accept, routed back to the source shard.
    let accept = HandoffAccept { entity_id: req.entity_id.clone() };
    if let Ok(json) = serde_json::to_string(&accept) {
        let mut inner = vec![0x21u8]; // HandoffAccept tag
        inner.extend_from_slice(json.as_bytes());
        route_to_shard(peer, &broker_conn, req.from_shard, &inner);
        info!("[INFO] HandoffAccept sent for entity {}", req.entity_id);
    }
}

// Source shard: destination accepted → GhostUpdates may start flowing.
fn handle_handoff_accept(data: &[u8], handoff_mgr: &mut HandoffManager) {
    if data.len() <= 1 { return; }
    if let Ok(msg) = serde_json::from_slice::<HandoffAccept>(&data[1..]) {
        if let Some(h) = handoff_mgr.pending.get_mut(&msg.entity_id) {
            h.accepted = true;
            info!("[INFO] HandoffAccept received for entity {}", msg.entity_id);
        }
    }
}

fn handle_handoff_reject(data: &[u8], handoff_mgr: &mut HandoffManager) {
    if data.len() <= 1 { return; }
    if let Ok(msg) = serde_json::from_slice::<HandoffReject>(&data[1..]) {
        info!("[INFO] HandoffReject received for entity {}", msg.entity_id);
        handoff_mgr.complete_handoff(&msg.entity_id);
    }
}

// Destination shard: refresh the ghost copy, and promote it to Owned once the
// entity enters our zone. Promotion-by-position is robust to dropped datagrams
// since GhostUpdate is repeated every tick (unlike a one-shot HandoffComplete).
fn handle_ghost_update(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
    config: &ServerConfig,
) {
    if data.len() <= 1 { return; }
    let msg = match serde_json::from_slice::<GhostUpdate>(&data[1..]) { Ok(m) => m, Err(_) => return };

    let entered = match handoff_mgr.ghost_entities.get_mut(&msg.entity_id) {
        Some(ghost) => {
            ghost.pos_x = msg.pos_x;
            ghost.pos_y = msg.pos_y;
            ghost.vel_x = msg.vel_x;
            ghost.vel_y = msg.vel_y;
            shard_for_2x2(ghost.pos_x, ghost.pos_y) == Some(config.shard_id)
        }
        None => false,
    };

    if entered {
        if let Some(ghost) = handoff_mgr.remove_ghost(&msg.entity_id) {
            registry.promote_to_owned(ghost);
            info!("[INFO] Ghost {} entered our zone → promoted to Owned", msg.entity_id);
        }
    }
}

// Destination shard: take full authority — the ghost becomes the owned entity.
fn handle_handoff_complete(
    data: &[u8],
    registry: &mut PlayerRegistry,
    handoff_mgr: &mut HandoffManager,
) {
    if data.len() <= 1 { return; }
    if let Ok(msg) = serde_json::from_slice::<HandoffComplete>(&data[1..]) {
        info!("[INFO] HandoffComplete received for entity {}", msg.entity_id);
        if let Some(ghost) = handoff_mgr.remove_ghost(&msg.entity_id) {
            registry.promote_to_owned(ghost);
        }
        handoff_mgr.complete_handoff(&msg.entity_id);
    }
}

// Source shard tick: stream ghost updates to the destination, and finalize the
// handoff once the entity has actually left this shard's region.
fn drive_handoffs(
    mut registry: ResMut<PlayerRegistry>,
    mut handoff_mgr: ResMut<HandoffManager>,
    peer: Res<GamePeerResource>,
    broker: Res<BrokerConnection>,
    config: Res<ServerConfig>,
) {
    let broker_conn = match broker.connection { Some(c) => c, None => return };

    struct Step { entity_id: String, uuid: [u8; 16], dest: u32, x: f32, y: f32, vx: f32, vy: f32, accepted: bool, crossed: bool }
    let mut steps = Vec::new();

    for player in registry.players.values() {
        if player.state != EntityState::PendingHandoff { continue; }
        let h = match handoff_mgr.pending.get(&player.id) { Some(h) => h, None => continue };
        let uuid = match uuid::Uuid::parse_str(&player.id) { Ok(u) => *u.as_bytes(), Err(_) => continue };
        let crossed = shard_for_2x2(player.pos_x, player.pos_y) != Some(config.shard_id);
        steps.push(Step {
            entity_id: player.id.clone(), uuid, dest: h.dest_shard_id, accepted: h.accepted,
            x: player.pos_x, y: player.pos_y, vx: player.vel_x, vy: player.vel_y, crossed,
        });
    }

    for step in steps {
        // Retransmit the request until the destination accepts (datagrams can drop).
        if !step.accepted {
            let req = HandoffRequest {
                entity_id: step.entity_id.clone(), from_shard: config.shard_id,
                pos_x: step.x, pos_y: step.y, vel_x: step.vx, vel_y: step.vy,
            };
            if let Ok(json) = serde_json::to_string(&req) {
                let mut inner = vec![0x20u8]; // HandoffRequest tag
                inner.extend_from_slice(json.as_bytes());
                route_to_shard(&peer.0, &broker_conn, step.dest, &inner);
            }
            continue;
        }

        // Accepted: stream the authoritative position to the ghost each tick.
        let gu = GhostUpdate { entity_id: step.entity_id.clone(), pos_x: step.x, pos_y: step.y, vel_x: step.vx, vel_y: step.vy };
        if let Ok(json) = serde_json::to_string(&gu) {
            let mut inner = vec![0x23u8]; // GhostUpdate tag
            inner.extend_from_slice(json.as_bytes());
            route_to_shard(&peer.0, &broker_conn, step.dest, &inner);
        }

        // Once the entity has left our region, finalize and drop it locally.
        // The destination also promotes on position (handle_ghost_update), so a
        // lost HandoffComplete is not fatal — GhostUpdate is repeated every tick.
        if step.crossed {
            let hc = HandoffComplete { entity_id: step.entity_id.clone() };
            if let Ok(json) = serde_json::to_string(&hc) {
                let mut inner = vec![0x24u8]; // HandoffComplete tag
                inner.extend_from_slice(json.as_bytes());
                route_to_shard(&peer.0, &broker_conn, step.dest, &inner);
            }
            registry.remove_player_by_uuid(step.uuid);
            handoff_mgr.complete_handoff(&step.entity_id);
            info!("[INFO] HandoffComplete sent for entity {} → shard {}", step.entity_id, step.dest);
        }
    }
}

fn process_handoff_timeouts(
    time: Res<Time>,
    mut handoff_mgr: ResMut<HandoffManager>,
    mut registry: ResMut<PlayerRegistry>,
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
        // Revert to Owned so the entity is simulated and reported again, instead
        // of being stuck in PendingHandoff (which would silence it).
        if let Ok(u) = uuid::Uuid::parse_str(&entity_id) {
            registry.set_state_by_uuid(*u.as_bytes(), EntityState::Owned);
        }
        warn!("[WARN] Handoff timeout for entity {} → reverted to Owned", entity_id);
    }
}
