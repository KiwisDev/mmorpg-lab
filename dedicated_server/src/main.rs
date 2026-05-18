use bevy::prelude::*;
use bevy::log::LogPlugin;
use bevy::app::ScheduleRunnerPlugin;
use std::time::Duration;
use game_sockets::{GamePeer, GameNetworkEvent, GameStreamReliability, GameConnection, GameStream};
use game_sockets::protocols::QuicBackend;
use crate::resources::{ServerConfig, PlayerRegistry, HeartbeatTimer, Orchestrator};
use shared::Heartbeat;
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
        .init_resource::<HeartbeatTimer>()
        .add_systems(Startup, (setup_networking, connect_to_orchestrator).chain())
        .add_systems(Update, (handle_network_events, send_heartbeat).chain())
        .run();
}

fn setup_networking(mut commands: Commands, config: Res<ServerConfig>) {
    let mut peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = peer.listen("0.0.0.0", config.port) {
        error!("[ERROR] Failed to bind Quic: {}", e);
        // Consider exiting the app here
    } else {
        info!("[INFO] Listening on: 0.0.0.0:{}", config.port);
    }
    commands.insert_resource(GamePeerResource(peer));
}

fn connect_to_orchestrator(mut peer: ResMut<GamePeerResource>, config: Res<ServerConfig>) {
    info!("[INFO] Connecting to orchestrator at {}", config.orchestrator_addr);
    if let Err(e) = peer.0.connect(&config.orchestrator_addr.ip().to_string(), config.orchestrator_addr.port()) {
        error!("[ERROR] Failed to connect to orchestrator: {}", e);
    }
}

fn handle_network_events(
    mut peer: ResMut<GamePeerResource>,
    mut registry: ResMut<PlayerRegistry>,
    mut orchestrator: ResMut<Orchestrator>,
    config: Res<ServerConfig>,
) {
    while let Ok(Some(event)) = peer.0.poll() {
        match event {
            GameNetworkEvent::Connected(conn) => {
                if orchestrator.connection.is_none() {
                    info!("[INFO] Orchestrator connected: {:?}", conn);
                    orchestrator.connection = Some(conn);
                } else {
                    info!("[INFO] Peer connected: {:?}", conn);
                }
            }
            GameNetworkEvent::Disconnected(conn) => {
                if Some(conn) == orchestrator.connection {
                    orchestrator.connection = None;
                    warn!("[WARN] Orchestrator disconnected.");
                } else {
                    info!("[INFO] Peer disconnected: {:?}", conn);
                    registry.remove_player(&conn);
                }
            }
            GameNetworkEvent::Message { connection, stream, data } => {
                let message = String::from_utf8_lossy(&data);
                info!("[DEBUG] Received message from {:?} on stream {:?}: {}", connection, stream, message);

                if let Some(username) = message.strip_prefix("JOIN ") {
                    handle_join(username, connection, stream, &mut peer.0, &mut registry, config.max_players);
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
                ip: "0.0.0.0".to_string(),
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