use std::collections::{HashMap, VecDeque};
use bevy::prelude::*;
use bevy::input::mouse::MouseWheel;
use bevy::sprite::{MaterialMesh2dBundle, Mesh2dHandle};
use bytes::{BytesMut, BufMut};
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use uuid::Uuid;

const SPEED: f32 = 200.0;
const PLAYER_RADIUS: f32 = 20.0;
const WORLD_SIZE: f32 = 1000.0;
const BOUNDARY: f32 = 500.0;

// Where the local player spawns (center of shard 0). The JOIN carries this so the
// owning shard can reject duplicate creations from AOI-relayed inputs (see F.2).
const SPAWN_X: f32 = 250.0;
const SPAWN_Y: f32 = 250.0;

// AOI half-sides in world units, must match the spatial service (F.3).
//   near = full-rate visibility (20 Hz), far = low-rate ring (5 Hz)
const AOI_NEAR: f32 = 200.0;
const AOI_FAR: f32 = 350.0;

// Remote player interpolation. We buffer a few snapshots and render the past so
// the per-frame motion is smooth regardless of the 20 Hz/5 Hz update rate.
const INTERP_BUFFER_SIZE: usize = 3;
// Position jumps larger than this (shard handoff, first appearance) snap instead
// of interpolating, so the player doesn't visibly streak across the world.
const SNAP_THRESHOLD: f32 = 400.0;

// Same 2x2 layout as the server/spatial service, for the HUD and tinting.
fn shard_for_2x2(x: f32, y: f32) -> u32 {
    let col = if x >= BOUNDARY { 1 } else { 0 };
    let row = if y >= BOUNDARY { 1 } else { 0 };
    row * 2 + col
}

fn shard_center_world(shard: u32) -> (f32, f32) {
    let col = (shard % 2) as f32;
    let row = (shard / 2) as f32;
    (col * BOUNDARY + BOUNDARY / 2.0, row * BOUNDARY + BOUNDARY / 2.0)
}

fn shard_bg_color(shard: u32) -> Color {
    match shard {
        0 => Color::rgba(0.8, 0.2, 0.2, 0.15),
        1 => Color::rgba(0.2, 0.4, 0.8, 0.15),
        2 => Color::rgba(0.2, 0.7, 0.3, 0.15),
        _ => Color::rgba(0.8, 0.7, 0.2, 0.15),
    }
}

// ── Resources ──────────────────────────────────────────────────────────────────

#[derive(Resource)]
struct NetworkPeer(GamePeer);

#[derive(Resource, Clone)]
struct LocalPlayer {
    uuid: [u8; 16],
    broker_conn: Option<GameConnection>,
}

// A remote player and when we last received an update for it (any stream).
struct TrackedPlayer {
    entity: Entity,
    last_seen: f32,
}

// One received position stamped with the local time we got it (elapsed_seconds).
struct Snapshot {
    t: f32,
    position: Vec2,
}

// Per-remote-player interpolation state. `render_pos` is the only thing that
// drives the Transform; network updates only push snapshots, never the Transform.
#[derive(Component)]
struct SnapshotBuffer {
    snapshots: VecDeque<Snapshot>,
    render_pos: Vec2,
}

#[derive(Resource, Default)]
struct OtherPlayers(HashMap<[u8; 16], TrackedPlayer>);

// Drop a remote player not refreshed within this many seconds. Must exceed the
// slowest stream period (far AOI = 5 Hz = 0.2s) so far players don't flicker.
const OTHER_PLAYER_TIMEOUT: f32 = 1.0;

// Desired velocity from keyboard input
#[derive(Resource, Default)]
struct LocalVelocity(Vec2);

// Timer to throttle ClientInput send rate (20fps)
#[derive(Resource)]
struct InputSendTimer(Timer);

impl Default for InputSendTimer {
    fn default() -> Self {
        InputSendTimer(Timer::from_seconds(0.05, TimerMode::Repeating))
    }
}

// Retries JOIN every 2s until the server acknowledges us (we appear in a Broadcast)
#[derive(Resource)]
struct JoinRetryTimer(Timer);

impl Default for JoinRetryTimer {
    fn default() -> Self {
        JoinRetryTimer(Timer::from_seconds(2.0, TimerMode::Repeating))
    }
}

#[derive(Resource, Default)]
struct Joined(bool);

// Marker for the local player entity
#[derive(Component)]
struct SelfMarker;

// Marker for the HUD text showing the current shard
#[derive(Component)]
struct ShardLabel;

// ── Gatekeeper ─────────────────────────────────────────────────────────────────

fn do_login() -> [u8; 16] {
    let gatekeeper = std::env::var("GATEKEEPER_ADDR")
        .unwrap_or("http://127.0.0.1:3000".to_string());

    let body = serde_json::json!({"username": "player", "password": "1234"});
    let json: serde_json::Value = reqwest::blocking::Client::new()
        .post(format!("{}/login", gatekeeper))
        .json(&body)
        .send()
        .expect("Cannot reach gatekeeper — is it running?")
        .json()
        .expect("Gatekeeper returned invalid JSON");

    match json.get("player_id").and_then(|v| v.as_str()) {
        Some(id) => {
            let uuid: Uuid = id.parse().expect("player_id is not a valid UUID");
            println!("Logged in as {}", uuid);
            *uuid.as_bytes()
        }
        None => {
            let err = json.get("error").and_then(|v| v.as_str()).unwrap_or("unknown error");
            panic!(
                "Gatekeeper rejected login: {}\n\
                 Make sure the full stack is running:\n\
                 broker, spatial_service, orchestrator, gatekeeper, dedicated_server",
                err
            );
        }
    }
}

// ── Startup systems ────────────────────────────────────────────────────────────

fn setup_network(mut commands: Commands, _local: Res<LocalPlayer>) {
    let broker_addr = std::env::var("BROKER_ADDR").unwrap_or("127.0.0.1:9010".to_string());
    let parts: Vec<&str> = broker_addr.rsplitn(2, ':').collect();
    let port: u16 = parts[0].parse().unwrap_or(9000);
    let host = parts[1];

    let peer = GamePeer::new(QuicBackend::new());
    peer.connect(host, port).expect("Failed to connect to broker");

    commands.insert_resource(NetworkPeer(peer));
    commands.insert_resource(OtherPlayers::default());
    commands.insert_resource(LocalVelocity::default());
    commands.insert_resource(InputSendTimer::default());
    commands.insert_resource(JoinRetryTimer::default());
    commands.insert_resource(Joined::default());

    info!("Connecting to broker at {}:{}", host, port);
}

fn setup_scene(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
) {
    commands.spawn(Camera2dBundle::default());

    // Tint each shard's quadrant so the partitioning is visible.
    for shard in 0..4u32 {
        let (cx, cy) = shard_center_world(shard);
        commands.spawn(SpriteBundle {
            sprite: Sprite {
                color: shard_bg_color(shard),
                custom_size: Some(Vec2::new(BOUNDARY, BOUNDARY)),
                ..default()
            },
            transform: Transform::from_xyz(cx, cy, -10.0),
            ..default()
        });
    }

    // World border + the two internal shard boundaries (thin dark sprites).
    let line = Color::rgb(0.15, 0.15, 0.15);
    let t = 3.0;
    let center = BOUNDARY; // world is [0,1000], center at 500
    let lines = [
        // internal boundaries
        (center, center, t, WORLD_SIZE),         // vertical x=500
        (center, center, WORLD_SIZE, t),         // horizontal y=500
        // world border
        (center, 0.0, WORLD_SIZE, t),            // top
        (center, WORLD_SIZE, WORLD_SIZE, t),     // bottom
        (0.0, center, t, WORLD_SIZE),            // left
        (WORLD_SIZE, center, t, WORLD_SIZE),     // right
    ];
    for (x, y, w, h) in lines {
        commands.spawn(SpriteBundle {
            sprite: Sprite { color: line, custom_size: Some(Vec2::new(w, h)), ..default() },
            transform: Transform::from_xyz(x, y, -9.0),
            ..default()
        });
    }

    // Local player: red circle, spawned at the center of shard 0.
    commands.spawn((
        SelfMarker,
        MaterialMesh2dBundle {
            mesh: Mesh2dHandle(meshes.add(Circle::new(PLAYER_RADIUS))),
            material: materials.add(ColorMaterial::from(Color::RED)),
            transform: Transform::from_xyz(SPAWN_X, SPAWN_Y, 1.0),
            ..default()
        },
    ));

    // HUD: current shard label (top-left).
    commands.spawn((
        ShardLabel,
        TextBundle::from_section(
            "Shard 0",
            TextStyle { font_size: 28.0, color: Color::WHITE, ..default() },
        )
        .with_style(Style {
            position_type: PositionType::Absolute,
            top: Val::Px(10.0),
            left: Val::Px(10.0),
            ..default()
        }),
    ));
}

// ── Update systems ─────────────────────────────────────────────────────────────

fn handle_network(
    time: Res<Time>,
    mut peer: ResMut<NetworkPeer>,
    mut local: ResMut<LocalPlayer>,
    mut joined: ResMut<Joined>,
    mut others: ResMut<OtherPlayers>,
    mut self_query: Query<&mut Transform, With<SelfMarker>>,
    mut other_query: Query<&mut SnapshotBuffer>,
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
) {
    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    let now = time.elapsed_seconds();

    // Local position drives the AOI cull. We read it once: a one-frame-stale
    // value is fine, and it avoids re-borrowing self_query inside the loop.
    let self_pos = self_query.get_single().ok().map(|t| t.translation.truncate());

    loop {
        match peer.0.poll() {
            Ok(Some(GameNetworkEvent::Connected(conn))) => {
                info!("Connected to broker: {}", conn.connection_id);
                local.broker_conn = Some(conn);
                send_join(&peer.0, &local.uuid, conn, &stream);
            }
            Ok(Some(GameNetworkEvent::Disconnected(_))) => {
                warn!("Disconnected from broker");
                local.broker_conn = None;
            }
            Ok(Some(GameNetworkEvent::Message { data, .. })) => {
                // Broadcast: TAG(1) + payload_len(2) + payload
                if data.len() < 3 || data[0] != 0x04 { continue; }
                let payload_len = u16::from_le_bytes([data[1], data[2]]) as usize;
                if data.len() < 3 + payload_len || payload_len < 2 { continue; }

                let payload = &data[3..3 + payload_len];
                let player_count = u16::from_le_bytes([payload[0], payload[1]]) as usize;

                for i in 0..player_count {
                    let off = 2 + i * 24;
                    if payload.len() < off + 24 { break; }
                    let uuid: [u8; 16] = payload[off..off+16].try_into().unwrap();
                    let x = f32::from_le_bytes(payload[off+16..off+20].try_into().unwrap());
                    let y = f32::from_le_bytes(payload[off+20..off+24].try_into().unwrap());

                    if uuid == local.uuid {
                        joined.0 = true;
                        if let Ok(mut t) = self_query.get_single_mut() {
                            t.translation = Vec3::new(x, y, 1.0);
                        }
                        continue;
                    }

                    // AOI cull: subscriptions are per-shard, so a broadcast can carry
                    // players well outside our AOI square. Render only what's inside it.
                    if let Some(sp) = self_pos
                        && ((x - sp.x).abs() > AOI_FAR || (y - sp.y).abs() > AOI_FAR)
                    {
                        continue;
                    }

                    let new_pos = Vec2::new(x, y);
                    if let Some(tracked) = others.0.get_mut(&uuid) {
                        tracked.last_seen = now;
                        if let Ok(mut buf) = other_query.get_mut(tracked.entity) {
                            // A jump too big to interpolate means a teleport/handoff:
                            // drop history and snap render_pos to the new position.
                            let snap = buf.snapshots.back()
                                .map_or(false, |last| (new_pos - last.position).length() > SNAP_THRESHOLD);
                            if snap {
                                buf.snapshots.clear();
                                buf.render_pos = new_pos;
                            }
                            buf.snapshots.push_back(Snapshot { t: now, position: new_pos });
                            if buf.snapshots.len() > INTERP_BUFFER_SIZE {
                                buf.snapshots.pop_front();
                            }
                        }
                    } else {
                        let mut snapshots = VecDeque::with_capacity(INTERP_BUFFER_SIZE);
                        snapshots.push_back(Snapshot { t: now, position: new_pos });
                        let e = commands.spawn((
                            MaterialMesh2dBundle {
                                mesh: Mesh2dHandle(meshes.add(Circle::new(PLAYER_RADIUS))),
                                material: materials.add(ColorMaterial::from(Color::BLUE)),
                                transform: Transform::from_xyz(x, y, 0.0),
                                ..default()
                            },
                            SnapshotBuffer { snapshots, render_pos: new_pos },
                        )).id();
                        others.0.insert(uuid, TrackedPlayer { entity: e, last_seen: now });
                    }
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => { error!("Network error: {}", e); break; }
        }
    }

    // Despawn players we haven't heard about for a while (left AOI / disconnected).
    // Players still reported by any stream keep a fresh last_seen and survive.
    others.0.retain(|_uuid, tracked| {
        if now - tracked.last_seen > OTHER_PLAYER_TIMEOUT {
            commands.entity(tracked.entity).despawn();
            false
        } else {
            true
        }
    });
}

// Smoothly move remote players toward buffered positions every frame. We render
// `render_lag` in the past (one observed update interval) so there is always a
// future snapshot to interpolate toward. If the buffer runs dry we freeze.
fn interpolate_positions(
    time: Res<Time>,
    mut query: Query<(&mut SnapshotBuffer, &mut Transform)>,
) {
    let now = time.elapsed_seconds();
    for (mut buf, mut transform) in query.iter_mut() {
        let n = buf.snapshots.len();
        if n >= 2 {
            // render_lag adapts to the stream rate (50ms near, 200ms far).
            let render_lag = buf.snapshots[n - 1].t - buf.snapshots[n - 2].t;
            let render_time = now - render_lag;

            if render_time <= buf.snapshots[0].t {
                // render_time is before our oldest snapshot: clamp to it.
                buf.render_pos = buf.snapshots[0].position;
            } else {
                // Find the pair of snapshots that brackets render_time and lerp.
                // If none brackets it, render_time is past the latest → freeze.
                for i in 0..n - 1 {
                    let (at, ap) = (buf.snapshots[i].t, buf.snapshots[i].position);
                    let (bt, bp) = (buf.snapshots[i + 1].t, buf.snapshots[i + 1].position);
                    if at <= render_time && render_time <= bt {
                        let span = bt - at;
                        let f = if span > 0.0 { (render_time - at) / span } else { 1.0 };
                        buf.render_pos = ap.lerp(bp, f.clamp(0.0, 1.0));
                        break;
                    }
                }
            }
        }
        transform.translation.x = buf.render_pos.x;
        transform.translation.y = buf.render_pos.y;
    }
}

fn send_join(peer: &GamePeer, uuid: &[u8; 16], conn: GameConnection, stream: &GameStream) {
    // Subscribe to shard:0
    let mut topic = [0u8; 32];
    topic[..7].copy_from_slice(b"shard:0");
    let mut sub = BytesMut::with_capacity(49);
    sub.put_u8(0x01); // TAG_SUBSCRIBE
    sub.put_slice(uuid);
    sub.put_slice(&topic);
    let _ = peer.send(&conn, stream, sub.freeze());

    // Send JOIN, carrying the spawn position so only the owning shard creates us.
    let mut input = [0u8; 16];
    input[0] = 0x01; // JOIN
    input[1..5].copy_from_slice(&SPAWN_X.to_le_bytes());
    input[5..9].copy_from_slice(&SPAWN_Y.to_le_bytes());
    let mut msg = BytesMut::with_capacity(33);
    msg.put_u8(0x05); // TAG_CLIENT_INPUT
    msg.put_slice(uuid);
    msg.put_slice(&input);
    let _ = peer.send(&conn, stream, msg.freeze());
    info!("Sent Subscribe + JOIN");
}

// Retry JOIN every 2s until the server confirms us in a Broadcast.
fn retry_join(
    time: Res<Time>,
    mut timer: ResMut<JoinRetryTimer>,
    joined: Res<Joined>,
    peer: Res<NetworkPeer>,
    local: Res<LocalPlayer>,
) {
    if joined.0 { return; }
    if !timer.0.tick(time.delta()).just_finished() { return; }
    let conn = match local.broker_conn {
        Some(c) => c,
        None => return,
    };
    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    info!("Retrying Subscribe + JOIN...");
    send_join(&peer.0, &local.uuid, conn, &stream);
}

// Update the HUD label with the shard currently owning the local player.
fn update_local_shard(
    self_q: Query<&Transform, With<SelfMarker>>,
    mut label_q: Query<&mut Text, With<ShardLabel>>,
) {
    let Ok(t) = self_q.get_single() else { return; };
    let Ok(mut text) = label_q.get_single_mut() else { return; };
    let shard = shard_for_2x2(t.translation.x, t.translation.y);
    text.sections[0].value = format!("Shard {}", shard);
}

// Keep the camera centered on the local player.
fn camera_follow(
    player: Query<&Transform, (With<SelfMarker>, Without<Camera>)>,
    mut camera: Query<&mut Transform, With<Camera>>,
) {
    let Ok(player_t) = player.get_single() else { return; };
    let Ok(mut cam_t) = camera.get_single_mut() else { return; };
    cam_t.translation.x = player_t.translation.x;
    cam_t.translation.y = player_t.translation.y;
}

// Mouse wheel zooms the camera. Larger scale = more world visible (zoomed out).
fn zoom_camera(
    mut wheel: EventReader<MouseWheel>,
    mut projection: Query<&mut OrthographicProjection, With<Camera>>,
) {
    let Ok(mut proj) = projection.get_single_mut() else { return; };
    for ev in wheel.read() {
        proj.scale = (proj.scale - ev.y * 0.1).clamp(0.3, 3.0);
    }
}

// Draw the two AOI levels as squares around the local player so the
// near (full-rate) and far (low-rate) visibility rings are visible.
fn draw_aoi(player: Query<&Transform, With<SelfMarker>>, mut gizmos: Gizmos) {
    let Ok(t) = player.get_single() else { return; };
    let pos = t.translation.truncate();
    gizmos.rect_2d(pos, 0.0, Vec2::splat(2.0 * AOI_NEAR), Color::rgb(0.2, 0.9, 0.2));
    gizmos.rect_2d(pos, 0.0, Vec2::splat(2.0 * AOI_FAR), Color::rgba(0.9, 0.9, 0.2, 0.6));
}

fn read_input(keys: Res<ButtonInput<KeyCode>>, mut velocity: ResMut<LocalVelocity>) {
    let mut dir = Vec2::ZERO;
    if keys.pressed(KeyCode::ArrowLeft)  || keys.pressed(KeyCode::KeyA) { dir.x -= 1.0; }
    if keys.pressed(KeyCode::ArrowRight) || keys.pressed(KeyCode::KeyD) { dir.x += 1.0; }
    if keys.pressed(KeyCode::ArrowUp)    || keys.pressed(KeyCode::KeyW) { dir.y += 1.0; }
    if keys.pressed(KeyCode::ArrowDown)  || keys.pressed(KeyCode::KeyS) { dir.y -= 1.0; }
    velocity.0 = if dir != Vec2::ZERO { dir.normalize() * SPEED } else { Vec2::ZERO };
}

// Move the local player locally each frame for responsiveness.
fn apply_local_movement(
    time: Res<Time>,
    velocity: Res<LocalVelocity>,
    mut query: Query<&mut Transform, With<SelfMarker>>,
) {
    if let Ok(mut t) = query.get_single_mut() {
        t.translation.x += velocity.0.x * time.delta_seconds();
        t.translation.y += velocity.0.y * time.delta_seconds();
    }
}

fn send_input(
    time: Res<Time>,
    mut timer: ResMut<InputSendTimer>,
    velocity: Res<LocalVelocity>,
    peer: Res<NetworkPeer>,
    local: Res<LocalPlayer>,
) {
    if !timer.0.tick(time.delta()).just_finished() { return; }
    let broker_conn = match local.broker_conn {
        Some(c) => c,
        None => return,
    };

    let mut input = [0u8; 16];
    input[0] = 0x02; // MOVE
    input[1..5].copy_from_slice(&velocity.0.x.to_le_bytes());
    input[5..9].copy_from_slice(&velocity.0.y.to_le_bytes());

    let mut msg = BytesMut::with_capacity(33);
    msg.put_u8(0x05); // TAG_CLIENT_INPUT
    msg.put_slice(&local.uuid);
    msg.put_slice(&input);

    let stream = GameStream::new(0, GameStreamReliability::Unreliable);
    let _ = peer.0.send(&broker_conn, &stream, msg.freeze());
}

// ── Main ───────────────────────────────────────────────────────────────────────

fn main() {
    // Login before starting Bevy (blocking HTTP, simpler than async startup)
    let uuid = do_login();

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: "MMORPG Client".to_string(),
                resolution: (800.0, 600.0).into(),
                ..default()
            }),
            ..default()
        }))
        .insert_resource(LocalPlayer { uuid, broker_conn: None })
        .add_systems(Startup, (setup_network, setup_scene).chain())
        .add_systems(Update, (
            handle_network,
            interpolate_positions.after(handle_network),
            retry_join,
            read_input,
            apply_local_movement,
            camera_follow,
            zoom_camera,
            draw_aoi,
            update_local_shard,
            send_input,
        ))
        .run();
}
