mod quad_tree;

use std::collections::HashMap;
use std::time::Duration;
use bytes::{BytesMut, BufMut};
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use game_sockets::protocols::QuicBackend;

const TAG_SUBSCRIBE: u8 = 0x01;
const TAG_UNSUBSCRIBE: u8 = 0x02;
const TAG_POSITION_UPDATE: u8 = 0x10;
const TAG_CROSSING_ALERT: u8 = 0x11;
const BOUNDARY_MARGIN: f32 = 50.0;

// Actions returned by compute_position_update — no network, fully testable.
#[derive(Debug, PartialEq)]
enum SpatialAction {
    Subscribe([u8; 16], u32),
    Unsubscribe([u8; 16], u32),
    CrossingAlert([u8; 16], u32), // dest_shard_id
}

struct SpatialState {
    client_shard: HashMap<[u8; 16], u32>,
    broker_conn: Option<GameConnection>,
}

impl SpatialState {
    fn new() -> Self {
        Self { client_shard: HashMap::new(), broker_conn: None }
    }
}

// Pure logic: computes subscription changes and crossing alerts for a position update.
// Mutates client_shard when the entity moves to a new shard.
// Suppresses CrossingAlert toward the shard the entity just came from.
fn compute_position_update(
    client_uuid: [u8; 16],
    x: f32,
    y: f32,
    client_shard: &mut HashMap<[u8; 16], u32>,
    quad_tree: &quad_tree::QuadTree,
    margin: f32,
) -> Vec<SpatialAction> {
    let mut actions = Vec::new();

    let new_shard = match quad_tree.shard_for([x, y]) {
        Some(id) => id,
        None => return actions,
    };

    let old_shard = client_shard.get(&client_uuid).copied();

    if old_shard != Some(new_shard) {
        if let Some(old) = old_shard {
            actions.push(SpatialAction::Unsubscribe(client_uuid, old));
        }
        actions.push(SpatialAction::Subscribe(client_uuid, new_shard));
        client_shard.insert(client_uuid, new_shard);
    }

    // Don't alert toward the shard we just came from — we already left it.
    for dest in quad_tree.shards_near([x, y], margin) {
        if dest != new_shard && Some(dest) != old_shard {
            actions.push(SpatialAction::CrossingAlert(client_uuid, dest));
        }
    }

    actions
}

#[tokio::main]
async fn main() {
    let spatial_port: u16 = std::env::var("SPATIAL_PORT")
        .unwrap_or("9001".to_string())
        .parse()
        .expect("SPATIAL_PORT must be a valid port number");

    let broker_addr = std::env::var("BROKER_ADDR").unwrap_or("127.0.0.1:9000".to_string());
    let (broker_host, broker_port) = parse_addr(&broker_addr);

    let quad_tree = quad_tree::build_default();
    let mut state = SpatialState::new();
    let stream = GameStream::new(0, GameStreamReliability::Unreliable);

    let mut peer = GamePeer::new(QuicBackend::new());
    peer.listen("0.0.0.0", spatial_port).expect("Failed to bind spatial service port");
    peer.connect(&broker_host, broker_port).expect("Failed to connect to broker");

    println!("Spatial service listening on :{}", spatial_port);
    println!("Connecting to broker at {}", broker_addr);

    loop {
        match peer.poll() {
            Ok(Some(GameNetworkEvent::Connected(conn))) => {
                if state.broker_conn.is_none() {
                    println!("Connected to broker: {}", conn.connection_id);
                    state.broker_conn = Some(conn);
                } else {
                    println!("Shard connected: {}", conn.connection_id);
                }
            }
            Ok(Some(GameNetworkEvent::Disconnected(conn))) => {
                if state.broker_conn == Some(conn) {
                    println!("Broker disconnected");
                    state.broker_conn = None;
                } else {
                    println!("Shard disconnected: {}", conn.connection_id);
                }
            }
            Ok(Some(GameNetworkEvent::Message { connection, data, .. })) => {
                if data.is_empty() { continue; }
                match data[0] {
                    TAG_POSITION_UPDATE => handle_position_update(
                        &data, connection, &mut state, &quad_tree, &peer, &stream,
                    ),
                    tag => println!("Unknown tag: 0x{:02x}", tag),
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(e) => eprintln!("Network error: {}", e),
        }
    }
}

fn handle_position_update(
    data: &[u8],
    from: GameConnection,
    state: &mut SpatialState,
    quad_tree: &quad_tree::QuadTree,
    peer: &GamePeer,
    stream: &GameStream,
) {
    // TAG(1) + shard_id(4) + client_uuid(16) + x(4) + y(4) = 29 bytes
    if data.len() < 29 { return; }

    let _shard_id = u32::from_le_bytes(data[1..5].try_into().unwrap());
    let client_uuid: [u8; 16] = data[5..21].try_into().unwrap();
    let x = f32::from_le_bytes(data[21..25].try_into().unwrap());
    let y = f32::from_le_bytes(data[25..29].try_into().unwrap());

    let actions = compute_position_update(
        client_uuid, x, y, &mut state.client_shard, quad_tree, BOUNDARY_MARGIN,
    );

    for action in actions {
        match action {
            SpatialAction::Subscribe(uuid, sid) => {
                println!("Client {:?}: subscribe to shard:{}", &uuid[..4], sid);
                send_to_broker(state, peer, stream, TAG_SUBSCRIBE, &uuid, sid);
            }
            SpatialAction::Unsubscribe(uuid, sid) => {
                println!("Client {:?}: unsubscribe from shard:{}", &uuid[..4], sid);
                send_to_broker(state, peer, stream, TAG_UNSUBSCRIBE, &uuid, sid);
            }
            SpatialAction::CrossingAlert(uuid, dest) => {
                println!("CrossingAlert: client {:?} → dest shard:{}", &uuid[..4], dest);
                let mut buf = BytesMut::with_capacity(21);
                buf.put_u8(TAG_CROSSING_ALERT);
                buf.put_slice(&uuid);
                buf.put_u32_le(dest);
                // Send back to the shard that owns this entity.
                let _ = peer.send(&from, stream, buf.freeze());
            }
        }
    }
}

fn send_to_broker(
    state: &SpatialState,
    peer: &GamePeer,
    stream: &GameStream,
    tag: u8,
    client_uuid: &[u8; 16],
    shard_id: u32,
) {
    let broker_conn = match state.broker_conn {
        Some(c) => c,
        None => {
            eprintln!("Cannot send to broker: not connected");
            return;
        }
    };

    let topic = shard_id_to_topic(shard_id);
    let mut buf = BytesMut::with_capacity(1 + 16 + 32);
    buf.put_u8(tag);
    buf.put_slice(client_uuid);
    buf.put_slice(&topic);

    let _ = peer.send(&broker_conn, stream, buf.freeze());
}

fn shard_id_to_topic(shard_id: u32) -> [u8; 32] {
    let mut topic = [0u8; 32];
    let label = format!("shard:{}", shard_id);
    let bytes = label.as_bytes();
    let len = bytes.len().min(32);
    topic[..len].copy_from_slice(&bytes[..len]);
    topic
}

fn parse_addr(addr: &str) -> (String, u16) {
    let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
    let port = parts[0].parse().expect("Invalid port in address");
    let host = parts[1].to_string();
    (host, port)
}

#[cfg(test)]
mod tests {
    use super::*;

    // World layout (build_default): 1000×1000, split into 4 quadrants:
    //   Shard 0: x∈[0,500)  y∈[0,500)   top-left
    //   Shard 1: x∈[500,1000) y∈[0,500)  top-right
    //   Shard 2: x∈[0,500)  y∈[500,1000) bottom-left
    //   Shard 3: x∈[500,1000) y∈[500,1000) bottom-right

    fn tree() -> quad_tree::QuadTree { quad_tree::build_default() }

    fn uuid(n: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[0] = n;
        id
    }

    fn run(map: &mut HashMap<[u8;16], u32>, u: [u8;16], x: f32, y: f32) -> Vec<SpatialAction> {
        compute_position_update(u, x, y, map, &tree(), BOUNDARY_MARGIN)
    }

    // ── First entry ────────────────────────────────────────────────────────────

    #[test]
    fn first_entry_subscribe_only() {
        let mut map = HashMap::new();
        let u = uuid(1);
        let a = run(&mut map, u, 250.0, 250.0);
        assert_eq!(a, vec![SpatialAction::Subscribe(u, 0)]);
        assert_eq!(map[&u], 0);
    }

    #[test]
    fn first_entry_near_boundary_also_alerts() {
        // Player spawns 40 units from vertical boundary → Subscribe + CrossingAlert
        let mut map = HashMap::new();
        let u = uuid(2);
        let a = run(&mut map, u, 460.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Subscribe(u, 0),
            SpatialAction::CrossingAlert(u, 1),
        ]);
    }

    // ── Stay in same shard ─────────────────────────────────────────────────────

    #[test]
    fn no_action_when_staying_in_shard() {
        let mut map = HashMap::new();
        let u = uuid(3);
        map.insert(u, 0);
        let a = run(&mut map, u, 300.0, 300.0);
        assert!(a.is_empty());
    }

    // ── Vertical boundary crossings ────────────────────────────────────────────

    #[test]
    fn cross_vertical_left_to_right() {
        let mut map = HashMap::new();
        let u = uuid(4);
        map.insert(u, 0);
        // Land deep in shard 1, far from any boundary → no alert
        let a = run(&mut map, u, 700.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 1),
        ]);
        assert_eq!(map[&u], 1);
    }

    #[test]
    fn cross_vertical_right_to_left() {
        let mut map = HashMap::new();
        let u = uuid(5);
        map.insert(u, 1);
        let a = run(&mut map, u, 250.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 1),
            SpatialAction::Subscribe(u, 0),
        ]);
    }

    // Exact boundary point x=500 belongs to the right shard (inclusive ≥).
    #[test]
    fn cross_on_exact_vertical_boundary_line() {
        let mut map = HashMap::new();
        let u = uuid(6);
        map.insert(u, 0);
        let a = run(&mut map, u, 500.0, 250.0);
        // x=500 → shard 1. No CrossingAlert toward shard 0 (old_shard suppressed).
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 1),
        ]);
        assert_eq!(map[&u], 1);
    }

    // ── Horizontal boundary crossings ──────────────────────────────────────────

    #[test]
    fn cross_horizontal_top_to_bottom() {
        let mut map = HashMap::new();
        let u = uuid(7);
        map.insert(u, 0);
        let a = run(&mut map, u, 250.0, 700.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 2),
        ]);
    }

    #[test]
    fn cross_horizontal_bottom_to_top() {
        let mut map = HashMap::new();
        let u = uuid(8);
        map.insert(u, 2);
        let a = run(&mut map, u, 250.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 2),
            SpatialAction::Subscribe(u, 0),
        ]);
    }

    // ── Diagonal crossings ─────────────────────────────────────────────────────

    #[test]
    fn diagonal_jump_shard0_to_shard3() {
        // Fast teleport: skips the alert window entirely
        let mut map = HashMap::new();
        let u = uuid(9);
        map.insert(u, 0);
        let a = run(&mut map, u, 750.0, 750.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 3),
        ]);
    }

    #[test]
    fn diagonal_jump_shard1_to_shard2() {
        let mut map = HashMap::new();
        let u = uuid(10);
        map.insert(u, 1);
        let a = run(&mut map, u, 250.0, 750.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 1),
            SpatialAction::Subscribe(u, 2),
        ]);
    }

    // Step-by-step diagonal walk through the shared corner.
    // This is the richest scenario: player crosses from 0→1→3.
    #[test]
    fn diagonal_step_by_step_through_corner() {
        let mut map = HashMap::new();
        let u = uuid(11);
        map.insert(u, 0);

        // Step 1: approaching corner from shard 0 — all 3 neighbours are in range
        let a = run(&mut map, u, 490.0, 490.0);
        assert_eq!(a, vec![
            SpatialAction::CrossingAlert(u, 1),
            SpatialAction::CrossingAlert(u, 2),
            SpatialAction::CrossingAlert(u, 3),
        ]);
        assert_eq!(map[&u], 0); // no crossing yet

        // Step 2: cross into shard 1 (x>500), still near y=500 boundary
        // No alert toward shard 0 (just came from it)
        let a = run(&mut map, u, 510.0, 490.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 1),
            SpatialAction::CrossingAlert(u, 2),
            SpatialAction::CrossingAlert(u, 3),
        ]);
        assert_eq!(map[&u], 1);

        // Step 3: cross into shard 3 (y>500) — no alert toward shard 1 (just came from)
        let a = run(&mut map, u, 510.0, 510.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 1),
            SpatialAction::Subscribe(u, 3),
            SpatialAction::CrossingAlert(u, 0),
            SpatialAction::CrossingAlert(u, 2),
        ]);
        assert_eq!(map[&u], 3);
    }

    // ── Crossing alerts ────────────────────────────────────────────────────────

    #[test]
    fn alert_near_vertical_boundary() {
        let mut map = HashMap::new();
        let u = uuid(12);
        map.insert(u, 0);
        // x=460: 460+50=510 > 500 → shard 1 in margin
        let a = run(&mut map, u, 460.0, 250.0);
        assert_eq!(a, vec![SpatialAction::CrossingAlert(u, 1)]);
    }

    #[test]
    fn alert_near_horizontal_boundary() {
        let mut map = HashMap::new();
        let u = uuid(13);
        map.insert(u, 0);
        let a = run(&mut map, u, 250.0, 460.0);
        assert_eq!(a, vec![SpatialAction::CrossingAlert(u, 2)]);
    }

    #[test]
    fn alerts_near_corner_three_neighbours() {
        let mut map = HashMap::new();
        let u = uuid(14);
        map.insert(u, 0);
        // (490,490): all 4 shards within margin; alert for 1, 2, 3
        let a = run(&mut map, u, 490.0, 490.0);
        assert_eq!(a, vec![
            SpatialAction::CrossingAlert(u, 1),
            SpatialAction::CrossingAlert(u, 2),
            SpatialAction::CrossingAlert(u, 3),
        ]);
    }

    #[test]
    fn alert_from_shard3_near_corner() {
        let mut map = HashMap::new();
        let u = uuid(15);
        map.insert(u, 3);
        // (510,510): mirror of above from shard 3's perspective
        let a = run(&mut map, u, 510.0, 510.0);
        assert_eq!(a, vec![
            SpatialAction::CrossingAlert(u, 0),
            SpatialAction::CrossingAlert(u, 1),
            SpatialAction::CrossingAlert(u, 2),
        ]);
    }

    #[test]
    fn no_alert_deep_in_shard() {
        let mut map = HashMap::new();
        let u = uuid(16);
        map.insert(u, 3);
        let a = run(&mut map, u, 750.0, 750.0);
        assert!(a.is_empty());
    }

    // Exactly at margin distance: strict < means no alert fires.
    #[test]
    fn no_alert_at_exact_margin_distance() {
        let mut map = HashMap::new();
        let u = uuid(17);
        map.insert(u, 0);
        // x=450: 450+50=500, shard 1 requires 500 < 500 → FALSE
        let a = run(&mut map, u, 450.0, 250.0);
        assert!(a.is_empty());
    }

    #[test]
    fn alert_just_inside_margin() {
        let mut map = HashMap::new();
        let u = uuid(18);
        map.insert(u, 0);
        // x=451: 451+50=501 > 500 → alert
        let a = run(&mut map, u, 451.0, 250.0);
        assert_eq!(a, vec![SpatialAction::CrossingAlert(u, 1)]);
    }

    // After crossing, no spurious alert back toward the old shard.
    #[test]
    fn no_spurious_alert_toward_previous_shard() {
        let mut map = HashMap::new();
        let u = uuid(19);
        map.insert(u, 0);
        // x=510: just crossed into shard 1, still within margin of shard 0
        let a = run(&mut map, u, 510.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 1),
            // No CrossingAlert for shard 0 — we just left it
        ]);
    }

    // ── Out of bounds ──────────────────────────────────────────────────────────

    #[test]
    fn out_of_bounds_no_action() {
        let mut map = HashMap::new();
        let u = uuid(20);
        map.insert(u, 0);
        let a = run(&mut map, u, -50.0, 250.0);
        assert!(a.is_empty());
        assert_eq!(map[&u], 0); // unchanged
    }

    #[test]
    fn out_of_bounds_new_player_no_subscribe() {
        let mut map = HashMap::new();
        let u = uuid(21);
        let a = run(&mut map, u, 1500.0, 250.0);
        assert!(a.is_empty());
        assert!(!map.contains_key(&u));
    }

    #[test]
    fn out_of_bounds_then_reenter_different_shard() {
        let mut map = HashMap::new();
        let u = uuid(22);
        map.insert(u, 0);
        // Leave world
        run(&mut map, u, -100.0, 250.0);
        assert_eq!(map[&u], 0); // still remembered
        // Re-enter in shard 1
        let a = run(&mut map, u, 700.0, 250.0);
        assert_eq!(a, vec![
            SpatialAction::Unsubscribe(u, 0),
            SpatialAction::Subscribe(u, 1),
        ]);
    }

    // ── Multiple players independent ───────────────────────────────────────────

    #[test]
    fn two_players_independent() {
        let mut map = HashMap::new();
        let a = uuid(30);
        let b = uuid(31);

        run(&mut map, a, 250.0, 250.0); // a → shard 0
        run(&mut map, b, 750.0, 750.0); // b → shard 3

        // A moves to shard 1
        let actions = run(&mut map, a, 700.0, 250.0);
        assert_eq!(actions, vec![
            SpatialAction::Unsubscribe(a, 0),
            SpatialAction::Subscribe(a, 1),
        ]);
        assert_eq!(map[&b], 3); // B unaffected
    }

    // ── World edge and corner ──────────────────────────────────────────────────

    #[test]
    fn player_at_world_origin() {
        let mut map = HashMap::new();
        let u = uuid(32);
        // (0,0) is shard 0, no neighbours to the left or above
        let a = run(&mut map, u, 0.0, 0.0);
        assert_eq!(a, vec![SpatialAction::Subscribe(u, 0)]);
    }

    #[test]
    fn topic_encoding() {
        let t = shard_id_to_topic(0);
        assert_eq!(&t[..7], b"shard:0");
        assert_eq!(t[7], 0);

        let t = shard_id_to_topic(3);
        assert_eq!(&t[..7], b"shard:3");
        assert_eq!(t[7], 0);
    }
}
