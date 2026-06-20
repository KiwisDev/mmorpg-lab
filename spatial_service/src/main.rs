mod quad_tree;

use std::collections::{HashMap, HashSet};
use std::time::Duration;
use bytes::{BytesMut, BufMut};
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use game_sockets::protocols::QuicBackend;

const TAG_SUBSCRIBE: u8 = 0x01;
const TAG_UNSUBSCRIBE: u8 = 0x02;
const TAG_POSITION_UPDATE: u8 = 0x10;
const TAG_CROSSING_ALERT: u8 = 0x11;

// Authority: how close to a neighbour shard triggers a handoff pre-warm.
const BOUNDARY_MARGIN: f32 = 50.0;

// Visibility (AOI), half-side in world units. A shard is 500 wide:
//   near (20 Hz) = shards within AOI_NEAR of the player → "shard:N"
//   far  (5 Hz)  = extra shards within AOI_FAR (but not near) → "shard:N:far"
const AOI_NEAR: f32 = 200.0;
const AOI_FAR: f32 = 350.0;

// Subscription changes produced by compute_aoi_subscriptions — no network, testable.
#[derive(Debug, PartialEq)]
enum SubAction {
    Subscribe([u8; 16], String),   // (client_uuid, topic)
    Unsubscribe([u8; 16], String),
}

struct SpatialState {
    // Authority shard per client (drives handoff crossing alerts).
    client_home: HashMap<[u8; 16], u32>,
    // Current AOI topics each client is subscribed to (drives the diff).
    client_subs: HashMap<[u8; 16], HashSet<String>>,
    broker_conn: Option<GameConnection>,
}

impl SpatialState {
    fn new() -> Self {
        Self {
            client_home: HashMap::new(),
            client_subs: HashMap::new(),
            broker_conn: None,
        }
    }
}

// Authority side: returns the shards to alert for a possible handoff.
// Updates client_home, and suppresses the alert toward the shard just left.
fn compute_crossing_alerts(
    client_uuid: [u8; 16],
    x: f32,
    y: f32,
    client_home: &mut HashMap<[u8; 16], u32>,
    quad_tree: &quad_tree::QuadTree,
    margin: f32,
) -> Vec<u32> {
    let new_shard = match quad_tree.shard_for([x, y]) {
        Some(id) => id,
        None => return Vec::new(),
    };

    let old_shard = client_home.get(&client_uuid).copied();
    if old_shard != Some(new_shard) {
        client_home.insert(client_uuid, new_shard);
    }

    let mut alerts = Vec::new();
    for dest in quad_tree.shards_near([x, y], margin) {
        if dest != new_shard && Some(dest) != old_shard {
            alerts.push(dest);
        }
    }
    alerts
}

// Visibility side: computes the desired AOI topic set and diffs it against the
// client's current subscriptions. Returns the Unsubscribe/Subscribe changes and
// updates client_subs. Output is sorted so it is deterministic (testable).
fn compute_aoi_subscriptions(
    client_uuid: [u8; 16],
    x: f32,
    y: f32,
    client_subs: &mut HashMap<[u8; 16], HashSet<String>>,
    quad_tree: &quad_tree::QuadTree,
    near: f32,
    far: f32,
) -> Vec<SubAction> {
    // Out of the world: keep whatever the client already sees.
    if quad_tree.shard_for([x, y]).is_none() {
        return Vec::new();
    }

    let near_shards = quad_tree.shards_near([x, y], near);
    let far_shards = quad_tree.shards_near([x, y], far);

    let mut desired: HashSet<String> = HashSet::new();
    for s in &near_shards {
        desired.insert(format!("shard:{}", s));
    }
    for s in &far_shards {
        if !near_shards.contains(s) {
            desired.insert(format!("shard:{}:far", s));
        }
    }

    let current = client_subs.entry(client_uuid).or_default();

    let mut to_unsub: Vec<String> = current.difference(&desired).cloned().collect();
    let mut to_sub: Vec<String> = desired.difference(current).cloned().collect();
    to_unsub.sort();
    to_sub.sort();

    *current = desired;

    let mut actions = Vec::new();
    for topic in to_unsub {
        actions.push(SubAction::Unsubscribe(client_uuid, topic));
    }
    for topic in to_sub {
        actions.push(SubAction::Subscribe(client_uuid, topic));
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

    // Authority: alert neighbour shards so they can pre-warm a handoff.
    let alerts = compute_crossing_alerts(
        client_uuid, x, y, &mut state.client_home, quad_tree, BOUNDARY_MARGIN,
    );
    for dest in alerts {
        println!("CrossingAlert: client {:?} → dest shard:{}", &client_uuid[..4], dest);
        let mut buf = BytesMut::with_capacity(21);
        buf.put_u8(TAG_CROSSING_ALERT);
        buf.put_slice(&client_uuid);
        buf.put_u32_le(dest);
        // Send back to the shard that owns this entity.
        let _ = peer.send(&from, stream, buf.freeze());
    }

    // Visibility: update the client's AOI subscriptions at the broker.
    let subs = compute_aoi_subscriptions(
        client_uuid, x, y, &mut state.client_subs, quad_tree, AOI_NEAR, AOI_FAR,
    );
    for action in subs {
        match action {
            SubAction::Subscribe(uuid, topic) => {
                println!("Client {:?}: subscribe to {}", &uuid[..4], topic);
                send_to_broker(state, peer, stream, TAG_SUBSCRIBE, &uuid, &topic);
            }
            SubAction::Unsubscribe(uuid, topic) => {
                println!("Client {:?}: unsubscribe from {}", &uuid[..4], topic);
                send_to_broker(state, peer, stream, TAG_UNSUBSCRIBE, &uuid, &topic);
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
    topic: &str,
) {
    let broker_conn = match state.broker_conn {
        Some(c) => c,
        None => {
            eprintln!("Cannot send to broker: not connected");
            return;
        }
    };

    let mut buf = BytesMut::with_capacity(1 + 16 + 32);
    buf.put_u8(tag);
    buf.put_slice(client_uuid);
    buf.put_slice(&topic_to_bytes(topic));

    let _ = peer.send(&broker_conn, stream, buf.freeze());
}

fn topic_to_bytes(topic: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    let bytes = topic.as_bytes();
    let len = bytes.len().min(32);
    out[..len].copy_from_slice(&bytes[..len]);
    out
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

    // ── Crossing alerts (authority) ─────────────────────────────────────────────
    // compute_crossing_alerts returns the destination shards to pre-warm.

    fn cross(map: &mut HashMap<[u8; 16], u32>, u: [u8; 16], x: f32, y: f32) -> Vec<u32> {
        compute_crossing_alerts(u, x, y, map, &tree(), BOUNDARY_MARGIN)
    }

    #[test]
    fn first_entry_deep_no_alert() {
        let mut map = HashMap::new();
        let u = uuid(1);
        assert!(cross(&mut map, u, 250.0, 250.0).is_empty());
        assert_eq!(map[&u], 0); // home recorded
    }

    #[test]
    fn first_entry_near_boundary_alerts() {
        let mut map = HashMap::new();
        let u = uuid(2);
        assert_eq!(cross(&mut map, u, 460.0, 250.0), vec![1]);
        assert_eq!(map[&u], 0);
    }

    #[test]
    fn no_alert_when_staying_in_shard() {
        let mut map = HashMap::new();
        let u = uuid(3);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 300.0, 300.0).is_empty());
    }

    #[test]
    fn cross_vertical_updates_home_no_alert_when_deep() {
        let mut map = HashMap::new();
        let u = uuid(4);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 700.0, 250.0).is_empty());
        assert_eq!(map[&u], 1);
    }

    // Exact boundary point x=500 belongs to the right shard; no alert back to shard 0.
    #[test]
    fn cross_on_exact_vertical_boundary_line() {
        let mut map = HashMap::new();
        let u = uuid(6);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 500.0, 250.0).is_empty());
        assert_eq!(map[&u], 1);
    }

    #[test]
    fn cross_horizontal_updates_home() {
        let mut map = HashMap::new();
        let u = uuid(7);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 250.0, 700.0).is_empty());
        assert_eq!(map[&u], 2);
    }

    #[test]
    fn diagonal_jump_updates_home() {
        let mut map = HashMap::new();
        let u = uuid(9);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 750.0, 750.0).is_empty());
        assert_eq!(map[&u], 3);
    }

    // Step-by-step diagonal walk through the shared corner: 0→1→3.
    #[test]
    fn diagonal_step_by_step_through_corner() {
        let mut map = HashMap::new();
        let u = uuid(11);
        map.insert(u, 0);

        // Step 1: approaching corner from shard 0 — all 3 neighbours in range
        assert_eq!(cross(&mut map, u, 490.0, 490.0), vec![1, 2, 3]);
        assert_eq!(map[&u], 0); // no crossing yet

        // Step 2: cross into shard 1, still near corner — no alert toward shard 0
        assert_eq!(cross(&mut map, u, 510.0, 490.0), vec![2, 3]);
        assert_eq!(map[&u], 1);

        // Step 3: cross into shard 3 — no alert toward shard 1 (just left)
        assert_eq!(cross(&mut map, u, 510.0, 510.0), vec![0, 2]);
        assert_eq!(map[&u], 3);
    }

    #[test]
    fn alerts_near_corner_three_neighbours() {
        let mut map = HashMap::new();
        let u = uuid(14);
        map.insert(u, 0);
        assert_eq!(cross(&mut map, u, 490.0, 490.0), vec![1, 2, 3]);
    }

    #[test]
    fn no_alert_deep_in_shard() {
        let mut map = HashMap::new();
        let u = uuid(16);
        map.insert(u, 3);
        assert!(cross(&mut map, u, 750.0, 750.0).is_empty());
    }

    // Exactly at margin distance: strict < means no alert fires.
    #[test]
    fn no_alert_at_exact_margin_distance() {
        let mut map = HashMap::new();
        let u = uuid(17);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 450.0, 250.0).is_empty());
    }

    #[test]
    fn alert_just_inside_margin() {
        let mut map = HashMap::new();
        let u = uuid(18);
        map.insert(u, 0);
        assert_eq!(cross(&mut map, u, 451.0, 250.0), vec![1]);
    }

    // After crossing, no spurious alert back toward the old shard.
    #[test]
    fn no_spurious_alert_toward_previous_shard() {
        let mut map = HashMap::new();
        let u = uuid(19);
        map.insert(u, 0);
        assert!(cross(&mut map, u, 510.0, 250.0).is_empty());
        assert_eq!(map[&u], 1);
    }

    #[test]
    fn out_of_bounds_no_alert_keeps_home() {
        let mut map = HashMap::new();
        let u = uuid(20);
        map.insert(u, 0);
        assert!(cross(&mut map, u, -50.0, 250.0).is_empty());
        assert_eq!(map[&u], 0); // unchanged
    }

    #[test]
    fn out_of_bounds_new_player_not_recorded() {
        let mut map = HashMap::new();
        let u = uuid(21);
        assert!(cross(&mut map, u, 1500.0, 250.0).is_empty());
        assert!(!map.contains_key(&u));
    }

    #[test]
    fn two_players_independent_homes() {
        let mut map = HashMap::new();
        let a = uuid(30);
        let b = uuid(31);
        cross(&mut map, a, 250.0, 250.0); // a → shard 0
        cross(&mut map, b, 750.0, 750.0); // b → shard 3
        cross(&mut map, a, 700.0, 250.0); // a → shard 1
        assert_eq!(map[&a], 1);
        assert_eq!(map[&b], 3); // B unaffected
    }

    // ── AOI subscriptions (visibility) ──────────────────────────────────────────
    // compute_aoi_subscriptions returns the Unsubscribe/Subscribe diff (topics).

    fn aoi(subs: &mut HashMap<[u8; 16], HashSet<String>>, u: [u8; 16], x: f32, y: f32) -> Vec<SubAction> {
        compute_aoi_subscriptions(u, x, y, subs, &tree(), AOI_NEAR, AOI_FAR)
    }

    fn sub(u: [u8; 16], t: &str) -> SubAction { SubAction::Subscribe(u, t.to_string()) }
    fn unsub(u: [u8; 16], t: &str) -> SubAction { SubAction::Unsubscribe(u, t.to_string()) }

    // At a shard's centre: own shard near (20 Hz), the three others far (5 Hz).
    #[test]
    fn aoi_center_self_near_neighbours_far() {
        let mut subs = HashMap::new();
        let u = uuid(40);
        let a = aoi(&mut subs, u, 250.0, 250.0);
        assert_eq!(a, vec![
            sub(u, "shard:0"),
            sub(u, "shard:1:far"),
            sub(u, "shard:2:far"),
            sub(u, "shard:3:far"),
        ]);
    }

    // Re-computing the same position yields no change.
    #[test]
    fn aoi_no_change_when_stationary() {
        let mut subs = HashMap::new();
        let u = uuid(41);
        aoi(&mut subs, u, 250.0, 250.0);
        assert!(aoi(&mut subs, u, 250.0, 250.0).is_empty());
    }

    // Approaching the x=500 boundary promotes shard 1 from far to near.
    #[test]
    fn aoi_approach_boundary_promotes_far_to_near() {
        let mut subs = HashMap::new();
        let u = uuid(42);
        aoi(&mut subs, u, 250.0, 250.0); // center
        let a = aoi(&mut subs, u, 350.0, 250.0);
        assert_eq!(a, vec![
            unsub(u, "shard:1:far"),
            sub(u, "shard:1"),
        ]);
    }

    // Walking into a world corner drops the far neighbours entirely.
    #[test]
    fn aoi_corner_drops_far_neighbours() {
        let mut subs = HashMap::new();
        let u = uuid(43);
        aoi(&mut subs, u, 250.0, 250.0); // center: 0 near, 1/2/3 far
        let a = aoi(&mut subs, u, 50.0, 50.0);
        assert_eq!(a, vec![
            unsub(u, "shard:1:far"),
            unsub(u, "shard:2:far"),
            unsub(u, "shard:3:far"),
        ]);
    }

    // Out of bounds keeps the current subscriptions untouched.
    #[test]
    fn aoi_out_of_bounds_keeps_subs() {
        let mut subs = HashMap::new();
        let u = uuid(44);
        aoi(&mut subs, u, 250.0, 250.0);
        let before = subs[&u].clone();
        let a = aoi(&mut subs, u, -50.0, 250.0);
        assert!(a.is_empty());
        assert_eq!(subs[&u], before);
    }

    #[test]
    fn topic_encoding() {
        let t = topic_to_bytes("shard:0");
        assert_eq!(&t[..7], b"shard:0");
        assert_eq!(t[7], 0);

        let t = topic_to_bytes("shard:3:far");
        assert_eq!(&t[..11], b"shard:3:far");
        assert_eq!(t[11], 0);
    }
}

// Property-based invariants for the AOI logic over random in-bounds positions.
#[cfg(test)]
mod aoi_prop_tests {
    use super::*;
    use proptest::prelude::*;

    fn in_bounds() -> impl Strategy<Value = [f32; 2]> {
        (0.0f32..999.0, 0.0f32..999.0).prop_map(|(x, y)| [x, y])
    }

    proptest! {
        // The owning shard's near topic is always part of the AOI.
        #[test]
        fn owner_topic_always_subscribed(pos in in_bounds()) {
            let tree = quad_tree::build_default();
            let mut subs = HashMap::new();
            let u = [7u8; 16];
            compute_aoi_subscriptions(u, pos[0], pos[1], &mut subs, &tree, AOI_NEAR, AOI_FAR);
            let owner = tree.shard_for(pos).unwrap();
            let owner_topic = format!("shard:{}", owner);
            prop_assert!(subs[&u].contains(&owner_topic));
        }

        // A shard is never both near and far at once.
        #[test]
        fn near_and_far_are_disjoint(pos in in_bounds()) {
            let tree = quad_tree::build_default();
            let mut subs = HashMap::new();
            let u = [8u8; 16];
            compute_aoi_subscriptions(u, pos[0], pos[1], &mut subs, &tree, AOI_NEAR, AOI_FAR);
            for s in 0..4 {
                let near_topic = format!("shard:{}", s);
                let far_topic = format!("shard:{}:far", s);
                let near = subs[&u].contains(&near_topic);
                let far = subs[&u].contains(&far_topic);
                prop_assert!(!(near && far));
            }
        }

        // Recomputing the same position produces no further changes (stable).
        #[test]
        fn aoi_is_idempotent(pos in in_bounds()) {
            let tree = quad_tree::build_default();
            let mut subs = HashMap::new();
            let u = [9u8; 16];
            compute_aoi_subscriptions(u, pos[0], pos[1], &mut subs, &tree, AOI_NEAR, AOI_FAR);
            let second = compute_aoi_subscriptions(u, pos[0], pos[1], &mut subs, &tree, AOI_NEAR, AOI_FAR);
            prop_assert!(second.is_empty());
        }
    }
}
