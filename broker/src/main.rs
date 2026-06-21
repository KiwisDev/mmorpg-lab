use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use game_sockets::{GameNetworkEvent, GamePeer, GameConnection, GameStream, GameStreamReliability};
use game_sockets::protocols::QuicBackend;
use tokio::sync::RwLock;
use bytes::{BytesMut, Buf, BufMut};
use uuid::Uuid;

const TAG_SUBSCRIBE: u8 = 0x01;
const TAG_UNSUBSCRIBE: u8 = 0x02;
const TAG_PUBLISH: u8 = 0x03;
const TAG_BROADCAST: u8 = 0x04;
const TAG_CLIENT_INPUT: u8 = 0x05;
const TAG_ROUTE_TO_SHARD: u8 = 0x06;

type Topic = [u8; 32];
type ClientId = Uuid;

#[derive(Default)]
pub struct BrokerState {
    // Topic -> Set of ClientIds subscribed
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    // ClientId -> Set of Topics they are subscribed to (for routing inputs)
    client_topics: HashMap<ClientId, HashSet<Topic>>,
    // Topic -> GameConnection (which Shard is authoritative/registered for this topic)
    shards: HashMap<Topic, GameConnection>,
    // ClientId -> actual QUIC GameConnection (transport assigns connection_id, not the UUID)
    client_connections: HashMap<ClientId, GameConnection>,
}

impl BrokerState {
    // Subscribe is issued by the spatial service, so it must NOT touch
    // client_connections — that mapping is owned by ClientInput (the client itself).
    pub fn subscribe(&mut self, topic: Topic, client_id: ClientId) {
        self.subscriptions.entry(topic).or_default().insert(client_id);
        self.client_topics.entry(client_id).or_default().insert(topic);
    }

    pub fn unsubscribe(&mut self, topic: Topic, client_id: ClientId) {
        if let Some(subs) = self.subscriptions.get_mut(&topic) {
            subs.remove(&client_id);
            if subs.is_empty() {
                self.subscriptions.remove(&topic);
            }
        }
        if let Some(topics) = self.client_topics.get_mut(&client_id) {
            topics.remove(&topic);
            if topics.is_empty() {
                self.client_topics.remove(&client_id);
                self.client_connections.remove(&client_id);
            }
        }
    }
}

pub struct PubSubBroker {
    socket: tokio::sync::Mutex<GamePeer>,
    state: Arc<RwLock<BrokerState>>
}

impl PubSubBroker {
    pub async fn new(bind_addr: &str, bind_port: u16) -> anyhow::Result<Self> {
        let socket = GamePeer::new(QuicBackend::new());
        socket.listen(bind_addr, bind_port).expect("Failed to bind Quic socket");
        Ok(Self { 
            socket: tokio::sync::Mutex::new(socket), 
            state: Arc::new(RwLock::new(BrokerState::default())) 
        })
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // We use a default stream for pub/sub messaging
        let default_stream = GameStream::new(0, GameStreamReliability::Unreliable);
        
        loop {
            // Check for new network events
            let event = {
                let mut socket = self.socket.lock().await;
                socket.poll()?
            };

            if let Some(event) = event {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        println!("Connected: {}", connection.connection_id);
                    }
                    GameNetworkEvent::Disconnected(connection) => {
                        println!("Disconnected: {}", connection.connection_id);
                        let mut state = self.state.write().await;

                        // Find which client owned this connection (if any).
                        let client_id = state.client_connections.iter()
                            .find(|(_, conn)| **conn == connection)
                            .map(|(id, _)| *id);

                        if let Some(client_id) = client_id {
                            if let Some(topics) = state.client_topics.remove(&client_id) {
                                // Tell every shard this client was on that it left.
                                let mut input = [0u8; 16];
                                input[0] = 0x03; // LEAVE
                                let mut leave = BytesMut::with_capacity(1 + 16 + 16);
                                leave.put_u8(TAG_CLIENT_INPUT);
                                leave.put_slice(client_id.as_bytes());
                                leave.put_slice(&input);
                                let leave_bytes = leave.freeze();

                                let socket = self.socket.lock().await;
                                for topic in &topics {
                                    if let Some(subs) = state.subscriptions.get_mut(topic) {
                                        subs.remove(&client_id);
                                        if subs.is_empty() {
                                            state.subscriptions.remove(topic);
                                        }
                                    }
                                    if let Some(shard_conn) = state.shards.get(topic) {
                                        let _ = socket.send(shard_conn, &default_stream, leave_bytes.clone());
                                    }
                                }
                            }
                            state.client_connections.remove(&client_id);
                            println!("Cleaned up client {} after disconnect", client_id);
                        }
                    }
                    GameNetworkEvent::Message { mut data, connection, .. } => {
                        if data.is_empty() { continue; }

                        let tag = data.get_u8();
                        match tag {
                            TAG_SUBSCRIBE => {
                                if data.remaining() >= 16 + 32 {
                                    let mut client_id_bytes = [0u8; 16];
                                    data.copy_to_slice(&mut client_id_bytes);
                                    let client_id = Uuid::from_bytes(client_id_bytes);
                                    
                                    let mut topic = [0u8; 32];
                                    data.copy_to_slice(&mut topic);
                                    
                                    println!("Subscribe: client {} to topic {:?}", client_id, topic);
                                    let mut state = self.state.write().await;
                                    state.subscribe(topic, client_id);
                                }
                            }
                            TAG_UNSUBSCRIBE => {
                                if data.remaining() >= 16 + 32 {
                                    let mut client_id_bytes = [0u8; 16];
                                    data.copy_to_slice(&mut client_id_bytes);
                                    let client_id = Uuid::from_bytes(client_id_bytes);
                                    
                                    let mut topic = [0u8; 32];
                                    data.copy_to_slice(&mut topic);
                                    
                                    println!("Unsubscribe: client {} from topic {:?}", client_id, topic);
                                    let mut state = self.state.write().await;
                                    state.unsubscribe(topic, client_id);
                                }
                            }
                            TAG_PUBLISH => {
                                if data.remaining() >= 32 + 2 {
                                    let mut topic = [0u8; 32];
                                    data.copy_to_slice(&mut topic);
                                    
                                    let payload_len = data.get_u16_le();
                                    if data.remaining() >= payload_len as usize {
                                        let payload = data.copy_to_bytes(payload_len as usize);
                                        
                                        let mut state = self.state.write().await;
                                        // Register this connection as the shard for this topic
                                        state.shards.insert(topic, connection);

                                        // Send Broadcast to all subscribed clients
                                        if let Some(subscribers) = state.subscriptions.get(&topic) {
                                            // Prepare broadcast packet
                                            let mut out_buf = BytesMut::with_capacity(1 + 2 + payload.len());
                                            out_buf.put_u8(TAG_BROADCAST);
                                            out_buf.put_u16_le(payload_len);
                                            out_buf.put(payload);
                                            let out_bytes = out_buf.freeze();

                                            let socket = self.socket.lock().await;
                                            for &sub_id in subscribers {
                                                if let Some(client_conn) = state.client_connections.get(&sub_id) {
                                                    let _ = socket.send(client_conn, &default_stream, out_bytes.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            TAG_CLIENT_INPUT => {
                                if data.remaining() >= 16 + 16 {
                                    let mut client_id_bytes = [0u8; 16];
                                    data.copy_to_slice(&mut client_id_bytes);
                                    let client_id = Uuid::from_bytes(client_id_bytes);
                                    
                                    let mut input = [0u8; 16];
                                    data.copy_to_slice(&mut input);

                                    let mut state = self.state.write().await;
                                    // The client is the only sender of ClientInput: record its real
                                    // connection here so Broadcasts reach it (Subscribes come from spatial).
                                    state.client_connections.insert(client_id, connection);

                                    // Forward ClientInput to all shards the client is subscribed to
                                    if let Some(topics) = state.client_topics.get(&client_id) {
                                        let mut out_buf = BytesMut::with_capacity(1 + 16 + 16);
                                        out_buf.put_u8(TAG_CLIENT_INPUT);
                                        out_buf.put_slice(&client_id_bytes);
                                        out_buf.put_slice(&input);
                                        let out_bytes = out_buf.freeze();

                                        let socket = self.socket.lock().await;
                                        for topic in topics {
                                            if let Some(shard_conn) = state.shards.get(topic) {
                                                let _ = socket.send(shard_conn, &default_stream, out_bytes.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            TAG_ROUTE_TO_SHARD => {
                                // Inter-shard message: dest_topic[32] + inner_payload.
                                // Forward the inner payload verbatim to the shard owning that topic.
                                if data.remaining() >= 32 {
                                    let mut topic = [0u8; 32];
                                    data.copy_to_slice(&mut topic);
                                    let inner = data.copy_to_bytes(data.remaining());

                                    let state = self.state.read().await;
                                    if let Some(shard_conn) = state.shards.get(&topic) {
                                        let socket = self.socket.lock().await;
                                        let _ = socket.send(shard_conn, &default_stream, inner);
                                    } else {
                                        println!("RouteToShard: no shard registered for topic {:?}", &topic[..8]);
                                    }
                                }
                            }
                            _ => {
                                println!("Unknown tag: {}", tag);
                            }
                        }
                    }
                    _ => {}
                }
            } else {
                // Sleep slightly to avoid busy-looping if poll() returns None
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port: u16 = std::env::var("BROKER_PORT")
        .unwrap_or("9010".to_string())
        .parse()
        .expect("BROKER_PORT must be a valid port number");
    println!("Starting Broker on port {}...", port);
    let broker = PubSubBroker::new("0.0.0.0", port).await?;
    broker.run().await?;
    Ok(())
}