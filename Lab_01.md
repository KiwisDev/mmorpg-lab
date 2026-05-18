# Lab – MMORPG Server Architecture : du serveur dédié à la flotte

**Cours :** Programmation réseau pour jeux 
**Langage principal :** Rust  
**Dépendances clés :** Bevy 0.18, `game_sockets`, Redis, Axum/Rocket

---

## Mise en contexte

Les premiers MMORPGs comme *Ultima Online* (1997) ou *EverQuest* (1999) ont été les premiers à devoir résoudre un problème fondamental : comment faire jouer des milliers de joueurs simultanément sur un monde persistant ? La réponse architecturale adoptée à l'époque, et encore largement utilisée aujourd'hui, repose sur une **flotte de serveurs dédiés** coordonnés par une infrastructure d'orchestration.

Dans ce laboratoire, vous allez implémenter une version simplifiée de cette architecture en quatre composants distincts qui communiquent entre eux.

---

## Architecture cible

```
┌──────────────────────────────────────────────────────┐
│                     CLIENT (fictif)                  │
│   1. POST /login  ──────────────────────────────┐    │
│   2. Reçoit { ip, port }                        │    │
│   3. Connexion UDP directe au Dedicated Server  │    │
└─────────────────────────────────────────────────┼────┘
                                                  │
          ┌───────────────────────────────────────▼──────┐
          │       GATEKEEPER (Axum/Rocket REST API)      │
          │  POST /login   →  vérifie crédentials (dummy)│
          │                →  interroge Redis            │
          │                →  retourne { ip, port, zone }│
          └───────────────────────────┬──────────────────┘
                                      │ GET server disponible
                          ┌───────────▼───────────┐
                          │        REDIS          │
                          │  clé : server:<id>    │
                          │  valeur : JSON        │
                          │  { ip, port, zone,    │
                          │    status, players }  │
                          └───────────▲───────────┘
                                      │ HSET / EXPIRE
                          ┌───────────┴───────────┐
                          │      ORCHESTRATOR     │
                          │  - Spawn les DS       │
                          │  - Maintient N "hot"  │
                          │  - Heartbeat polling  │
                          └───────────▲───────────┘
                                      │ heartbeat UDP
                          ┌───────────┴───────────┐
                          │   DEDICATED SERVER(S) │
                          │   Bevy + game_sockets │
                          │   Envoie heartbeat    │
                          │   Accepte joueurs UDP │
                          └───────────────────────┘
```

---

## Composant 1 – Dedicated Game Server (Bevy + game_sockets)

### Objectif

Implémenter un serveur de jeu minimaliste capable d'accepter des connexions de joueurs et d'envoyer un heartbeat périodique à l'orchestrateur.

### Comportement attendu

- Écoute sur un port UDP configurable (variable d'environnement `DS_PORT`).
- Accepte des joueurs qui envoient un message `JOIN { username }`.
- Répond avec `WELCOME { player_id }`.
- Envoie toutes les **5 secondes** un heartbeat UDP à l'orchestrateur : `HEARTBEAT { id, ip, port, zone, player_count }`.
- Se déclare `FULL` dans son heartbeat si `player_count >= MAX_PLAYERS`.

### Structure Bevy suggérée

```rust
// main.rs
fn main() {
    App::new()
        .add_plugins(MinimalPlugins)
        .insert_resource(ServerConfig::from_env())
        .add_systems(Startup, bind_socket)
        .add_systems(Update, (receive_packets, send_heartbeat).chain())
        .run();
}
```

```rust
// resources.rs
#[derive(Resource)]
pub struct ServerConfig {
    pub id: String,          // UUID généré au démarrage
    pub port: u16,
    pub zone: String,        // ex: "zone_A"
    pub max_players: usize,
    pub orchestrator_addr: SocketAddr,
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<SocketAddr, PlayerInfo>,
}
```

### Points à implémenter

- [ ] `bind_socket` : ouvre le socket UDP via `game_sockets`
- [ ] `receive_packets` : lit les paquets entrants, dispatche `JOIN`
- [ ] `send_heartbeat` : timer 5 s, sérialise et envoie le heartbeat

---

## Composant 2 – Orchestrateur

### Objectif

Maintenir une flotte de serveurs dédiés. L'orchestrateur écoute les heartbeats UDP, met à jour Redis, et s'assure qu'un minimum de serveurs vides (`HOT_SERVERS_MIN`) sont toujours disponibles.

### Comportement attendu

- Écoute sur un port UDP configurable (`ORCH_PORT`).
- À chaque heartbeat reçu, met à jour la clé Redis `server:<id>` avec un TTL de X secondes.
  - Si pas de heartbeat reçu pendant X s → Redis expire la clé automatiquement.
- Vérifie toutes les Y secondes le nombre de serveurs avec `status: available`.
  - Si ce nombre est inférieur à `HOT_SERVERS_MIN` → spawn un nouveau processus `dedicated_server` avec un port libre.

### Points à implémenter

- [ ] Tâche async `heartbeat_listener` (tokio UDP socket)
- [ ] Mise à jour Redis avec `redis-rs` (commandes `HSET` + `EXPIRE`)
- [ ] Tâche async `scaler_loop` avec `tokio::time::interval`
- [ ] `count_available_servers` : `SCAN` + `HGET status`
- [ ] `spawn_server`

---

## Composant 3 – Redis

### Objectif

Redis joue le rôle de **registre partagé** entre l'orchestrateur et le Gatekeeper. Vous n'implémentez pas Redis vous-mêmes : vous le configurez et vous comprenez son rôle.

### Démarrage

```bash
# Via Docker
docker run -d --name redis-mmorpg -p 6379:6379 redis:7-alpine
```

### Commandes utiles à connaître

```bash
redis-cli KEYS "server:*"               # lister tous les serveurs
redis-cli HGETALL server:<uuid>         # inspecter un serveur
redis-cli TTL server:<uuid>             # vérifier le TTL restant
redis-cli MONITOR                       # observer les commandes en temps réel
```

### Questions de réflexion

1. Pourquoi utilise-t-on un TTL plutôt qu'une suppression explicite pour détecter les serveurs morts ?
2. Quelle est la différence entre `HSET` et `SET` dans ce contexte ?
3. Que se passe-t-il si l'orchestrateur redémarre ? Les clés sont-elles perdues ?

---

## Composant 4 – Gatekeeper (Axum/Rocket REST API)

### Objectif

Exposer une API REST simple. Le Gatekeeper est le point d'entrée unique du client : il authentifie (de façon fictive) et retourne les coordonnées d'un serveur disponible.

### Endpoints

#### `POST /login`

**Corps de la requête :**
```json
{ "username": "valere", "password": "1234" }
```

**Réponse succès (200) :**
```json
{
  "player_id": "uuid-généré",
  "server": {
    "ip": "127.0.0.1",
    "port": 7001,
    "zone": "zone_A"
  }
}
```

**Réponse erreur – aucun serveur disponible (503) :**
```json
{ "error": "No server available" }
```

**Authentification :** accepter n'importe quel `username` non vide avec le mot de passe `"1234"`.

#### `GET /health`

Retourne `{ "status": "ok" }` — utile pour vérifier que le service tourne.

### Structure suggérée

```
gatekeeper/
├── src/
│   ├── main.rs        // Axum router, état partagé
│   ├── handlers.rs    // login_handler, health_handler
│   └── redis_pool.rs  // connexion Redis, find_available_server()
```

### Points à implémenter

- [ ] Router Axum/Rocket avec état partagé (`Arc<AppState>`)
- [ ] `login_handler` : valide les crédentiels, appelle `find_available_server`
- [ ] Retourner les bons codes HTTP (200, 401, 503)

---

## Intégration et test de bout en bout

Une fois les quatre composants opérationnels, testez le flux complet :

```bash
docker run -d --name redis-mmorpg -p 6379:6379 redis:7-alpine

cargo run -p orchestrator

cargo run -p gatekeeper

curl -X POST http://localhost:3000/login \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "password": "1234"}'

redis-cli KEYS "server:*"
redis-cli HGETALL server:<uuid-retourné>
```

Le test est réussi si :
- La réponse du Gatekeeper contient un `ip` et un `port` valides.
- Le TTL du serveur dans Redis se renouvelle toutes les 5 secondes grâce aux heartbeats.
- Tuer un `dedicated_server` fait disparaître sa clé Redis après 15 secondes, et l'orchestrateur en spawne un nouveau.

---

## Structure du dépôt suggérée (Cargo workspace)

```
mmorpg-lab/
├── Cargo.toml              # [workspace] members = [...]
├── dedicated_server/
│   └── src/main.rs
├── orchestrator/
│   └── src/main.rs
├── gatekeeper/
│   └── src/main.rs
├── shared/                 # types communs (Heartbeat, ServerInfo...)
│   └── src/lib.rs
└── docker-compose.yml      # Redis + optionnellement les services
```

**`shared/src/lib.rs`** contiendra les structures partagées sérialisables :

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}
```

---

## Dépendances Cargo suggérées

| Crate | Usage |
|---|---|
| `bevy` 0.18 | Moteur ECS pour le Dedicated Server |
| `game_sockets` | Abstraction réseau (voir cours) |
| `tokio` (full) | Runtime async pour orchestrateur et gatekeeper |
| `axum` | Framework REST pour le Gatekeeper |
| `rocket` | Framework REST pour le Gatekeeper |
| `redis` | Client Redis (orchestrateur et gatekeeper) |
| `deadpool-redis` | Pool de connexions Redis pour Axum |
| `serde` + `serde_json` | Sérialisation JSON |
| `uuid` | Génération d'identifiants uniques |
| `tracing` + `tracing-subscriber` | Logs structurés |
| `anyhow` + `thiserror` | Logs structurés |

---

## Remise

- Dépôt Git (un par équipe de 2/3)
- `README.md` décrivant comment démarrer l'ensemble en une commande (`docker-compose up` ou script shell)
- Captures d'écran ou logs du test bout en bout

**Date de remise :** lundi 18 mai