# mmorpg-lab

Implémentation d'une architecture de flotte de serveurs de jeu en Rust, dans le cadre du cours de Programmation réseau pour jeux.

## Architecture

```
CLIENT ──POST /login──► GATEKEEPER (Axum REST, :3000)
                              │ cherche un serveur disponible
                              ▼
                           REDIS (registre des serveurs, clés TTL)
                              ▲
                          ORCHESTRATOR (écoute QUIC, :9000)
                              ▲ heartbeat QUIC toutes les 5s
                         DEDICATED SERVER(S) (Bevy ECS + game_sockets)
```

**Composants :**

- **`shared`** — types sérialisables communs (`Heartbeat`, `ServerInfo`)
- **`dedicated_server`** — serveur de jeu Bevy, écoute les joueurs en QUIC, envoie des heartbeats à l'orchestrateur
- **`orchestrator`** — reçoit les heartbeats, met à jour Redis, spawne des serveurs si la flotte est insuffisante
- **`gatekeeper`** — API REST Axum, authentifie les joueurs et retourne les coordonnées d'un serveur disponible

**Communication :**
- Dedicated server → Orchestrator : heartbeat JSON via QUIC (non-fiable) toutes les 5 secondes
- Orchestrator → Redis : `HSET server:<uuid>` + `EXPIRE` (TTL 15s) à chaque heartbeat
- Client → Gatekeeper : `POST /login` (HTTP)
- Gatekeeper → Redis : `KEYS server:*` + `HMGET status/ip/port/zone`

La détection de serveurs morts repose sur le TTL Redis : si un serveur ne renouvelle pas son heartbeat pendant 15 secondes, sa clé expire automatiquement.

## Démarrage

**Prérequis :** Rust, Docker

```bash
./start.sh
```

Le script :
1. Compile les trois binaires
2. Démarre Redis via Docker
3. Lance l'orchestrateur (qui spawne automatiquement un `dedicated_server`)
4. Lance le gatekeeper

## Test bout-en-bout

Attendre ~10 secondes que le dedicated server envoie son premier heartbeat, puis :

```bash
# Vérifier que le serveur est enregistré dans Redis
redis-cli KEYS "server:*"
redis-cli HGETALL server:<uuid>

# Tester le login
curl -X POST http://localhost:3000/login \
  -H "Content-Type: application/json" \
  -d '{"username": "alice", "password": "1234"}'
```

Réponse attendue :
```json
{
  "player_id": "87630b5b-b581-44ff-a9e8-b7d05a9017ed",
  "server": {
    "ip": "0.0.0.0",
    "port": 36322,
    "zone": "zone_A"
  }
}
```

### Logs

```
> sh ./start.sh
==> Starting Redis...
redis-mmorpg
Redis ready.
==> Starting Orchestrator (background)...
Orchestrator listening on port 9000
[scaler] Available servers: 0 (min: 1)
[scaler] Spawned dedicated_server on port 50657
[INFO] Dedicated Game Server starting...
2026-05-18T23:29:04.109519Z  INFO dedicated_server: [INFO] Listening on: 0.0.0.0:50657
2026-05-18T23:29:04.109621Z  INFO dedicated_server: [INFO] Connecting to orchestrator at 127.0.0.1:9000
Server connected: 46aa4f2c-880f-42d5-8613-a451dc64e741
2026-05-18T23:29:04.126860Z  INFO dedicated_server: [INFO] Orchestrator connected: GameConnection { connection_id: 42144659-a6e0-4a92-8584-eeb9d9696f60 }
==> Starting Gatekeeper (background)...

All services are running.
  Orchestrator PID : 162665
  Gatekeeper PID   : 162773

Test the login endpoint:
  curl -X POST http://localhost:3000/login \
    -H 'Content-Type: application/json' \
    -d '{"username": "alice", "password": "1234"}'

Press Ctrl+C to stop.
Gatekeeper listening on 0.0.0.0:3000
2026-05-18T23:29:09.123375Z  INFO dedicated_server: [INFO] Heartbeat sent - Players: 0/10, Status: available
Heartbeat from server 309161f7-8311-481c-8e8c-e2845d7c2bbf (zone: zone_A, players: 0/10)

> curl -X POST http://localhost:3000/login \
        -H "Content-Type: application/json" \
        -d '{"username": "alice", "password": "1234"}'
{"player_id":"35652272-5ca5-4b68-ba27-7e8671e1ec47","server":{"ip":"127.0.0.1","port":50657,"zone":"zone_A"}}⏎
```

## Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `ORCH_PORT` | `9000` | Port QUIC de l'orchestrateur |
| `ORCH_ADDR` | `127.0.0.1:9000` | Adresse de l'orchestrateur (pour les dedicated servers) |
| `HOT_SERVERS_MIN` | `1` | Nombre minimum de serveurs disponibles |
| `REDIS_URL` | `redis://127.0.0.1:6379` | URL de connexion Redis |
| `DS_PORT` | `9000` | Port QUIC du dedicated server |
| `DS_ZONE` | `zone_A` | Zone de jeu du dedicated server |
| `DS_MAX_PLAYERS` | `10` | Capacité maximale du dedicated server |