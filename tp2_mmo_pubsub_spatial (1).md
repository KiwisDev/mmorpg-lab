---
title: "TP2 — MMO : PubSub, Service Spatial et Autorité Flexible"
subtitle: "Programmation Réseau Avancée pour Jeux"
author: "UQAC"
date: "2026"
lang: fr
---

# TP2 — MMO : PubSub, Service Spatial et Autorité Flexible

| | |
|---|---|
| **Cours** | Programmation Réseau Avancée pour Jeux |
| **Remise** | Dans 2 semaines |
| **Technologie** | Bevy (Rust) — obligatoire côté serveur |
| **Transport** | `game_sockets` (fourni) — QUIC, datagrams non fiables |

> **Objectif :** Étendre votre serveur MMO avec un **broker PubSub** (point d'entrée unique pour les clients), un **service spatial** (Quad Tree) qui orchestre les abonnements, et un mécanisme d'**autorité flexible** permettant à une entité de migrer entre shards.

---

## 1. Architecture cible

Le broker est le **seul point de contact** des clients. Ils ne connaissent pas l'existence des shards. Les shards publient l'état du monde sur le broker ; le service spatial décide quels clients reçoivent quoi en gérant leurs abonnements. Les shards publient sur des topics correspondant à leur identifiant (ex. `shard:0`, `shard:1`). Le service spatial abonne chaque client au topic du shard qui couvre sa position actuelle.

---

## 2. Partie 1 — Broker PubSub (25 points)

Le broker est un processus Bevy indépendant. C'est le **seul serveur** auquel les clients se connectent via `game_sockets`.

**Protocole de messages (binaire, little-endian) :**

| Tag (u8) | Émetteur | Message | Champs |
|---|---|---|---|
| `0x01` | service spatial | `Subscribe` | `client_id: u32`, `topic: [u8; 32]` |
| `0x02` | service spatial | `Unsubscribe` | `client_id: u32`, `topic: [u8; 32]` |
| `0x03` | shard | `Publish` | `topic: [u8; 32]`, `payload_len: u16`, `payload: [u8]` |
| `0x04` | broker → client | `Broadcast` | `payload_len: u16`, `payload: [u8]` |
| `0x05` | client | `ClientInput` | `client_id: u32`, `input: [u8; 16]` (transféré au shard approprié) |

**À remettre :**
- [ ] Les clients se connectent au broker et reçoivent des `Broadcast`
- [ ] `Subscribe`/`Unsubscribe` mettent à jour la map correctement
- [ ] Les inputs clients sont relayés au bon shard

---

## 3. Partie 2 — Service Spatial avec Quad Tree (30 points)

Le service spatial maintient un Quad Tree du monde et traduit les positions en abonnements PubSub. Chaque feuille du Quad Tree est associée à un shard. Le topic est l'**identifiant du shard** responsable de cette feuille (ex. `shard:0`), pas la position géographique — un client garde le même topic tant qu'il reste dans la région du même shard.

```rust
pub struct QuadTree {
    bounds: Rect,
    depth: u8,
    max_depth: u8,
    children: Option<Box<[QuadTree; 4]>>,
    shard_id: Option<u32>,  // défini uniquement sur les feuilles
}

impl QuadTree {
    /// Retourne le shard_id de la feuille contenant `pos`.
    pub fn shard_for(&self, pos: Vec2) -> Option<u32> { ... }

    /// Retourne les shard_ids distincts dans un rayon `margin` autour de `pos`.
    /// Utilisé pour détecter l'approche d'une frontière inter-shard.
    pub fn shards_near(&self, pos: Vec2, margin: f32) -> Vec<u32> { ... }
}
```

**Comportement sur `PositionUpdate`** (envoyé par les shards, tag `0x10`) :

```
Tag (u8) | client_id: u32 | x: f32 | y: f32
```

1. Calculer le nouveau `shard_id` via `shard_for(pos)`
2. Si le shard a changé : envoyer `Unsubscribe(ancien topic)` puis `Subscribe(nouveau topic)` au broker
3. Si `shards_near(pos, margin)` retourne plusieurs shards : émettre un `CrossingAlert` (voir Partie 3)

---

## 4. Partie 3 — Autorité Flexible (35 points)

Quand une entité approche d'une frontière inter-shard, la simulation migre du shard source vers le shard destination sans déconnexion du client.

**États d'une entité :**

| État | Signification |
|---|---|
| `Owned` | Simulée normalement par ce shard |
| `PendingHandoff` | Dans la marge — transfert en cours vers le shard voisin |
| `Ghost` | Simulée par le voisin — copie lecture seule locale |

**Protocole inter-shards :**

| Tag (u8) | Message | Champs |
|---|---|---|
| `0x20` | `HandoffRequest` | `entity_id: u32`, `pos: Vec2`, `vel: Vec2`, `state: [u8; 64]` |
| `0x21` | `HandoffAccept` | `entity_id: u32` |
| `0x22` | `HandoffReject` | `entity_id: u32` |
| `0x23` | `GhostUpdate` | `entity_id: u32`, `pos: Vec2`, `vel: Vec2` |
| `0x24` | `HandoffComplete` | `entity_id: u32` |

Le transfert est déclenché par le `CrossingAlert` du service spatial. Le shard destination spawn l'entité en état `Ghost` dès `HandoffAccept`, reçoit les `GhostUpdate` à chaque tick, puis prend l'autorité complète à `HandoffComplete`. Si `HandoffReject`, l'entité rebondit sur la frontière.

Pendant toute la transition, les deux shards publient les positions de l'entité sur le broker — le client ne voit pas d'interruption.
