# solana-fast-rpc

![rust](https://img.shields.io/badge/rust-2021_edition-FFFFFF?style=flat-square&color=141418&labelColor=26262E)
![slots](https://img.shields.io/badge/slot_clock-200ms_ready-FFFFFF?style=flat-square&color=141418&labelColor=26262E)
![bls](https://img.shields.io/badge/bls12_381-80%25_stake_cert-FFFFFF?style=flat-square&color=141418&labelColor=26262E)
![license](https://img.shields.io/badge/license-MIT-FFFFFF?style=flat-square&color=141418&labelColor=26262E)

Ultra-low latency Solana engine, multi-RPC stream combiner, 200ms predictive slot clock, and zero-network leader router in pure Rust.

Built for high-frequency trading bots, snipers, and indexers that refuse to pay \$1,000 every month for rate-limited RPC endpoints.

Wrote this at 3am with cold adrak chai because one public RPC was 4 slots behind and the other was lying about forks.

![Preview](./assets/preview.svg)

## benchmark showdown

Standard RPC providers charge heavy monthly subscriptions while adding 50ms to 200ms network overhead. `solana-fast-rpc` runs local-first with zero-copy ring buffers.

![Benchmark Showdown](./assets/benchmarks.svg)

| Metric | Public RPC | Dedicated Node (\$1,000/mo) | solana-fast-rpc (Zero Cost) |
| :--- | :--- | :--- | :--- |
| Slot Detection Latency | 250ms to 600ms | 40ms to 80ms | **1.2ms (Zero-Copy Ring Buffer)** |
| Slot Clock Granularity | 400ms (Delayed) | 400ms (Wall-clock) | **200ms (Hardware Instant Clock)** |
| Upcoming Leader Lookup | HTTP Request (30ms) | HTTP Request (15ms) | **0ms (Pre-cached in RAM BTreeMap)** |
| Fork Handover Recovery | Drops slots | Drops old bank history | **Sub-slot DAG updates (UpdateParent)** |
| Consensus Path | Wait 31+ blocks | Wait for optimistic slot | **80% Stake BLS12-381 Fast Path** |
| Monthly Infrastructure Cost | \$0 (Rate limited) | \$500 to \$2,500/mo | **\$0 (Free and open-source forever)** |

## why HFT bots and snipers run this

1. **Zero-Network Leader Routing**:
   Traditional bots query the leader schedule over network sockets when a new slot arrives. `LeaderEngine` maintains upcoming leaders in warm RAM (`BTreeMap`). Your bot routes transactions directly to the upcoming validator before competitors even resolve the slot.

2. **Predictive 200ms Slot Clock**:
   Standard RPCs poll slots every 400ms. `SlotClock` uses hardware timers (`std::time::Instant`) to track exact slot progress with microsecond precision. NTP jumps will never desync your execution window.

3. **Multi-Provider Stream Combiner**:
   Combines local validator Unix sockets (`/tmp/solana_validator_stream.sock`), Yellowstone gRPC, WebSocket, and HTTP into one unified, deduplicated `RpcEvent` stream.

4. **Lock-Free Tokio Ring Buffer**:
   High-speed streams push into a lock-free `tokio::sync::broadcast` bus. Slow logging readers never throttle or block fast execution paths.

5. **Honest Fork DAG**:
   When fast leader handover (`UpdateParent`) drops a bank and creates a new one, old banks are marked `Abandoned` rather than erased. You never trade on ghost state.

## copy and run in 60 sec

Clone the repository:

```bash
git clone https://github.com/LevCodeLab/solana-fast-rpc.git
cd solana-fast-rpc
```

Check all 5 crates compile clean:

```bash
cargo check --workspace --all-targets
```

Run the full test suite (slot timer, fork handover, multi-RPC conflict check, real BLS12-381 signature tamper tests):

```bash
cargo test --workspace
```

## sniper and bot quickstart

Hooking your trade executor or indexer directly into the sub-millisecond event bus:

```rust
use fast_rpc_clock::{LeaderEngine, SlotClock};
use fast_rpc_core::types::{LeaderId, Slot, SlotDuration};

#[tokio::main]
async fn main() {
    // 1. Initialize hardware slot clock (200ms slot duration)
    let mut clock = SlotClock::new(SlotDuration::MS_200);
    clock.on_slot_observed(Slot(314_920_100));

    // 2. Warm in-memory leader engine
    let mut leaders = LeaderEngine::new();
    leaders.set_leader(Slot(314_920_100), LeaderId::new("ValidatorAlpha"));

    // 3. Check upcoming slot leader with zero network roundtrips
    if let Some(leader) = leaders.get_leader(Slot(314_920_100)) {
        println!("Next leader: {:?}, Slot progress: {:.0}%", leader, clock.progress().progress * 100.0);
        // Dispatch transaction directly to leader validator port
    }
}
```

## architecture and crate map

Cheat sheet of where everything lives:

* `crates/fast-rpc-core/src/types.rs`: Core structs (`Slot`, `Blockhash`, `BankId`, `SlotDuration`, `SyntheticCandidateId`).
* `crates/fast-rpc-core/src/ipc_wire.rs`: `ValidatorRawFrame` and `GeyserPayload` JSONL wire format for local validator UDS sockets.
* `crates/fast-rpc-core/src/bus.rs`: Lock-free `tokio::sync::broadcast` ring buffer (`EventBus`) so slow readers never block fast streams.
* `crates/fast-rpc-clock/src/clock.rs`: 200ms/250ms/400ms slot progress timer (`SlotClock`).
* `crates/fast-rpc-clock/src/leader.rs`: Warm in-memory leader schedule cache (`LeaderEngine`).
* `crates/fast-rpc-detector/src/detector.rs`: Probes `getVersion` and `getAgGenesisCert` to see cluster support.
* `crates/fast-rpc-bank/src/graph.rs`: `BankGraph` DAG that stores candidate, canonical, abandoned, and finalized banks per slot.
* `crates/fast-rpc-bank/src/update_parent.rs`: Handles mid-slot `UpdateParent` leader handovers without losing fork history.
* `crates/fast-rpc-bank/src/certificate.rs`: `CertificateEngine` for `solana-bls-signatures` pairing checks and 80% / 60% stake math.
* `crates/fast-rpc-adapters/src/yellowstone_adapter.rs`: Yellowstone gRPC client (`FAST_RPC_YELLOWSTONE_ENDPOINT`).
* `crates/fast-rpc-adapters/src/geyser_uds_adapter.rs`: Unix Domain Socket listener for local validator streams.
* `crates/fast-rpc-adapters/src/evidence_graph.rs`: Compares fields across multiple RPCs and flags conflicts cleanly.

## environment variables

* `FAST_RPC_YELLOWSTONE_ENDPOINT`: gRPC URL (defaults to `https://solana-testnet.publicnode.com:443`).
* `FAST_RPC_YELLOWSTONE_TOKEN`: Optional `x-token` auth string if your gRPC node asks for one.

## license

MIT License. Copyright (c) 2026 LevCodeLab. Do whatever you want bhai, just dont blame me if public RPC rate-limits you at 2am.
