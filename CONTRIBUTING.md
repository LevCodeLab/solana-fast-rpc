# contributing to solana-fast-rpc

namaste bhai. thanks for checking out the code instead of just opening an issue saying "pls fix".

this repo runs on pure Rust, 3am adrak chai, and zero external path hacks. keep it simple and fast.

## local setup in 3 steps

- install stable Rust (`rustup default stable`).
- run `cargo check --workspace --all-targets` to make sure nothing is broken.
- run `cargo test --workspace` and make sure every test stays green.

## code rules we actually care about

- **never mix `bank_id` across providers** - `bank_id` only makes sense inside one validator node. across different RPCs always match by `(slot, blockhash)`.
- **never guess forks** - first block seen is NOT always the winner. wait for confirmed blockhash or BLS cert before marking `Canonical`.
- **never delete dead forks** - mark dropped banks as `Abandoned` with a clear reason string so you can still debug what happened.
- **no floats in stake math** - use integer basis points (`10_000` = 100%, `8_000` = 80% fast finality) with `u128` math so nothing overflows.
- **keep hot paths network-free** - `SlotClock` and `LeaderEngine` must never make HTTP calls inside lookup methods.
- **no secrets or private URLs** - use generic names like `provider-primary` and dummy test hashes in all tests and examples.

## running the examples

- test the 200ms slot clock and leader handoff demo:

```bash
cargo run -p fast-rpc-clock --example slot_handoff_demo
```

- test the multi-bank fork graph and 80% BLS cert verifier demo:

```bash
cargo run -p fast-rpc-bank --example fork_resolver_demo
```

## sending a PR

- make a clean branch like `feat/your-thing` or `fix/your-bug`.
- add a unit test for whatever you changed (if it is not tested, only bhagwan knows if it works tomorrow).
- check `cargo clippy --workspace` if you feel extra disciplined.
- open the PR with short 1-line bullets explaining what changed and why.
