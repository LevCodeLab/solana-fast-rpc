use fast_rpc_clock::{LeaderEngine, SlotClock};
use fast_rpc_core::types::{LeaderId, Slot, SlotDuration};
use std::time::{Duration, Instant};

fn main() {
    println!("=== solana-fast-rpc // 200ms slot clock & leader handoff demo ===");

    let mut clock = SlotClock::new(SlotDuration::MS_200);
    let mut leaders = LeaderEngine::new();

    let start_slot = Slot(314_920_100);
    leaders.populate_schedule(
        start_slot,
        vec![
            LeaderId::new("ValidatorNodeAlpha1111111111111111111111111"),
            LeaderId::new("ValidatorNodeAlpha1111111111111111111111111"),
            LeaderId::new("ValidatorNodeBeta22222222222222222222222222"),
            LeaderId::new("ValidatorNodeGamma3333333333333333333333333"),
        ],
    );

    let t0 = Instant::now();
    clock.on_slot_observed_at(start_slot, t0);

    for offset_ms in [0_u64, 84, 160, 198] {
        let snap = clock.progress_at(t0 + Duration::from_millis(offset_ms));
        println!(
            "[clock] slot={} elapsed={}ms remaining={}ms progress={:.0}%",
            snap.slot,
            snap.elapsed_ms,
            snap.remaining_ms,
            snap.progress * 100.0
        );
    }

    for idx in 0..3 {
        let cur = Slot(start_slot.as_u64() + idx);
        let nxt = cur.next();
        let handoff = leaders.is_leader_transition(cur, nxt);
        println!(
            "[leader] slot={} -> {} | next_slot={} -> {} | handoff={}",
            cur,
            leaders.leader_for(cur).unwrap(),
            nxt,
            leaders.leader_for(nxt).unwrap(),
            handoff
        );
    }

    let t1 = t0 + Duration::from_millis(199);
    clock.on_slot_observed_at(start_slot.next(), t1);
    println!(
        "[tick] advanced to slot={} | measured_prev_slot={:?}",
        clock.current_slot(),
        clock.last_measured_duration()
    );
}
