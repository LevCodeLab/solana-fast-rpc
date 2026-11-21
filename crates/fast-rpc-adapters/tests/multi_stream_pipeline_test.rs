use fast_rpc_adapters::{
    GeyserPayload, LocalGeyserModeAdapter, ValidatorGeyserAdapter, ValidatorRawFrame,
    DEFAULT_VALIDATOR_SOCKET_PATH,
};
use fast_rpc_bank::{
    BankGraph, BankNode, CertificateEngine, EpochStakeTable, ReplacementEngine, StakeEngine,
    ValidatorStakeEntry,
};
use fast_rpc_clock::{LeaderEngine, SlotClock};
use fast_rpc_core::events::RpcEventKind;
use fast_rpc_core::types::{
    BankState, CertificateValidationStatus, SlotDuration, SyntheticCandidateId,
};

#[test]
fn test_full_pipeline_local_stream_to_fork_graph_and_bls_cert() {
    let adapter = LocalGeyserModeAdapter::new(None);
    let events = adapter.emit_full_fidelity_batch();

    let mut clock = SlotClock::new(SlotDuration::MS_200);
    let mut leaders = LeaderEngine::new();
    let mut graph = BankGraph::new();
    let mut replacements = ReplacementEngine::new();
    let cert_engine = CertificateEngine::new();

    let mut stake_engine = StakeEngine::new();
    let validators: Vec<ValidatorStakeEntry> = (0..16)
        .map(|i| ValidatorStakeEntry {
            identity: format!("validator-{}", i),
            stake_lamports: 100_000_000_000,
            is_active: true,
            rank: i,
            bls_pubkey: None,
        })
        .collect();
    stake_engine.register_epoch_stake(EpochStakeTable::new(500, validators, 1000));

    let mut certified_fast_path = false;

    for ev in events {
        match ev.kind {
            RpcEventKind::SlotObserved { .. } => {
                clock.on_slot_observed(ev.slot);
            }
            RpcEventKind::LeaderScheduleObserved {
                starting_slot,
                leaders: sched,
            } => {
                leaders.populate_schedule(starting_slot, sched);
            }
            RpcEventKind::BankCreated { identity, parent } => {
                let bank_id = identity.bank_id.clone();
                let key = graph.insert_bank(BankNode::new(identity, Some(parent), 1_000_000));
                if let Some(bid) = bank_id {
                    let _ = replacements.correlate_replacement(
                        &ev.provider,
                        ev.slot,
                        bid,
                        key,
                        1_000_100,
                    );
                }
            }
            RpcEventKind::UpdateParent {
                slot,
                cleared_bank_id: Some(cleared_id),
                ..
            } => {
                replacements.record_cleared_bank(
                    &mut graph,
                    &ev.provider,
                    slot,
                    cleared_id,
                    1_000_050,
                );
            }
            RpcEventKind::CertificateObserved {
                slot,
                kind,
                raw_bytes: Some(raw),
                block_id,
                ..
            } => {
                let mut parsed = cert_engine.parse_certificate_with_context(
                    kind,
                    slot,
                    block_id,
                    ev.provider.as_str(),
                    raw,
                    1_000_200,
                );
                let stake_res = cert_engine.verify_certificate(&mut parsed, 500, &stake_engine);
                if parsed.validation_status == CertificateValidationStatus::FinalityEffective
                    && stake_res.threshold_met
                {
                    certified_fast_path = true;
                }
            }
            RpcEventKind::CanonicalObserved { canonical_identity } => {
                let key = fast_rpc_bank::BankKey::from_identity(&canonical_identity);
                graph.mark_canonical(&key, 1_000_300);
            }
            _ => {}
        }
    }

    assert_eq!(clock.current_slot().as_u64(), 1_000_000);
    assert_eq!(leaders.cached_slots_count(), 3);
    assert_eq!(replacements.completed_replacements().len(), 1);
    assert!(certified_fast_path);

    let slot_banks = graph.get_banks_for_slot(clock.current_slot());
    assert_eq!(slot_banks.len(), 2);
    assert!(slot_banks.iter().any(|b| b.state == BankState::Abandoned));
    assert!(slot_banks.iter().any(|b| b.state == BankState::Canonical));
}

#[test]
fn test_ipc_wire_frame_and_default_socket_constants() {
    let adapter = ValidatorGeyserAdapter::with_default_socket(None);
    assert_eq!(adapter.socket_path(), DEFAULT_VALIDATOR_SOCKET_PATH);
    assert_eq!(
        DEFAULT_VALIDATOR_SOCKET_PATH,
        "/tmp/solana_validator_stream.sock"
    );

    let synth = SyntheticCandidateId::new("slot-900-a");
    assert_eq!(synth.to_string(), "synth-cand-slot-900-a");

    let frame = ValidatorRawFrame {
        sequence: 1,
        observer_validator: "ValidatorNodeAlpha1111111111111111111111111".to_string(),
        observed_at_nanos: 42_000,
        payload: GeyserPayload::Heartbeat {
            timestamp_nanos: 42_000,
            active_slots: 4,
        },
    };
    let encoded = serde_json::to_string(&frame).expect("serialize frame");
    let decoded: ValidatorRawFrame = serde_json::from_str(&encoded).expect("deserialize frame");
    assert_eq!(decoded.sequence, 1);
}
