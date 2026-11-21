use fast_rpc_bank::{
    BankGraph, BankNode, CanonicalEvidence, CanonicalResolver, CertificateEngine, EpochStakeTable,
    FinalityEngine, ReplacementEngine, StakeEngine, ValidatorStakeEntry,
};
use fast_rpc_core::identity::BankIdentity;
use fast_rpc_core::types::{
    BankId, Blockhash, CertificateKind, ParentReference, ProviderId, Slot,
};

fn main() {
    println!("=== solana-fast-rpc // fork graph, update-parent & bls 80% cert demo ===");

    let mut graph = BankGraph::new();
    let mut replacements = ReplacementEngine::new();
    let resolver = CanonicalResolver::new();
    let cert_engine = CertificateEngine::new();
    let mut finality = FinalityEngine::new();

    let mut stake_engine = StakeEngine::new();
    let validators: Vec<ValidatorStakeEntry> = (0..10)
        .map(|rank| ValidatorStakeEntry {
            identity: format!("validator-node-{}", rank),
            stake_lamports: 100_000_000_000,
            is_active: true,
            rank,
            bls_pubkey: None,
        })
        .collect();
    stake_engine.register_epoch_stake(EpochStakeTable::new(612, validators, 1_000_000));

    let provider = ProviderId::new("local-validator");
    let slot = Slot(314_920_100);

    // 1. First candidate bank arrives on old parent
    let bank_14 = BankNode::new(
        BankIdentity::new(
            provider.clone(),
            Some(BankId(14)),
            slot,
            Some(Blockhash::new("BlockhashCandidate14Alpha11111111111111111")),
        ),
        Some(ParentReference::SlotOnly(Slot(314_920_099))),
        1_000_000_000,
    );
    let key_14 = graph.insert_bank(bank_14);
    println!("[fork-dag] inserted {} state={}", BankId(14), graph.get_bank(&key_14).unwrap().state);

    // 2. Fast leader handover UpdateParent clears bank-14 and spawns bank-15
    replacements.record_cleared_bank(&mut graph, &provider, slot, BankId(14), 1_049_000_000);
    let winning_hash = Blockhash::new("BlockhashCanonical15Beta222222222222222222");
    let bank_15 = BankNode::new(
        BankIdentity::new(
            provider.clone(),
            Some(BankId(15)),
            slot,
            Some(winning_hash.clone()),
        ),
        Some(ParentReference::KnownBlockhash(Blockhash::new(
            "ParentBlockhashAuthoritative99999999999999",
        ))),
        1_052_000_000,
    );
    let key_15 = graph.insert_bank(bank_15);
    let rep = replacements
        .correlate_replacement(&provider, slot, BankId(15), key_15.clone(), 1_052_000_000)
        .expect("replacement correlated");
    println!(
        "[handover] cleared={} -> replacement={:?} (old bank state={})",
        rep.cleared_bank_id,
        rep.replacement_bank_id,
        graph.get_bank(&key_14).unwrap().state
    );

    // 3. Canonical resolution + 80% stake BLS FinalizeFast certificate check
    let evidence = CanonicalEvidence::ConfirmedBlockhash(winning_hash.clone());
    let res = resolver.resolve_slot(&mut graph, slot, Some(&evidence), 1_075_000_000);
    println!("[canonical] resolution={:?}", res);

    let mut raw_cert = vec![0xAB; 48];
    raw_cert.push(0b1111_1111); // 8 of 10 equal-stake validators = 8000 bps (80%)

    let mut parsed_cert = cert_engine.parse_certificate_with_context(
        CertificateKind::FinalizeFast,
        slot,
        Some(winning_hash.to_string()),
        "local-validator",
        raw_cert,
        1_090_400_000,
    );
    let stake_res = cert_engine.verify_certificate(&mut parsed_cert, 612, &stake_engine);
    println!(
        "[bls-cert] status={} stake={} bps (required={} bps)",
        parsed_cert.validation_status,
        stake_res.participation_basis_points,
        stake_res.required_threshold_basis_points
    );

    let rec = finality
        .record_finality(&mut graph, &key_15, 1_142_400_000)
        .expect("finality recorded");
    println!(
        "[finality] slot={} bank={} state={} finality_latency={}us",
        rec.slot,
        BankId(15),
        graph.get_bank(&key_15).unwrap().state,
        rec.finality_latency_us
    );
}
