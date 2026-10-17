//! Acceptance test suite for typed LLM decisions and journaling (E5-S7).

use honba_engine::audit::{AuditLog, JournalHeader, JournalV1};
use honba_engine::decision::{LlmDecision, LlmDecisionError, LlmDecisionKind};

#[test]
fn schema_rejects_increase() {
    // 1. Increasing quantity is strictly rejected
    let res_inc = LlmDecision::new(
        "ord-1",
        LlmDecisionKind::SizeDown,
        100.0,
        150.0, // Attempted increase!
        "increasing size on momentum",
    );
    assert!(matches!(
        res_inc,
        Err(LlmDecisionError::QuantityIncreased {
            original,
            decided
        }) if (original - 100.0).abs() < 1e-6 && (decided - 150.0).abs() < 1e-6
    ));

    // 2. Non-positive original quantity is rejected
    let res_zero = LlmDecision::new("ord-2", LlmDecisionKind::Veto, 0.0, 0.0, "zero qty");
    assert!(matches!(
        res_zero,
        Err(LlmDecisionError::NonPositiveOriginalQuantity(_))
    ));

    // 3. Veto with non-zero quantity is rejected
    let res_veto_bad = LlmDecision::new(
        "ord-3",
        LlmDecisionKind::Veto,
        50.0,
        10.0,
        "veto but kept size",
    );
    assert!(matches!(
        res_veto_bad,
        Err(LlmDecisionError::InvalidVetoQuantity(_))
    ));

    // 4. Approve with modified quantity is rejected
    let res_approve_bad = LlmDecision::new(
        "ord-4",
        LlmDecisionKind::Approve,
        50.0,
        25.0,
        "approve with half size",
    );
    assert!(matches!(
        res_approve_bad,
        Err(LlmDecisionError::InvalidApproveQuantity { .. })
    ));

    // 5. SizeDown with equal or zero quantity is rejected
    let res_sizedown_equal = LlmDecision::new(
        "ord-5",
        LlmDecisionKind::SizeDown,
        50.0,
        50.0,
        "sizedown but kept same",
    );
    assert!(matches!(
        res_sizedown_equal,
        Err(LlmDecisionError::InvalidSizeDownQuantity { .. })
    ));

    let res_sizedown_zero = LlmDecision::new(
        "ord-6",
        LlmDecisionKind::SizeDown,
        50.0,
        0.0,
        "sizedown to zero should be veto",
    );
    assert!(matches!(
        res_sizedown_zero,
        Err(LlmDecisionError::InvalidSizeDownQuantity { .. })
    ));

    // 6. Valid cases pass
    let valid_veto = LlmDecision::new(
        "ord-ok-1",
        LlmDecisionKind::Veto,
        100.0,
        0.0,
        "regime shift risk",
    )
    .expect("valid veto");
    assert_eq!(valid_veto.decided_quantity, 0.0);

    let valid_sizedown = LlmDecision::new(
        "ord-ok-2",
        LlmDecisionKind::SizeDown,
        100.0,
        40.0,
        "high volatility environment",
    )
    .expect("valid size-down");
    assert_eq!(valid_sizedown.decided_quantity, 40.0);

    let valid_approve = LlmDecision::new(
        "ord-ok-3",
        LlmDecisionKind::Approve,
        100.0,
        100.0,
        "all risk checks passed",
    )
    .expect("valid approve");
    assert_eq!(valid_approve.decided_quantity, 100.0);
}

#[test]
fn decision_in_journal() {
    let mut log = AuditLog::new();

    let veto = LlmDecision::new(
        "order-alpha-1",
        LlmDecisionKind::Veto,
        250.0,
        0.0,
        "earnings announcement imminent; veto order",
    )
    .expect("valid veto");

    let sizedown = LlmDecision::new(
        "order-beta-2",
        LlmDecisionKind::SizeDown,
        100.0,
        25.0,
        "scale down position size due to liquidity drain",
    )
    .expect("valid size-down");

    let approve = LlmDecision::new(
        "order-gamma-3",
        LlmDecisionKind::Approve,
        50.0,
        50.0,
        "approved trend confirmation",
    )
    .expect("valid approve");

    // Record decisions into audit log
    let seq0 = log.record_llm_decision(&veto);
    let seq1 = log.record_llm_decision(&sizedown);
    let seq2 = log.record_llm_decision(&approve);

    assert_eq!(seq0, 0);
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
    assert_eq!(log.len(), 3);

    // Roundtrip through Journal v1 NDJSON
    let header = JournalHeader {
        schema_version: 3,
        journal_version: 1,
        seed: 42,
        config_hash: "cfg_hash_test".to_string(),
        dataset_id: "ds_test".to_string(),
        git_rev: None,
        trial_count: Some(1),
    };

    let journal = JournalV1::from_audit_log(header.clone(), &log);
    let mut ndjson_buf = Vec::new();
    journal
        .write_ndjson(&mut ndjson_buf)
        .expect("write_ndjson must succeed");

    let read_journal = JournalV1::read_ndjson(&ndjson_buf[..]).expect("read_ndjson must succeed");
    assert_eq!(read_journal.header, header);
    assert_eq!(read_journal.log.len(), 3);

    // Replay log to verify replay state preserves LLM decisions
    let replay = log.replay();
    assert_eq!(replay.llm_decisions().len(), 3);

    let d_veto = replay
        .llm_decision("order-alpha-1")
        .expect("veto decision found");
    assert_eq!(d_veto.decision, "veto");
    assert_eq!(d_veto.original_qty, 250.0);
    assert_eq!(d_veto.decided_qty, 0.0);
    assert!(d_veto.rationale.contains("earnings announcement"));

    let d_sizedown = replay
        .llm_decision("order-beta-2")
        .expect("size-down decision found");
    assert_eq!(d_sizedown.decision, "size_down");
    assert_eq!(d_sizedown.original_qty, 100.0);
    assert_eq!(d_sizedown.decided_qty, 25.0);

    let d_approve = replay
        .llm_decision("order-gamma-3")
        .expect("approve decision found");
    assert_eq!(d_approve.decision, "approve");
    assert_eq!(d_approve.original_qty, 50.0);
    assert_eq!(d_approve.decided_qty, 50.0);
}
