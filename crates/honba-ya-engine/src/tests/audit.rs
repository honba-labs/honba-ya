//! Unit tests for `crate::audit`.

use crate::{AuditKind, AuditLog, AuditRecord, TradingState};

fn sample_log() -> AuditLog {
    let mut log = AuditLog::new();
    log.record(AuditKind::EventDispatched { ts_event: 7 });
    log.record(AuditKind::OrderSubmitted {
        order_id: "O-1".to_string(),
        instrument: "X.NSE".to_string(),
        side: "buy".to_string(),
    });
    log.record(AuditKind::FillProduced {
        order_id: "O-1".to_string(),
        quantity: 5.0,
        price: 101.0,
        ts_event: 7,
    });
    log
}

#[test]
fn a_fresh_log_is_empty() {
    let log = AuditLog::new();
    assert!(log.is_empty());
    assert_eq!(log.len(), 0);
    assert!(log.records().is_empty());
}

#[test]
fn seq_starts_at_zero_and_advances_by_one_per_record() {
    let mut log = AuditLog::new();
    assert_eq!(log.record(AuditKind::EventDispatched { ts_event: 1 }), 0);
    assert_eq!(
        log.record(AuditKind::CancelRequested {
            order_id: "O-1".to_string()
        }),
        1
    );
    assert_eq!(
        log.record(AuditKind::StateChanged {
            from: TradingState::Active,
            to: TradingState::Halted,
        }),
        2
    );
    assert_eq!(log.len(), 3);
    assert!(!log.is_empty());
}

#[test]
fn records_are_returned_in_insertion_order_with_matching_seq() {
    let log = sample_log();
    assert_eq!(
        log.records(),
        [
            AuditRecord {
                seq: 0,
                kind: AuditKind::EventDispatched { ts_event: 7 },
            },
            AuditRecord {
                seq: 1,
                kind: AuditKind::OrderSubmitted {
                    order_id: "O-1".to_string(),
                    instrument: "X.NSE".to_string(),
                    side: "buy".to_string(),
                },
            },
            AuditRecord {
                seq: 2,
                kind: AuditKind::FillProduced {
                    order_id: "O-1".to_string(),
                    quantity: 5.0,
                    price: 101.0,
                    ts_event: 7,
                },
            },
        ]
    );
}

#[test]
fn interleaved_kinds_keep_one_monotonic_seq() {
    let mut log = AuditLog::new();
    let kinds = [
        AuditKind::CancelRequested {
            order_id: "A".to_string(),
        },
        AuditKind::EventDispatched { ts_event: 1 },
        AuditKind::OrderRejected {
            order_id: "B".to_string(),
            reason: "no execution attached".to_string(),
        },
        AuditKind::EventDispatched { ts_event: 2 },
        AuditKind::CancelRequested {
            order_id: "C".to_string(),
        },
    ];
    let mut expected = Vec::new();
    for (i, kind) in kinds.iter().enumerate() {
        let seq = log.record(kind.clone());
        assert_eq!(seq, i as u64);
        expected.push(AuditRecord {
            seq,
            kind: kind.clone(),
        });
    }
    assert_eq!(log.records(), expected);
}

#[test]
fn logs_with_the_same_history_are_equal_and_debuggable() {
    assert_eq!(sample_log(), sample_log());
    assert_ne!(sample_log(), AuditLog::new());
    let rendered = format!("{:?}", sample_log());
    assert!(rendered.contains("EventDispatched"));
    assert!(rendered.contains("OrderSubmitted"));
    let cloned = sample_log();
    assert_eq!(cloned.records().len(), 3);
    assert_eq!(format!("{cloned:?}"), rendered);
}

#[test]
fn audit_replay_matches_state() {
    let log = sample_log();
    let replayed = log.replay();

    let inst = honba_messages::InstrumentId::new("X", honba_messages::Exchange::new("NSE"));
    assert_eq!(replayed.position(&inst), 5.0);
    assert_eq!(replayed.dispatches(), 1);
    assert_eq!(replayed.trading_state(), TradingState::Active);

    let order = replayed.order("O-1").expect("order O-1 must exist");
    assert_eq!(order.filled_qty, 5.0);
    assert_eq!(order.side, honba_messages::OrderSide::Buy);
    assert_eq!(order.instrument, inst);
}

#[test]
fn audit_replay_complex_lifecycle() {
    let mut log = AuditLog::new();
    let inst = honba_messages::InstrumentId::new("RELIANCE", honba_messages::Exchange::new("NSE"));
    log.record(AuditKind::OrderSubmitted {
        order_id: "ORD-1".to_string(),
        instrument: "RELIANCE.NSE".to_string(),
        side: "buy".to_string(),
    });
    log.record(AuditKind::OrderLifecycle {
        order_id: "ORD-1".to_string(),
        event: honba_messages::OrderEventKind::Accepted,
        ts_event: 10,
    });
    log.record(AuditKind::FillProduced {
        order_id: "ORD-1".to_string(),
        quantity: 10.0,
        price: 2500.0,
        ts_event: 15,
    });
    log.record(AuditKind::OrderSubmitted {
        order_id: "ORD-2".to_string(),
        instrument: "RELIANCE.NSE".to_string(),
        side: "sell".to_string(),
    });
    log.record(AuditKind::FillProduced {
        order_id: "ORD-2".to_string(),
        quantity: 4.0,
        price: 2510.0,
        ts_event: 20,
    });
    log.record(AuditKind::CancelRequested {
        order_id: "ORD-2".to_string(),
    });
    log.record(AuditKind::OrderLifecycle {
        order_id: "ORD-2".to_string(),
        event: honba_messages::OrderEventKind::Cancelled,
        ts_event: 25,
    });
    log.record(AuditKind::StateChanged {
        from: TradingState::Active,
        to: TradingState::Reducing,
    });

    let replayed = log.replay();
    assert_eq!(replayed.position(&inst), 6.0); // 10 buy - 4 sell
    assert_eq!(replayed.trading_state(), TradingState::Reducing);

    let ord2 = replayed.order("ORD-2").expect("ORD-2 must exist");
    assert_eq!(ord2.filled_qty, 4.0);
    assert_eq!(
        ord2.lifecycle,
        Some(honba_messages::OrderEventKind::Cancelled)
    );
    assert!(!ord2.cancel_requested);
}

#[test]
fn journal_writer_roundtrip() {
    let log = sample_log();
    let mut buffer = Vec::new();
    log.write_ndjson(&mut buffer)
        .expect("write_ndjson must succeed");

    let read_log = AuditLog::read_ndjson(&buffer[..]).expect("read_ndjson must succeed");
    assert_eq!(log, read_log);
    assert_eq!(log.replay(), read_log.replay());
}

#[test]
fn journal_schema_roundtrip() {
    let header = crate::JournalHeader {
        schema_version: 4,
        journal_version: 1,
        seed: 42,
        config_hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_string(),
        dataset_id: "NSE:TCS:1d:2024-01-01:2024-12-31".to_string(),
        git_rev: Some("a06d5d9".to_string()),
        trial_count: Some(1),
    };
    let log = sample_log();
    let journal = crate::JournalV1::from_audit_log(header.clone(), &log);

    let mut buffer = Vec::new();
    journal
        .write_ndjson(&mut buffer)
        .expect("write_ndjson must succeed");

    let read = crate::JournalV1::read_ndjson(&buffer[..]).expect("read_ndjson must succeed");
    assert_eq!(read.header, header);
    assert_eq!(read.log, log);
    assert_eq!(read.log.replay(), log.replay());
}
