use chrono::Utc;
use honba_core::{
    Exchange, FillReport, Money, InstrumentId, OrderAck, OrderRequest, OrderSide, OrderStatus, OrderType,
    Price, Quantity,
};
use honba_persistence::SqliteAuditLedger;

#[tokio::test]
async fn test_sqlite_sebi_audit_ledger_lifecycle() {
    let ledger = SqliteAuditLedger::new_in_memory()
        .await
        .expect("in-memory audit ledger creation failed");

    let algo_id = "algo-sebi-test-01";
    let client_id = "client-retail-99";
    let inst = InstrumentId::new(Exchange::Nse, "RELIANCE");
    let req = OrderRequest::new(
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("2850.50").unwrap()),
        Quantity::from_u64(100),
        algo_id,
    )
    .with_client_id(client_id);
    let correlation_id = req.correlation_id();

    // 1. Record Order Attempt
    let attempt_id = ledger.record_order_attempt(&req).await.unwrap();
    assert!(attempt_id > 0);

    // 2. Record Pre-Trade Risk Refusal
    let refusal_id = ledger
        .record_risk_refusal(&req, "SEBI OTR threshold breached: 52.5 > 50.0", 42)
        .await
        .unwrap();
    assert!(refusal_id > attempt_id);

    // 3. Record Broker Ack
    let ack = OrderAck {
        order_id: "BRK-ORDER-999".to_string(),
        client_order_id: req.client_order_id,
        exchange_order_id: Some("EX-NSE-888".to_string()),
        status: OrderStatus::New,
        timestamp: Utc::now(),
        correlation_id: Some(correlation_id),
        client_id: Some(client_id.to_string()),
    };
    let ack_id = ledger.record_broker_ack(&req, &ack, 1250).await.unwrap();
    assert!(ack_id > refusal_id);

    // 4. Record Modification
    let mod_id = ledger
        .record_modification(
            algo_id,
            client_id,
            correlation_id,
            &ack.order_id,
            "NSE:RELIANCE",
            Some(Price::from_rupees("2855.00").unwrap()),
            Quantity::from_u64(100),
            310,
        )
        .await
        .unwrap();
    assert!(mod_id > ack_id);

    // 5. Record Cancellation
    let cancel_id = ledger
        .record_cancellation(
            algo_id,
            client_id,
            correlation_id,
            &ack.order_id,
            "NSE:RELIANCE",
            "User requested cancel",
            250,
        )
        .await
        .unwrap();
    assert!(cancel_id > mod_id);

    // 6. Record Fill
    let fill = FillReport {
        fill_id: "FILL-111".to_string(),
        order_id: ack.order_id.clone(),
        client_order_id: req.client_order_id,
        instrument: inst,
        side: OrderSide::Buy,
        price: Price::from_rupees("2850.50").unwrap(),
        quantity: Quantity::from_u64(100),
        algo_id: algo_id.to_string(),
        timestamp: Utc::now(),
        correlation_id: Some(correlation_id),
        client_id: Some(client_id.to_string()),
        fee: Money::zero_inr(),
        fee_breakdown: None,
    };
    let fill_record_id = ledger.record_fill(&fill).await.unwrap();
    assert!(fill_record_id > cancel_id);

    // 7. Verification: count total records
    let count = ledger.count().await.unwrap();
    assert_eq!(count, 6);

    // 8. Query by algo_id
    let algo_records = ledger.query_by_algo_id(algo_id).await.unwrap();
    assert_eq!(algo_records.len(), 6);
    assert_eq!(algo_records[0].event_type, "order_attempt");
    assert_eq!(algo_records[1].event_type, "risk_refusal");
    assert_eq!(
        algo_records[1].refusal_reason.as_deref(),
        Some("SEBI OTR threshold breached: 52.5 > 50.0")
    );
    assert_eq!(algo_records[1].latency_us, Some(42));
    assert_eq!(algo_records[2].event_type, "broker_ack");
    assert_eq!(algo_records[2].latency_us, Some(1250));

    // 9. Query by correlation_id
    let corr_records = ledger
        .query_by_correlation_id(&correlation_id.to_string())
        .await
        .unwrap();
    assert_eq!(corr_records.len(), 6);

    // 10. Query by event_type
    let fills = ledger.query_by_event_type("fill").await.unwrap();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price.as_deref(), Some("2850.50"));

    // 11. Export NDJSON
    let mut buffer = Vec::new();
    let exported = ledger.export_ndjson(&mut buffer).await.unwrap();
    assert_eq!(exported, 6);
    let ndjson_str = String::from_utf8(buffer).unwrap();
    let lines: Vec<&str> = ndjson_str.trim().split('\n').collect();
    assert_eq!(lines.len(), 6);
}
