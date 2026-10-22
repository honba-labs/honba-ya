//! Unit tests for `crate::scripted_gateway::ScriptedGateway`.

use honba_core::rust_decimal::Decimal;
use honba_core::{OrderRequest, OrderSide, OrderType, Price, Quantity};

use crate::fixtures::any_instrument;
use crate::ports::{ExecutionGateway, PortError};
use crate::scripted_gateway::ScriptedGateway;

fn order(tag: &str, qty: u64) -> OrderRequest {
    let mut order = OrderRequest::new(
        any_instrument(),
        OrderSide::Buy,
        OrderType::Market,
        Quantity::from_u64(qty),
        "scripted-gateway-test",
    );
    order.tag = Some(tag.to_string());
    order
}

#[tokio::test]
async fn submit_assigns_a_venue_id_distinct_from_the_client_id() {
    let mut gw = ScriptedGateway::new();
    let ack = gw.submit_order(order("C-1", 10)).await.unwrap();
    assert_eq!(ack.order_id, "V-1");
    assert_eq!(ack.client_order_id.to_string(), gw.submitted()[0].client_order_id.to_string());
    assert_eq!(gw.submitted().len(), 1);
    assert_eq!(gw.submitted()[0].tag.as_deref(), Some("C-1"));
}

#[tokio::test]
async fn scripted_fill_is_yielded_once_under_the_venue_id() {
    let mut gw = ScriptedGateway::new();
    let ack = gw.submit_order(order("C-1", 10)).await.unwrap();
    gw.script_fill(&ack.order_id, Quantity::from_u64(4), Price::new(Decimal::from(101)));
    let fill = gw.next_fill().await.unwrap().unwrap();
    assert_eq!(fill.order_id, ack.order_id);
    assert_eq!(fill.client_order_id, ack.client_order_id);
    assert_eq!(fill.quantity, Quantity::from_u64(4));
    assert_eq!(fill.price, Price::new(Decimal::from(101)));
    assert!(gw.next_fill().await.unwrap().is_none());
}

#[tokio::test]
async fn scripted_failures_are_one_shot() {
    let mut gw = ScriptedGateway::new();
    gw.fail_next_submit(PortError::Timeout);
    assert_eq!(
        gw.submit_order(order("C-1", 1)).await,
        Err(PortError::Timeout)
    );
    assert!(gw.submit_order(order("C-2", 1)).await.is_ok());

    gw.fail_next_cancel(PortError::Unavailable("down".into()));
    let id = "V-2";
    assert!(gw.cancel_order(id).await.is_err());
    assert!(gw.cancel_order(id).await.is_ok());
    assert_eq!(gw.cancellations(), vec!["V-2".to_string(), "V-2".to_string()]);

    gw.script_fill_error(PortError::Transport("eof".into()));
    assert!(gw.next_fill().await.is_err());
    assert!(gw.next_fill().await.unwrap().is_none());
}

#[tokio::test]
async fn clones_share_one_script() {
    let gw = ScriptedGateway::new();
    let mut moved = gw.clone();
    moved.submit_order(order("C-1", 1)).await.unwrap();
    assert_eq!(gw.submitted().len(), 1);
    assert_eq!(gw.venue_ids(), vec!["V-1".to_string()]);
}
