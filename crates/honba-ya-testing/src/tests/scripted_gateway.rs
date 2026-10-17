//! Unit tests for `crate::scripted_gateway::ScriptedGateway`.

use honba_ports::{ExecutionGateway, PortError};

use crate::fixtures::any_instrument;
use crate::scripted_gateway::ScriptedGateway;
use honba_messages::{Order, OrderId, OrderSide, OrderType, TimeInForce, UnixNanos};

fn order(id: &str, qty: f64) -> Order {
    Order::new(
        OrderId::new(id),
        any_instrument(),
        OrderSide::Buy,
        OrderType::Market,
        qty,
        None,
        TimeInForce::Day,
        UnixNanos::from_u64(1),
        UnixNanos::from_u64(1),
    )
}

#[tokio::test]
async fn submit_assigns_a_venue_id_distinct_from_the_client_id() {
    let mut gw = ScriptedGateway::new();
    let id = gw.submit_order(order("C-1", 10.0)).await.unwrap();
    assert_eq!(id.as_str(), "V-1");
    assert_eq!(gw.submitted().len(), 1);
    assert_eq!(gw.submitted()[0].order_id().as_str(), "C-1");
}

#[tokio::test]
async fn scripted_fill_is_yielded_once_under_the_venue_id() {
    let mut gw = ScriptedGateway::new();
    let id = gw.submit_order(order("C-1", 10.0)).await.unwrap();
    gw.script_fill(&id, 4.0, 101.0);
    let t = gw.next_fill().await.unwrap().unwrap();
    assert_eq!(t.order_id(), &id);
    assert_eq!(t.quantity(), 4.0);
    assert_eq!(t.price(), 101.0);
    assert!(gw.next_fill().await.unwrap().is_none());
}

#[tokio::test]
async fn scripted_failures_are_one_shot() {
    let mut gw = ScriptedGateway::new();
    gw.fail_next_submit(PortError::Timeout);
    assert_eq!(
        gw.submit_order(order("C-1", 1.0)).await,
        Err(PortError::Timeout)
    );
    assert!(gw.submit_order(order("C-2", 1.0)).await.is_ok());

    gw.fail_next_cancel(PortError::Unavailable("down".into()));
    let id = OrderId::new("V-2");
    assert!(gw.cancel_order(id.clone()).await.is_err());
    assert!(gw.cancel_order(id.clone()).await.is_ok());
    assert_eq!(gw.cancellations(), vec![id.clone(), id]);

    gw.script_fill_error(PortError::Transport("eof".into()));
    assert!(gw.next_fill().await.is_err());
    assert!(gw.next_fill().await.unwrap().is_none());
}

#[tokio::test]
async fn clones_share_one_script() {
    let gw = ScriptedGateway::new();
    let mut moved = gw.clone();
    moved.submit_order(order("C-1", 1.0)).await.unwrap();
    assert_eq!(gw.submitted().len(), 1);
}
