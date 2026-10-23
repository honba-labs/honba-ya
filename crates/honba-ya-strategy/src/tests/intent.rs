//! Unit tests for `crate::intent`.

use honba_core::{OrderSide, OrderType, Price, Quantity};
use honba_testing::fixtures::exact_decimal;
use rust_decimal::Decimal;

use super::any_instrument;
use crate::intent::*;

fn p(val: f64) -> Price {
    Price::new(exact_decimal(val))
}

fn q(val: f64) -> Quantity {
    Quantity::new(exact_decimal(val))
}

#[test]
fn constructors_are_valid() {
    for i in [
        OrderIntent::market_buy(any_instrument(), q(1.0)),
        OrderIntent::market_sell(any_instrument(), q(1.0)),
        OrderIntent::limit_buy(any_instrument(), q(1.0), p(10.0)),
        OrderIntent::limit_sell(any_instrument(), q(1.0), p(10.0)),
        OrderIntent::stop_buy(any_instrument(), q(1.0), p(10.0)),
        OrderIntent::stop_sell(any_instrument(), q(1.0), p(10.0)),
        OrderIntent::stop_limit_buy(any_instrument(), q(1.0), p(10.0), p(10.5)),
        OrderIntent::stop_limit_sell(any_instrument(), q(1.0), p(10.0), p(9.5)),
    ] {
        assert_eq!(i.validate(), Ok(()), "{i:?}");
    }
}

#[test]
fn stop_constructors_set_trigger_not_limit() {
    let i = OrderIntent::stop_sell(any_instrument(), q(2.0), p(99.0));
    assert_eq!(i.order_type, OrderType::StopLoss(p(99.0)));
    assert_eq!(i.side, OrderSide::Sell);
}

#[test]
fn validate_rejects_non_positive_quantity() {
    use IntentError::*;
    let base = OrderIntent::stop_limit_buy(any_instrument(), q(1.0), p(10.0), p(10.5));
    let zero_q = Quantity::new(Decimal::ZERO);
    let neg_q = Quantity::new(Decimal::from(-1));

    for bad_q in [zero_q, neg_q] {
        let intent = OrderIntent {
            quantity: bad_q,
            ..base.clone()
        };
        assert_eq!(intent.validate(), Err(NonPositiveQuantity(bad_q)));
    }
}

#[test]
fn into_order_keeps_order_type_and_prices() {
    let order = OrderIntent::stop_buy(any_instrument(), q(1.0), p(10.0))
        .into_order("algo-1")
        .unwrap();
    assert_eq!(order.order_type, OrderType::StopLoss(p(10.0)));
    assert_eq!(order.order_type.limit_price(), None);

    let order = OrderIntent::market_buy(any_instrument(), q(1.0))
        .into_order("algo-1")
        .unwrap();
    assert_eq!(order.order_type, OrderType::Market);

    let sl = OrderIntent::stop_limit_sell(any_instrument(), q(2.0), p(100.0), p(99.5))
        .into_order("algo-1")
        .unwrap();
    assert_eq!(
        sl.order_type,
        OrderType::StopLimit {
            trigger: p(100.0),
            limit: p(99.5),
        }
    );
    assert_eq!(sl.order_type.limit_price(), Some(p(99.5)));
}

#[test]
fn into_order_rejects_invalid_intents() {
    let neg_q = Quantity::new(Decimal::from(-1));
    assert_eq!(
        OrderIntent::market_buy(any_instrument(), neg_q).into_order("algo-1"),
        Err(IntentError::NonPositiveQuantity(neg_q))
    );
}
