//! Invalid intents never reach execution (ADR 006).
//!
//! A strategy that emits an intent violating the `OrderIntent` invariants
//! must have it rejected with a typed [`IntentError`] at the runner boundary,
//! and no order may be submitted for it. Valid intents in the same batch are
//! unaffected.

use std::sync::{Arc, Mutex};

use honba_engine::{Engine, ExecutionEngine, Result};
use honba_entities::Trade;
use honba_messages::{Bar, InstrumentId, Order, OrderType};
use honba_strategy::{IntentError, OrderIntent, Strategy, StrategyContext, StrategyRunner};
use honba_testing::VecFeed;
use honba_testing::fixtures::instrument;

fn id() -> InstrumentId {
    instrument("NIFTY50")
}

/// Records every submitted order; never fills.
#[derive(Clone, Default)]
struct RecordingExecution {
    orders: Arc<Mutex<Vec<Order>>>,
}

impl ExecutionEngine for RecordingExecution {
    fn submit(&mut self, order: Order) -> Result<()> {
        self.orders.lock().unwrap().push(order);
        Ok(())
    }

    fn cancel(&mut self, _order_id: &str, _now: honba_messages::UnixNanos) -> Result<()> {
        Ok(())
    }

    fn drain_fills(&mut self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
}

type Rejections = Arc<Mutex<Vec<(OrderIntent, IntentError)>>>;

/// Emits two invalid intents and one valid one on the first bar.
struct Misbehaving {
    fired: bool,
    rejected: Rejections,
}

impl Strategy for Misbehaving {
    fn name(&self) -> &str {
        "misbehaving"
    }

    fn on_bar(&mut self, ctx: &mut dyn StrategyContext, _bar: &Bar) -> Result<()> {
        if !self.fired {
            self.fired = true;
            ctx.submit(OrderIntent::market_buy(id(), -1.0));
            ctx.submit(OrderIntent {
                price: None,
                ..OrderIntent::limit_buy(id(), 1.0, 100.0)
            });
            ctx.submit(OrderIntent::market_buy(id(), 2.0));
        }
        Ok(())
    }

    fn on_intent_rejected(
        &mut self,
        _ctx: &mut dyn StrategyContext,
        intent: &OrderIntent,
        error: &IntentError,
    ) -> Result<()> {
        self.rejected.lock().unwrap().push((intent.clone(), *error));
        Ok(())
    }
}

#[test]
fn engine_run_rejects_invalid_intents_without_submitting_orders() {
    let execution = RecordingExecution::default();
    let rejected = Rejections::default();
    let strategy = Misbehaving {
        fired: false,
        rejected: rejected.clone(),
    };

    let mut engine = Engine::new();
    engine.add_handler(StrategyRunner::new(strategy, execution.clone()));
    let mut feed = VecFeed::new(vec![
        VecFeed::bar("NIFTY50", 100.0, 1),
        VecFeed::bar("NIFTY50", 101.0, 2),
    ]);
    engine
        .run(&mut feed)
        .expect("an invalid intent must not abort the run");

    let orders = execution.orders.lock().unwrap();
    assert_eq!(orders.len(), 1, "only the valid intent becomes an order");
    assert_eq!(orders[0].quantity(), 2.0);
    assert_eq!(orders[0].order_type(), OrderType::Market);

    let rejected = rejected.lock().unwrap();
    let errors: Vec<IntentError> = rejected.iter().map(|(_, e)| *e).collect();
    assert_eq!(
        errors,
        vec![
            IntentError::NonPositiveQuantity(-1.0),
            IntentError::MissingPrice(OrderType::Limit),
        ]
    );
}

#[test]
fn runner_keeps_typed_rejections() {
    use honba_engine::Handler;

    let execution = RecordingExecution::default();
    let strategy = Misbehaving {
        fired: false,
        rejected: Rejections::default(),
    };
    let mut runner = StrategyRunner::new(strategy, execution.clone());
    let bar = VecFeed::bar("NIFTY50", 100.0, 1);
    runner.on_event(bar.event(), bar.ts_init()).unwrap();

    let rejections = runner.rejections();
    assert_eq!(rejections.len(), 2);
    assert_eq!(rejections[0].error, IntentError::NonPositiveQuantity(-1.0));
    assert_eq!(rejections[0].intent.quantity, -1.0);
    assert_eq!(rejections[0].ts_init, bar.ts_init());
    assert_eq!(
        rejections[1].error,
        IntentError::MissingPrice(OrderType::Limit)
    );
    assert_eq!(execution.orders.lock().unwrap().len(), 1);
}
