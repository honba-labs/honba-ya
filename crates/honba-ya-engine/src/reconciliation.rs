//! Reconciliation on startup and reconnect (E2-S7).
//!
//! Diffs broker-reported orders, positions, and trades against the engine's
//! [`StateCache`], identifying:
//! - Missed fills (broker reported fills that the engine did not see);
//! - Stale open orders (orders open in cache but closed or unknown at broker);
//! - Ghost orders (orders reported by broker that the engine has no record of);
//! - Position drift (net positions differing between engine and broker).
//!
//! Generates synthetic events to update the engine queue and catch up state.

use std::collections::{HashMap, HashSet};

use honba_messages::{
    Event, InstrumentId, OrderId, OrderSide, OrderStatus, UnixNanos, VenueOrderId,
};

use crate::cache::{CacheQuery, StateCache, TrackedOrder};

/// An order reported by a broker during reconciliation.
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerOrderReport {
    /// Client order id, if present / correlated.
    pub order_id: Option<OrderId>,
    /// Venue / broker order id.
    pub venue_order_id: VenueOrderId,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Side of the order.
    pub side: OrderSide,
    /// Total ordered quantity.
    pub quantity: f64,
    /// Cumulative filled quantity according to broker.
    pub filled_qty: f64,
    /// Average fill price, if available.
    pub avg_price: Option<f64>,
    /// Current broker order status.
    pub status: OrderStatus,
}

/// A position reported by a broker.
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerPositionReport {
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Net signed quantity held at the broker.
    pub quantity: f64,
}

/// A trade/fill reported by a broker.
#[derive(Clone, Debug, PartialEq)]
pub struct BrokerTradeReport {
    /// Broker trade / execution id.
    pub trade_id: String,
    /// Client order id, if correlated.
    pub order_id: Option<OrderId>,
    /// Venue order id, if correlated.
    pub venue_order_id: Option<VenueOrderId>,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Side of the fill.
    pub side: OrderSide,
    /// Filled quantity.
    pub quantity: f64,
    /// Execution price.
    pub price: f64,
    /// Execution timestamp.
    pub ts_event: UnixNanos,
}

/// A broker snapshot taken on startup or reconnect.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BrokerSnapshot {
    /// Orders currently known to the broker.
    pub orders: Vec<BrokerOrderReport>,
    /// Positions held at the broker.
    pub positions: Vec<BrokerPositionReport>,
    /// Trades executed during the session.
    pub trades: Vec<BrokerTradeReport>,
}

impl BrokerSnapshot {
    /// Creates an empty broker snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an order report.
    pub fn with_order(mut self, order: BrokerOrderReport) -> Self {
        self.orders.push(order);
        self
    }

    /// Adds a position report.
    pub fn with_position(mut self, instrument_id: InstrumentId, quantity: f64) -> Self {
        self.positions.push(BrokerPositionReport {
            instrument_id,
            quantity,
        });
        self
    }

    /// Adds a trade report.
    pub fn with_trade(mut self, trade: BrokerTradeReport) -> Self {
        self.trades.push(trade);
        self
    }
}

/// A detected missed fill: broker reported fill quantity ahead of local state.
#[derive(Clone, Debug, PartialEq)]
pub struct MissedFill {
    /// Client order identifier.
    pub order_id: OrderId,
    /// Venue order id, if known.
    pub venue_order_id: Option<VenueOrderId>,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Order side.
    pub side: OrderSide,
    /// The missed quantity (`broker_filled - cache_filled`).
    pub quantity: f64,
    /// Total cumulative filled quantity at broker.
    pub cum_qty: f64,
    /// Price of the fill.
    pub price: f64,
    /// Whether this fill completes the order.
    pub completes_order: bool,
}

/// A ghost order: present at the broker but unknown to the engine.
#[derive(Clone, Debug, PartialEq)]
pub struct GhostOrder {
    /// Venue order id.
    pub venue_order_id: VenueOrderId,
    /// Client order id, if assigned out-of-band.
    pub order_id: Option<OrderId>,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Order side.
    pub side: OrderSide,
    /// Quantity.
    pub quantity: f64,
    /// Filled quantity.
    pub filled_qty: f64,
    /// Status at broker.
    pub status: OrderStatus,
}

/// Position drift detected between engine cache and broker.
#[derive(Clone, Debug, PartialEq)]
pub struct PositionDrift {
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// Quantity in engine cache.
    pub cache_quantity: f64,
    /// Quantity reported by broker.
    pub broker_quantity: f64,
    /// Signed drift (`broker_quantity - cache_quantity`).
    pub drift: f64,
}

/// A stale open order: open in engine cache, but closed or dropped at broker.
#[derive(Clone, Debug, PartialEq)]
pub struct StaleOrder {
    /// Client order identifier.
    pub order_id: OrderId,
    /// Status in cache.
    pub cache_status: OrderStatus,
    /// Status at broker (or None if missing from broker).
    pub broker_status: Option<OrderStatus>,
}

/// The result of reconciling broker state against the engine cache.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReconciliationReport {
    /// Missed fills detected.
    pub missed_fills: Vec<MissedFill>,
    /// Ghost orders detected.
    pub ghost_orders: Vec<GhostOrder>,
    /// Position drifts detected.
    pub position_drifts: Vec<PositionDrift>,
    /// Stale open orders detected.
    pub stale_orders: Vec<StaleOrder>,
    /// Synthetic events generated to bring engine state in sync.
    pub synthetic_events: Vec<Event>,
}

impl ReconciliationReport {
    /// Whether any position drift was detected.
    pub fn has_drift(&self) -> bool {
        !self.position_drifts.is_empty()
    }

    /// Whether broker state and cache were in complete agreement.
    pub fn is_clean(&self) -> bool {
        self.missed_fills.is_empty()
            && self.ghost_orders.is_empty()
            && self.position_drifts.is_empty()
            && self.stale_orders.is_empty()
    }

    /// Applies all generated synthetic events to `cache`.
    pub fn apply_to_cache(&self, cache: &mut StateCache) {
        for event in &self.synthetic_events {
            cache.apply_event(event);
        }
    }
}

/// The reconciler engine.
pub struct Reconciler;

impl Reconciler {
    /// Reconciles broker state from `snapshot` against the local `cache`.
    pub fn reconcile(
        cache: &StateCache,
        snapshot: &BrokerSnapshot,
        now: UnixNanos,
    ) -> ReconciliationReport {
        let mut report = ReconciliationReport::default();
        let mut seen_cache_orders = HashSet::new();
        let mut missed_fills_signed: HashMap<InstrumentId, f64> = HashMap::new();

        // 1. Reconcile broker orders against cache
        for b_order in &snapshot.orders {
            let matched_cache_order: Option<(&String, &TrackedOrder)> =
                if let Some(ref cid) = b_order.order_id {
                    cache.orders().get_key_value(cid.as_str()).or_else(|| {
                        cache.orders().iter().find(|(_, t)| {
                            t.venue_order_id.as_ref() == Some(&b_order.venue_order_id)
                        })
                    })
                } else {
                    cache
                        .orders()
                        .iter()
                        .find(|(_, t)| t.venue_order_id.as_ref() == Some(&b_order.venue_order_id))
                };

            if let Some((cid_str, tracked)) = matched_cache_order {
                let order_id = OrderId::new(cid_str);
                seen_cache_orders.insert(cid_str.clone());

                // Check for missed fills
                let cache_filled = tracked.state.filled_qty;
                let broker_filled = b_order.filled_qty;
                if broker_filled > cache_filled + 1e-9 {
                    let missed_qty = broker_filled - cache_filled;
                    let price = b_order.avg_price.unwrap_or(0.0);
                    let completes = (broker_filled + 1e-9 >= b_order.quantity)
                        || b_order.status == OrderStatus::Filled;

                    report.missed_fills.push(MissedFill {
                        order_id: order_id.clone(),
                        venue_order_id: Some(b_order.venue_order_id.clone()),
                        instrument_id: b_order.instrument_id.clone(),
                        side: b_order.side,
                        quantity: missed_qty,
                        cum_qty: broker_filled,
                        price,
                        completes_order: completes,
                    });

                    let signed_missed = match b_order.side {
                        OrderSide::Sell => -missed_qty,
                        _ => missed_qty,
                    };
                    *missed_fills_signed
                        .entry(b_order.instrument_id.clone())
                        .or_insert(0.0) += signed_missed;

                    let syn_event = if completes {
                        Event::OrderFilled {
                            order_id: order_id.clone(),
                            last_qty: missed_qty,
                            last_px: price,
                            ts_event: now,
                        }
                    } else {
                        Event::OrderPartiallyFilled {
                            order_id: order_id.clone(),
                            last_qty: missed_qty,
                            cum_qty: broker_filled,
                            last_px: price,
                            ts_event: now,
                        }
                    };
                    report.synthetic_events.push(syn_event);
                }

                // Check for stale open orders
                if tracked.is_working() && is_stale_status(b_order.status) {
                    report.stale_orders.push(StaleOrder {
                        order_id: order_id.clone(),
                        cache_status: tracked.state.status,
                        broker_status: Some(b_order.status),
                    });

                    match b_order.status {
                        OrderStatus::Cancelled => {
                            report.synthetic_events.push(Event::OrderCancelled {
                                order_id: order_id.clone(),
                                ts_event: now,
                            });
                        }
                        OrderStatus::Expired => {
                            report.synthetic_events.push(Event::OrderExpired {
                                order_id: order_id.clone(),
                                ts_event: now,
                            });
                        }
                        OrderStatus::Rejected => {
                            report.synthetic_events.push(Event::OrderRejected {
                                order_id: order_id.clone(),
                                reason: "broker_rejected_on_reconnect".to_string(),
                                ts_event: now,
                            });
                        }
                        _ => {}
                    }
                }
            } else {
                // Ghost order: broker reports order unknown to engine
                report.ghost_orders.push(GhostOrder {
                    venue_order_id: b_order.venue_order_id.clone(),
                    order_id: b_order.order_id.clone(),
                    instrument_id: b_order.instrument_id.clone(),
                    side: b_order.side,
                    quantity: b_order.quantity,
                    filled_qty: b_order.filled_qty,
                    status: b_order.status,
                });
            }
        }

        // 2. Check for working orders in cache missing from broker
        for (cid, tracked) in cache.orders() {
            if tracked.is_working() && !seen_cache_orders.contains(cid) {
                let order_id = OrderId::new(cid);
                report.stale_orders.push(StaleOrder {
                    order_id: order_id.clone(),
                    cache_status: tracked.state.status,
                    broker_status: None,
                });
                report.synthetic_events.push(Event::OrderCancelled {
                    order_id,
                    ts_event: now,
                });
            }
        }

        // 3. Reconcile positions
        let mut all_instruments: HashSet<InstrumentId> = HashSet::new();
        all_instruments.extend(cache.positions().keys().cloned());
        for p in &snapshot.positions {
            all_instruments.insert(p.instrument_id.clone());
        }

        let broker_pos_map: HashMap<InstrumentId, f64> = snapshot
            .positions
            .iter()
            .map(|p| (p.instrument_id.clone(), p.quantity))
            .collect();

        for inst in all_instruments {
            let cache_pos = cache.position(&inst);
            let broker_pos = broker_pos_map.get(&inst).copied().unwrap_or(0.0);
            let missed_signed = missed_fills_signed.get(&inst).copied().unwrap_or(0.0);
            let expected_after_fills = cache_pos + missed_signed;

            let drift = broker_pos - expected_after_fills;
            if drift.abs() > 1e-9 {
                report.position_drifts.push(PositionDrift {
                    instrument_id: inst,
                    cache_quantity: cache_pos,
                    broker_quantity: broker_pos,
                    drift,
                });
            }
        }

        report
    }
}

fn is_stale_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
    )
}
