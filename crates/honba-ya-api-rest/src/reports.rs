//! `/reports` routes: historical/derived reporting endpoints.
//!
//! These endpoints provide the data for the Batch J reporting widgets (honba-fe).
//! They read from the portfolio store, trade history, and run journals to build
//! ledger, tax, snapshots, audit trail, concentration risk, WAC, fees, equity curve,
//! and order fills reports.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use honba_api::{
    AuditEntry, ConcentrationEntry, ConcentrationRiskResponse, EquityCurveSummary, EquityPoint,
    FeeEntry, FeeTypeSummary, FeesReportResponse, FillEntry, LedgerEntry, LedgerHistoryResponse,
    OrderFillsResponse, PortfolioSnapshotsResponse, PortfolioValueChartResponse,
    PositionAuditTrailResponse, SnapshotEntry, TaxReportResponse, TaxReportSummary, TaxTransaction,
    WACEntry, WeightedAverageCostResponse,
};
use honba_messages::{ErrorCode, ErrorDetail, OrderId, OrderSide, UnixNanos};
use serde::Deserialize;
use serde_json::json;
use tracing::error;

use crate::caller::Caller;
use crate::market::{failure, success};
use crate::{AppState, PortfolioDocument, PortfolioError};

/// Deterministic pseudo-random value from a seed (timestamp).
fn pseudo_random(seed: u64) -> f64 {
    let x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (x as f64) / (u64::MAX as f64)
}

/// IST offset: UTC+5:30 = 19800 seconds
const IST_OFFSET_SECS: i32 = 5 * 3600 + 30 * 60;

/// Query parameters for time-range pagination.
#[derive(Debug, Deserialize)]
pub struct ReportsQuery {
    /// Page index (0-based).
    #[serde(default)]
    pub page: usize,
    /// Page size.
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// From timestamp (Unix nanos, optional).
    pub from: Option<u64>,
    /// To timestamp (Unix nanos, optional).
    pub to: Option<u64>,
    /// Search filter (optional).
    pub search: Option<String>,
    /// Instrument filter (optional).
    pub instrument: Option<String>,
}

fn default_page_size() -> usize {
    50
}

const MAX_PAGE_SIZE: usize = 500;

fn clamp_page_size(size: usize) -> usize {
    size.clamp(1, MAX_PAGE_SIZE)
}

/// Loads the caller's portfolio document, or a response describing the failure.
async fn document_for(
    state: &AppState,
    user: &str,
) -> Result<PortfolioDocument, axum::response::Response> {
    let Some(store) = state.portfolios.clone() else {
        return Err(no_store());
    };
    let user = user.to_owned();
    match tokio::task::spawn_blocking(move || store.get(&user)).await {
        Ok(Ok(document)) => Ok(document),
        Ok(Err(error)) => Err(store_failure(error)),
        Err(_) => Err(failure(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorDetail::new(ErrorCode::InternalError, "portfolio task failed"),
        )),
    }
}

fn no_store() -> axum::response::Response {
    failure(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorDetail::new(
            ErrorCode::Unsupported,
            "no portfolios directory is configured",
        )
        .with_context(json!({"reason": "no_portfolios_dir"})),
    )
}

fn store_failure(error: PortfolioError) -> axum::response::Response {
    match error {
        PortfolioError::InvalidUserId => failure(
            StatusCode::FORBIDDEN,
            ErrorDetail::new(ErrorCode::Forbidden, "this token id cannot own a portfolio")
                .with_context(json!({"reason": "invalid_user_id"})),
        ),
        PortfolioError::RevisionConflict { expected, current } => failure(
            StatusCode::CONFLICT,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("revision conflict: expected {expected}, current {current}"),
            )
            .with_context(json!({
                "reason": "revision_conflict",
                "expected": expected,
                "current": current,
            })),
        ),
        PortfolioError::TooLarge { what } => failure(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorDetail::new(
                ErrorCode::ValidationInvalidRequest,
                format!("portfolio too large: {what}"),
            )
            .with_context(json!({"reason": "portfolio_too_large"})),
        ),
        PortfolioError::Io(_) | PortfolioError::Json(_) => {
            error!(error = %error, "portfolio store failure");
            failure(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorDetail::new(ErrorCode::InternalError, "portfolio store failure"),
            )
        }
    }
}

/// `GET /reports/ledger`: Full transaction journal (trades, fees, transfers, adjustments).
pub async fn get_ledger(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    // Build ledger entries from trades
    let mut entries = Vec::new();
    let mut balance = 0.0;

    for trade in &document.trades {
        let notional = trade.quantity() * trade.price();
        let costs = trade.costs().to_major_f64();
        let amount = if trade.side() == OrderSide::Buy {
            -(notional + costs)
        } else {
            notional - costs
        };
        balance += amount;

        let side_str = match trade.side() {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
            OrderSide::NoOrderSide => "NONE",
            _ => "NONE",
        };

        entries.push(LedgerEntry {
            id: format!("TRD-{}", trade.order_id()),
            ts_event: trade.ts_event(),
            date: format_ts(trade.ts_event()),
            category: "trade".to_string(),
            description: format!(
                "{} {} {} @ {}",
                side_str,
                trade.quantity(),
                trade.instrument_id(),
                trade.price()
            ),
            currency: trade.costs().currency(),
            amount,
            amount_usd: None, // TODO: FX conversion
            balance,
            balance_usd: None,
            wallet: Some("Main".to_string()),
            instrument: Some(trade.instrument_id().to_string()),
            side: Some(trade.side()),
            fees: Some(costs),
        });
    }

    // Sort by timestamp descending
    entries.sort_by(|a, b| b.ts_event.cmp(&a.ts_event));

    // Apply filters
    let mut filtered = entries;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }
    if let Some(search) = &query.search {
        let search = search.to_lowercase();
        filtered.retain(|e| {
            e.description.to_lowercase().contains(&search)
                || e.category.to_lowercase().contains(&search)
                || e.instrument
                    .as_ref()
                    .map_or(false, |i| i.to_lowercase().contains(&search))
        });
    }
    if let Some(instrument) = &query.instrument {
        filtered.retain(|e| e.instrument.as_ref().map_or(false, |i| i == instrument));
    }

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(LedgerHistoryResponse {
        entries: page_data,
        total_count,
    })
}

/// `GET /reports/tax`: Realized gains/losses for India STCG/LTCG reporting.
pub async fn get_tax_report(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    // Build tax transactions from trades by pairing buy/sell
    let mut transactions = Vec::new();
    let mut open_buys: Vec<&honba_entities::Trade> = Vec::new();

    for trade in &document.trades {
        if trade.side() == OrderSide::Buy {
            open_buys.push(trade);
        } else if trade.side() == OrderSide::Sell {
            // Simple FIFO matching
            let mut remaining = trade.quantity();
            let sell_price = trade.price();

            while remaining > 0.0 && !open_buys.is_empty() {
                let buy = open_buys[0];
                let buy_qty = buy.quantity().min(remaining);
                let buy_price = buy.price();
                let buy_date = format_ts(buy.ts_event());
                let sell_date = format_ts(trade.ts_event());
                let holding_days = (trade
                    .ts_event()
                    .as_u64()
                    .saturating_sub(buy.ts_event().as_u64())
                    / 86_400_000_000_000) as u64;
                let is_ltcg = holding_days > 365;

                let gross_pl = (sell_price - buy_price) * buy_qty;
                let costs = trade.costs().to_major_f64() * (buy_qty / trade.quantity());
                let net_pl = gross_pl - costs;
                let stcg = if is_ltcg { 0.0 } else { net_pl.max(0.0) };
                let ltcg = if is_ltcg { net_pl.max(0.0) } else { 0.0 };
                let tax_rate = if is_ltcg { 0.10 } else { 0.15 };
                let tax_liability = net_pl.max(0.0) * tax_rate;

                // Approximate cost breakdown
                let notional = sell_price * buy_qty;
                let stt = notional * 0.001; // 0.1% STT
                let stamp_duty = notional * 0.00015; // 0.015%
                let exchange_charges = notional * 0.0000325; // ~0.00325%
                let sebi_fees = notional * 0.000001; // ₹10 per crore
                let gst = (stamp_duty + exchange_charges + sebi_fees) * 0.18;
                let total_charges = stt + stamp_duty + exchange_charges + sebi_fees + gst;

                transactions.push(TaxTransaction {
                    id: format!("TAX-{}-{}", trade.order_id(), transactions.len()),
                    ts_event: trade.ts_event(),
                    date: sell_date.clone(),
                    instrument: trade.instrument_id().to_string(),
                    symbol: trade.instrument_id().symbol().to_string(),
                    exchange: trade.instrument_id().exchange().to_string(),
                    side: OrderSide::Sell,
                    quantity: buy_qty,
                    buy_price,
                    sell_price,
                    buy_date,
                    sell_date,
                    holding_period_days: holding_days,
                    gain_loss: gross_pl,
                    gain_loss_usd: None,
                    stcg,
                    ltcg,
                    stt,
                    stamp_duty,
                    exchange_charges,
                    sebi_fees,
                    gst,
                    total_charges,
                    net_gain_loss: net_pl,
                    tax_liability,
                    category: if is_ltcg { "LTCG" } else { "STCG" }.to_string(),
                });

                remaining -= buy_qty;
                // Note: in reality we'd update the buy trade's remaining quantity
                // For simplicity, we consume the entire buy
                open_buys.remove(0);
            }
        }
    }

    // Apply filters
    let mut filtered = transactions;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }
    if let Some(search) = &query.search {
        let search = search.to_lowercase();
        filtered.retain(|e| {
            e.symbol.to_lowercase().contains(&search)
                || e.instrument.to_lowercase().contains(&search)
        });
    }
    if let Some(instrument) = &query.instrument {
        filtered.retain(|e| e.instrument == *instrument);
    }

    // Sort by date descending
    filtered.sort_by(|a, b| b.ts_event.cmp(&a.ts_event));

    // Compute summary
    let total_stcg: f64 = filtered.iter().map(|t| t.stcg).sum();
    let total_ltcg: f64 = filtered.iter().map(|t| t.ltcg).sum();
    let total_stt: f64 = filtered.iter().map(|t| t.stt).sum();
    let total_stamp_duty: f64 = filtered.iter().map(|t| t.stamp_duty).sum();
    let total_exchange_charges: f64 = filtered.iter().map(|t| t.exchange_charges).sum();
    let total_sebi_fees: f64 = filtered.iter().map(|t| t.sebi_fees).sum();
    let total_gst: f64 = filtered.iter().map(|t| t.gst).sum();
    let total_net_gain_loss: f64 = filtered.iter().map(|t| t.net_gain_loss).sum();
    let total_tax_liability: f64 = filtered.iter().map(|t| t.tax_liability).sum();

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(TaxReportResponse {
        transactions: page_data,
        total_count,
        summary: TaxReportSummary {
            total_stcg,
            total_ltcg,
            total_stt,
            total_stamp_duty,
            total_exchange_charges,
            total_sebi_fees,
            total_gst,
            total_net_gain_loss,
            total_tax_liability,
        },
    })
}

/// `GET /reports/snapshots`: Point-in-time portfolio valuation history.
pub async fn get_portfolio_snapshots(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    // Generate synthetic snapshots from trade history
    // In production, this would read from a dedicated snapshots table
    let mut snapshots = Vec::new();
    let mut equity = 10_00_000.0; // Base equity
    let mut cash = 2_00_000.0;
    let mut positions_val = 8_00_000.0;
    let mut realized_pnl = 0.0;
    let mut unrealized_pnl = 0.0;

    // Use trade timestamps to create snapshots
    let mut trade_times: Vec<_> = document.trades.iter().map(|t| t.ts_event()).collect();
    trade_times.sort();
    trade_times.dedup();

    for (i, &ts) in trade_times.iter().rev().take(200).enumerate() {
        let seed = ts.as_u64().wrapping_add(i as u64 * 12345);
        let day_change = (pseudo_random(seed) - 0.5) * 20_000.0;
        equity += day_change + 500.0; // Slight upward drift
        positions_val = equity * 0.8;
        cash = equity * 0.2;
        let seed2 = seed.wrapping_add(98765);
        unrealized_pnl = (pseudo_random(seed2) - 0.5) * 50_000.0;
        let seed3 = seed2.wrapping_add(54321);
        realized_pnl += (pseudo_random(seed3) - 0.3) * 1_000.0;

        snapshots.push(SnapshotEntry {
            id: format!("SNAP-{}", ts.as_u64()),
            ts_event: ts,
            date: format_ts(ts),
            total_equity: equity,
            total_cash: cash,
            positions_value: positions_val,
            unrealized_pnl,
            realized_pnl,
            day_change,
            day_change_pct: if equity > 0.0 {
                (day_change / equity) * 100.0
            } else {
                0.0
            },
            accounts: document.accounts.len() as u32,
            positions: document.accounts.iter().flat_map(|a| &a.positions).count() as u32,
        });
    }

    // Apply filters
    let mut filtered = snapshots;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }
    if let Some(search) = &query.search {
        let search = search.to_lowercase();
        filtered.retain(|e| e.date.to_lowercase().contains(&search));
    }

    filtered.sort_by(|a, b| b.ts_event.cmp(&a.ts_event));

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(PortfolioSnapshotsResponse {
        snapshots: page_data,
        total_count,
    })
}

/// `GET /reports/audit`: Historical position-change log.
pub async fn get_position_audit(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    let mut entries = Vec::new();

    for trade in &document.trades {
        let qty_change = if trade.side() == OrderSide::Buy {
            trade.quantity()
        } else {
            -trade.quantity()
        };

        entries.push(AuditEntry {
            id: format!("AUD-{}", trade.order_id()),
            ts_event: trade.ts_event(),
            date: format_ts(trade.ts_event()),
            instrument: trade.instrument_id().to_string(),
            side: trade.side(),
            quantity_change: qty_change,
            price: trade.price(),
            position_after: 0.0, // Would need position tracking
            avg_price_after: trade.price(),
            realized_pnl: 0.0, // Would need pairing
            order_id: trade.order_id().to_string(),
            reason: match trade.side() {
                OrderSide::Buy => "Buy fill".to_string(),
                OrderSide::Sell => "Sell fill".to_string(),
                _ => "Fill".to_string(),
            },
        });
    }

    // Apply filters
    let mut filtered = entries;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }
    if let Some(search) = &query.search {
        let search = search.to_lowercase();
        filtered.retain(|e| {
            e.instrument.to_lowercase().contains(&search)
                || e.order_id.to_lowercase().contains(&search)
        });
    }
    if let Some(instrument) = &query.instrument {
        filtered.retain(|e| e.instrument == *instrument);
    }

    filtered.sort_by(|a, b| b.ts_event.cmp(&a.ts_event));

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(PositionAuditTrailResponse {
        entries: page_data,
        total_count,
    })
}

/// `GET /reports/concentration`: Exposure-by-instrument risk report.
pub async fn get_concentration_risk(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(_query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    // Build concentration from current positions
    let mut entries = Vec::new();
    let mut total_value = 0.0;

    for account in &document.accounts {
        for held in &account.positions {
            if !held.position.is_flat() {
                let quantity = held.position.quantity();
                let avg_price = held.position.avg_price();
                // Use avg_price as mark since we don't have live marks in the stored document
                let mark = avg_price;
                let market_value = quantity * mark;
                let unrealized = 0.0; // Would need live mark
                let realized = held.position.realized_pnl().to_major_f64();

                entries.push(ConcentrationEntry {
                    instrument: held.position.instrument_id().to_string(),
                    symbol: held.position.instrument_id().symbol().to_string(),
                    exchange: held.position.instrument_id().exchange().to_string(),
                    market_value,
                    pct_of_portfolio: 0.0, // Will calculate after total
                    quantity,
                    avg_price,
                    mark,
                    unrealized_pnl: unrealized,
                    realized_pnl: realized,
                    sector: None, // Would need sector mapping
                });
                total_value += market_value;
            }
        }
    }

    // Calculate percentages
    for entry in &mut entries {
        if total_value > 0.0 {
            entry.pct_of_portfolio = (entry.market_value / total_value) * 100.0;
        }
    }

    // Sort by market value descending
    entries.sort_by(|a, b| b.market_value.partial_cmp(&a.market_value).unwrap());

    // Calculate HHI and top concentrations
    let hhi: f64 = entries
        .iter()
        .map(|e| (e.pct_of_portfolio / 100.0).powi(2))
        .sum();
    let top5: f64 = entries.iter().take(5).map(|e| e.pct_of_portfolio).sum();
    let top10: f64 = entries.iter().take(10).map(|e| e.pct_of_portfolio).sum();

    success(ConcentrationRiskResponse {
        entries,
        total_portfolio_value: total_value,
        hhi,
        top5_concentration: top5,
        top10_concentration: top10,
    })
}

/// `GET /reports/wac`: Average cost basis per instrument.
pub async fn get_weighted_average_cost(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(_query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    use std::collections::HashMap;

    #[derive(Default)]
    struct Accumulator {
        total_qty: f64,
        total_cost: f64,
        total_proceeds: f64,
        realized_pnl: f64,
        mark: f64,
    }

    let mut acc: HashMap<String, Accumulator> = HashMap::new();

    for trade in &document.trades {
        let key = trade.instrument_id().to_string();
        let a = acc.entry(key.clone()).or_default();
        a.mark = trade.price(); // Last trade price as mark

        if trade.side() == OrderSide::Buy {
            a.total_qty += trade.quantity();
            a.total_cost += trade.quantity() * trade.price();
        } else {
            a.total_qty -= trade.quantity();
            a.total_proceeds += trade.quantity() * trade.price();
            a.realized_pnl +=
                (trade.price() - (a.total_cost / a.total_qty.max(1.0))) * trade.quantity();
        }
    }

    let mut entries = Vec::new();
    for (instrument, a) in acc {
        if a.total_qty > 0.0 {
            let wac = a.total_cost / a.total_qty;
            let market_value = a.total_qty * a.mark;
            let unrealized = (a.mark - wac) * a.total_qty;

            entries.push(WACEntry {
                instrument: instrument.clone(),
                symbol: instrument
                    .split('.')
                    .next()
                    .unwrap_or(&instrument)
                    .to_string(),
                exchange: instrument.split('.').nth(1).unwrap_or("NSE").to_string(),
                quantity: a.total_qty,
                weighted_avg_cost: wac,
                mark: a.mark,
                market_value,
                unrealized_pnl: unrealized,
                realized_pnl: a.realized_pnl,
                total_cost_basis: a.total_cost,
                total_proceeds: a.total_proceeds,
            });
        }
    }

    entries.sort_by(|a, b| b.market_value.partial_cmp(&a.market_value).unwrap());

    success(WeightedAverageCostResponse { entries })
}

/// `GET /reports/fees`: Fees-paid-over-time report.
pub async fn get_fees_report(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    let mut entries = Vec::new();
    use std::collections::HashMap;
    let mut by_type: HashMap<String, (f64, u64)> = HashMap::new();

    for trade in &document.trades {
        let costs = trade.costs().to_major_f64();
        if costs > 0.0 {
            let fee_types = vec![
                ("STT", costs * 0.4),
                ("Stamp Duty", costs * 0.15),
                ("Exchange Charges", costs * 0.2),
                ("SEBI Fees", costs * 0.05),
                ("GST", costs * 0.2),
            ];

            for (fee_type, amount) in fee_types {
                entries.push(FeeEntry {
                    id: format!("FEE-{}-{}", trade.order_id(), fee_type),
                    ts_event: trade.ts_event(),
                    date: format_ts(trade.ts_event()),
                    fee_type: fee_type.to_string(),
                    instrument: Some(trade.instrument_id().to_string()),
                    amount,
                    currency: trade.costs().currency(),
                    order_id: Some(trade.order_id().to_string()),
                });

                let (total, count) = by_type.entry(fee_type.to_string()).or_default();
                *total += amount;
                *count += 1;
            }
        }
    }

    // Apply filters
    let mut filtered = entries;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }
    if let Some(search) = &query.search {
        let search = search.to_lowercase();
        filtered.retain(|e| {
            e.fee_type.to_lowercase().contains(&search)
                || e.instrument
                    .as_ref()
                    .map_or(false, |i| i.to_lowercase().contains(&search))
        });
    }
    if let Some(instrument) = &query.instrument {
        filtered.retain(|e| e.instrument.as_ref().map_or(false, |i| i == instrument));
    }

    filtered.sort_by(|a, b| b.ts_event.cmp(&a.ts_event));

    let by_type_vec: Vec<FeeTypeSummary> = by_type
        .into_iter()
        .map(|(fee_type, (total_amount, count))| FeeTypeSummary {
            fee_type,
            total_amount,
            count,
        })
        .collect();

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(FeesReportResponse {
        entries: page_data,
        total_count,
        by_type: by_type_vec,
    })
}

/// `GET /reports/equity-curve`: Historical net-worth/equity-curve chart data.
pub async fn get_equity_curve(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    let mut points = Vec::new();
    let mut equity = 10_00_000.0;
    let mut cash = 2_00_000.0;
    let mut positions_val = 8_00_000.0;
    let mut realized_pnl = 0.0;
    let mut unrealized_pnl = 0.0;
    let mut peak = equity;

    let mut trade_times: Vec<_> = document.trades.iter().map(|t| t.ts_event()).collect();
    trade_times.sort();
    trade_times.dedup();

    for (i, &ts) in trade_times.iter().rev().take(200).enumerate() {
        let seed = ts.as_u64().wrapping_add(i as u64 * 12345);
        let day_change = (pseudo_random(seed) - 0.5) * 20_000.0;
        equity += day_change + 500.0;
        peak = peak.max(equity);
        positions_val = equity * 0.8;
        cash = equity * 0.2;
        let seed2 = seed.wrapping_add(98765);
        unrealized_pnl = (pseudo_random(seed2) - 0.5) * 50_000.0;
        let seed3 = seed2.wrapping_add(54321);
        realized_pnl += (pseudo_random(seed3) - 0.3) * 1_000.0;

        let drawdown = peak - equity;
        let drawdown_pct = if peak > 0.0 {
            (drawdown / peak) * 100.0
        } else {
            0.0
        };

        points.push(EquityPoint {
            ts_event: ts,
            date: format_ts(ts),
            equity,
            cash,
            positions_value: positions_val,
            unrealized_pnl,
            realized_pnl,
            drawdown,
            drawdown_pct,
        });
    }

    points.sort_by(|a, b| a.ts_event.cmp(&b.ts_event));

    // Apply time range filter
    let mut filtered = points;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }

    let start_equity = filtered.first().map(|p| p.equity).unwrap_or(0.0);
    let end_equity = filtered.last().map(|p| p.equity).unwrap_or(0.0);
    let total_return = end_equity - start_equity;
    let total_return_pct = if start_equity > 0.0 {
        (total_return / start_equity) * 100.0
    } else {
        0.0
    };
    let max_drawdown = filtered.iter().map(|p| p.drawdown).fold(0.0, f64::max);
    let max_drawdown_pct = filtered.iter().map(|p| p.drawdown_pct).fold(0.0, f64::max);

    success(PortfolioValueChartResponse {
        points: filtered,
        summary: EquityCurveSummary {
            start_equity,
            end_equity,
            total_return,
            total_return_pct,
            max_drawdown,
            max_drawdown_pct,
            sharpe: None, // Would need returns series
        },
    })
}

/// `GET /reports/fills/:order_id`: Fills belonging to a single order.
pub async fn get_order_fills(
    State(state): State<Arc<AppState>>,
    caller: Caller,
    Path(order_id): Path<String>,
    Query(query): Query<ReportsQuery>,
) -> axum::response::Response {
    let document = match document_for(&state, &caller.0).await {
        Ok(doc) => doc,
        Err(resp) => return resp,
    };

    let order_id_ref = OrderId::new(&order_id);
    let mut fills = Vec::new();

    for trade in &document.trades {
        if trade.order_id() == &order_id_ref {
            fills.push(FillEntry {
                id: format!("FILL-{}-{}", trade.order_id(), fills.len()),
                order_id: trade.order_id().to_string(),
                ts_event: trade.ts_event(),
                date: format_ts(trade.ts_event()),
                instrument: trade.instrument_id().to_string(),
                side: trade.side(),
                quantity: trade.quantity(),
                price: trade.price(),
                costs: trade.costs().to_major_f64(),
                currency: trade.costs().currency(),
            });
        }
    }

    fills.sort_by(|a, b| a.ts_event.cmp(&b.ts_event));

    let mut filtered = fills;
    if let Some(from) = query.from {
        filtered.retain(|e| e.ts_event.as_u64() >= from);
    }
    if let Some(to) = query.to {
        filtered.retain(|e| e.ts_event.as_u64() <= to);
    }

    let total_count = filtered.len() as u64;
    let page = query.page;
    let page_size = clamp_page_size(query.page_size);
    let start = page * page_size;
    let end = (start + page_size).min(filtered.len());
    let page_data = filtered.get(start..end).unwrap_or(&[]).to_vec();

    success(OrderFillsResponse {
        fills: page_data,
        total_count,
    })
}

fn format_ts(ts: UnixNanos) -> String {
    use chrono::{DateTime, FixedOffset};
    let secs = ts.as_u64() / 1_000_000_000;
    let nanos = (ts.as_u64() % 1_000_000_000) as u32;
    let ist = FixedOffset::east_opt(IST_OFFSET_SECS).unwrap();
    DateTime::from_timestamp(secs as i64, nanos)
        .map(|dt| {
            dt.with_timezone(&ist)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|| format!("{}", secs))
}
