//! The engine's audit trail.

use std::collections::HashMap;

use honba_messages::{
    Exchange, IllegalTransition, InstrumentId, OrderEventKind, OrderSide, VenueOrderId,
};
use honba_risk::RiskRefusal;

use crate::state::TradingState;

/// One entry of an [`AuditLog`].
///
/// `seq` is monotonic and starts at zero, so a log can be compared and
/// diffed across runs without depending on wall-clock time.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    /// The position of this record in the log, counting from zero.
    pub seq: u64,
    /// What happened.
    pub kind: AuditKind,
}

/// What the engine did, and in what order it did it.
///
/// The engine records one entry per dispatched message and one entry per
/// command it applied, so the log is the ordered account of a run: what was
/// dispatched, what was submitted, refused, cancelled or filled, and how the
/// trading state moved.
#[derive(Debug, Clone, PartialEq)]
pub enum AuditKind {
    /// A queued message was popped and dispatched to the handlers.
    EventDispatched {
        /// `ts_event` of the dispatched message.
        ts_event: u64,
    },
    /// The execution sink accepted an order.
    OrderSubmitted {
        /// The client order identifier.
        order_id: String,
        /// The instrument, rendered as `"{symbol}.{exchange}"`.
        instrument: String,
        /// The side, rendered as `"buy"`, `"sell"` or `"no_order_side"`.
        side: String,
    },
    /// The risk gate refused an order (ADR 0018 decision 6). Always followed by the
    /// [`AuditKind::OrderRejected`] for the same order, whose `reason` is
    /// `refusal.error_code()`'s wire spelling.
    RiskRefused {
        /// The client order identifier.
        order_id: String,
        /// The typed refusal, with the numbers that caused it.
        refusal: RiskRefusal,
    },
    /// The engine refused to submit an order (a pre-gate refusal, ADR 0019
    /// decision 5); `reason` is the `ErrorCode` wire spelling, also carried by
    /// the `ExecutionEvent::Rejected` the engine enqueues.
    OrderRejected {
        /// The client order identifier.
        order_id: String,
        /// Why it was refused.
        reason: String,
    },
    /// A cancel was asked for and routed to the execution sink (formerly
    /// `OrderCancelled`, which never meant the venue had cancelled). Recorded
    /// once per order: repeated cancels are no-ops (ADR 0019 decision 6).
    CancelRequested {
        /// The client order identifier.
        order_id: String,
    },
    /// The execution sink reported a non-fill lifecycle event (accepted,
    /// rejected, cancelled, expired) that the engine applied and translated.
    /// Fills are [`AuditKind::FillProduced`]; the submitter's own
    /// `Submitted`/`CancelRequested` and pre-gate rejections have their own
    /// records.
    OrderLifecycle {
        /// The client order identifier.
        order_id: String,
        /// What happened.
        event: OrderEventKind,
        /// The kernel clock value stamped on the translated message.
        ts_event: u64,
    },
    /// An event the order-state machine refused: the state was not changed
    /// and no message was emitted (an illegal fill is still booked into the
    /// position map). In sims and tests this is a bug; live, it is flagged for
    /// reconciliation (E2-S7).
    IllegalTransition {
        /// The client order identifier.
        order_id: String,
        /// Why the transition was refused.
        error: IllegalTransition,
    },
    /// An event named a different venue order id than the one first recorded
    /// for the order; the recorded one is kept (ADR 0019 decision 3).
    VenueOrderIdDrift {
        /// The client order identifier.
        order_id: String,
        /// The id recorded first.
        recorded: VenueOrderId,
        /// The different id the event carried.
        received: VenueOrderId,
    },
    /// The execution sink produced a fill, which the engine turned back into
    /// an [`Event::OrderPartiallyFilled`](honba_messages::Event::OrderPartiallyFilled)
    /// or [`Event::OrderFilled`](honba_messages::Event::OrderFilled) message.
    FillProduced {
        /// The client order identifier.
        order_id: String,
        /// The filled quantity.
        quantity: f64,
        /// The fill price.
        price: f64,
        /// The kernel clock value stamped on the message.
        ts_event: u64,
    },
    /// The trading state changed.
    StateChanged {
        /// The state the engine left.
        from: TradingState,
        /// The state the engine entered.
        to: TradingState,
    },
    /// A duplicate fill was ignored because its content fingerprint was already recorded
    /// in the idempotent fill ledger (E2-S11).
    DuplicateFillIgnored {
        /// The client order identifier.
        order_id: String,
        /// The content fingerprint of the fill.
        fingerprint: String,
    },
    /// An AI/LLM decision on an order or intent was recorded (E5-S7).
    LlmDecisionRecorded {
        /// The client order identifier.
        order_id: String,
        /// The decision kind: "approve", "veto", or "size_down".
        decision: String,
        /// The original proposed quantity.
        original_qty: f64,
        /// The decided quantity (0.0 for veto, <= original for size_down).
        decided_qty: f64,
        /// Explanation provided by the LLM.
        rationale: String,
    },
}

/// An in-memory, append-only audit trail.
///
/// Recording is synchronous and order-preserving: [`AuditLog::record`] assigns
/// the next sequence number and appends, so the log reads back in exactly the
/// order things happened.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AuditLog {
    records: Vec<AuditRecord>,
    next_seq: u64,
}

impl AuditLog {
    /// Creates an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a record and returns the sequence number assigned to it.
    ///
    /// The first record of a log gets sequence number `0`, and every later
    /// record gets the next one, whatever its [`AuditKind`].
    pub fn record(&mut self, kind: AuditKind) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.records.push(AuditRecord { seq, kind });
        seq
    }

    /// Appends a record with an explicit sequence number.
    pub fn record_with_seq(&mut self, seq: u64, kind: AuditKind) {
        self.next_seq = self.next_seq.max(seq.wrapping_add(1));
        self.records.push(AuditRecord { seq, kind });
    }

    /// Returns the records in the order they were recorded.
    pub fn records(&self) -> &[AuditRecord] {
        &self.records
    }

    /// Returns the number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns `true` if nothing has been recorded yet.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Records an LLM decision into the audit log (E5-S7).
    pub fn record_llm_decision(&mut self, decision: &crate::decision::LlmDecision) -> u64 {
        self.record(AuditKind::LlmDecisionRecorded {
            order_id: decision.order_id.clone(),
            decision: match decision.decision {
                crate::decision::LlmDecisionKind::Approve => "approve".to_string(),
                crate::decision::LlmDecisionKind::Veto => "veto".to_string(),
                crate::decision::LlmDecisionKind::SizeDown => "size_down".to_string(),
            },
            original_qty: decision.original_quantity,
            decided_qty: decision.decided_quantity,
            rationale: decision.rationale.clone(),
        })
    }

    /// Replays this log to reconstruct the final engine state.
    pub fn replay(&self) -> ReplayState {
        ReplayState::from_log(self)
    }

    /// Writes all records as newline-delimited JSON to `writer`.
    pub fn write_ndjson<W: std::io::Write>(&self, writer: W) -> std::io::Result<()> {
        AuditJournalWriter::new(writer).write_log(self)
    }

    /// Reads an audit log from newline-delimited JSON.
    pub fn read_ndjson<R: std::io::BufRead>(reader: R) -> std::io::Result<Self> {
        AuditJournalReader::new(reader).read_log()
    }
}

/// An LLM decision reconstructed from audit replay (E5-S7).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplayLlmDecision {
    /// The client order identifier.
    pub order_id: String,
    /// The decision kind ("approve", "veto", "size_down").
    pub decision: String,
    /// The original proposed quantity.
    pub original_qty: f64,
    /// The decided quantity.
    pub decided_qty: f64,
    /// Rationale provided by the LLM.
    pub rationale: String,
}

/// The engine state reconstructed by replaying an [`AuditLog`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReplayState {
    /// Net position per instrument.
    positions: HashMap<InstrumentId, f64>,
    /// Orders submitted: order_id -> ReplayOrder.
    orders: HashMap<String, ReplayOrder>,
    /// The final trading state.
    trading_state: TradingState,
    /// Number of event dispatches observed in the log.
    dispatches: u64,
    /// Rejections per order_id -> reason.
    rejections: HashMap<String, String>,
    /// Reconstructed LLM decisions by order identifier.
    llm_decisions: HashMap<String, ReplayLlmDecision>,
}

/// The state of an order reconstructed during audit replay.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayOrder {
    /// The client order identifier.
    pub order_id: String,
    /// The instrument.
    pub instrument: InstrumentId,
    /// The order side.
    pub side: OrderSide,
    /// The cumulative filled quantity.
    pub filled_qty: f64,
    /// The latest lifecycle event kind, if any.
    pub lifecycle: Option<OrderEventKind>,
    /// Whether a cancel was requested.
    pub cancel_requested: bool,
}

fn parse_instrument(s: &str) -> InstrumentId {
    if let Some((symbol, exchange)) = s.split_once('.') {
        InstrumentId::new(symbol, Exchange::new(exchange))
    } else {
        InstrumentId::new(s, Exchange::new("UNKNOWN"))
    }
}

impl ReplayState {
    /// Rebuilds the final state by replaying all records in `log`.
    pub fn from_log(log: &AuditLog) -> Self {
        Self::from_records(log.records())
    }

    /// Rebuilds the final state by replaying an iterator of audit records.
    pub fn from_records<'a>(records: impl IntoIterator<Item = &'a AuditRecord>) -> Self {
        let mut state = Self {
            trading_state: TradingState::Active,
            ..Self::default()
        };
        for record in records {
            state.apply(&record.kind);
        }
        state
    }

    /// Applies one audit record to advance the state.
    pub fn apply(&mut self, kind: &AuditKind) {
        match kind {
            AuditKind::EventDispatched { .. } => {
                self.dispatches += 1;
            }
            AuditKind::OrderSubmitted {
                order_id,
                instrument,
                side,
            } => {
                let inst = parse_instrument(instrument);
                let s = match side.as_str() {
                    "buy" => OrderSide::Buy,
                    "sell" => OrderSide::Sell,
                    _ => OrderSide::NoOrderSide,
                };
                self.orders.insert(
                    order_id.clone(),
                    ReplayOrder {
                        order_id: order_id.clone(),
                        instrument: inst,
                        side: s,
                        filled_qty: 0.0,
                        lifecycle: None,
                        cancel_requested: false,
                    },
                );
            }
            AuditKind::RiskRefused { order_id, refusal } => {
                self.rejections
                    .insert(order_id.clone(), refusal.error_code().as_str().to_string());
            }
            AuditKind::OrderRejected { order_id, reason } => {
                self.rejections.insert(order_id.clone(), reason.clone());
            }
            AuditKind::CancelRequested { order_id } => {
                if let Some(order) = self.orders.get_mut(order_id) {
                    order.cancel_requested = true;
                }
            }
            AuditKind::OrderLifecycle {
                order_id, event, ..
            } => {
                if let Some(order) = self.orders.get_mut(order_id) {
                    order.lifecycle = Some(*event);
                    if matches!(
                        *event,
                        OrderEventKind::Cancelled
                            | OrderEventKind::Rejected
                            | OrderEventKind::Expired
                    ) {
                        order.cancel_requested = false;
                    }
                }
            }
            AuditKind::FillProduced {
                order_id, quantity, ..
            } => {
                if let Some(order) = self.orders.get_mut(order_id) {
                    order.filled_qty += quantity;
                    let signed = match order.side {
                        OrderSide::Sell => -quantity,
                        _ => *quantity,
                    };
                    *self
                        .positions
                        .entry(order.instrument.clone())
                        .or_insert(0.0) += signed;
                }
            }
            AuditKind::StateChanged { to, .. } => {
                self.trading_state = *to;
            }
            AuditKind::LlmDecisionRecorded {
                order_id,
                decision,
                original_qty,
                decided_qty,
                rationale,
            } => {
                self.llm_decisions.insert(
                    order_id.clone(),
                    ReplayLlmDecision {
                        order_id: order_id.clone(),
                        decision: decision.clone(),
                        original_qty: *original_qty,
                        decided_qty: *decided_qty,
                        rationale: rationale.clone(),
                    },
                );
            }
            AuditKind::IllegalTransition { .. }
            | AuditKind::VenueOrderIdDrift { .. }
            | AuditKind::DuplicateFillIgnored { .. } => {}
        }
    }

    /// Net position in `instrument` (0.0 if not held).
    pub fn position(&self, instrument: &InstrumentId) -> f64 {
        self.positions.get(instrument).copied().unwrap_or(0.0)
    }

    /// Net positions map.
    pub fn positions(&self) -> &HashMap<InstrumentId, f64> {
        &self.positions
    }

    /// Reconstructed order by client order id, if any.
    pub fn order(&self, order_id: &str) -> Option<&ReplayOrder> {
        self.orders.get(order_id)
    }

    /// All tracked orders.
    pub fn orders(&self) -> &HashMap<String, ReplayOrder> {
        &self.orders
    }

    /// The final trading state.
    pub fn trading_state(&self) -> TradingState {
        self.trading_state
    }

    /// Total event dispatches observed in the log.
    pub fn dispatches(&self) -> u64 {
        self.dispatches
    }

    /// Rejection reason for `order_id`, if rejected.
    pub fn rejection(&self, order_id: &str) -> Option<&str> {
        self.rejections.get(order_id).map(|s| s.as_str())
    }

    /// Reconstructed LLM decisions map.
    pub fn llm_decisions(&self) -> &HashMap<String, ReplayLlmDecision> {
        &self.llm_decisions
    }

    /// Looks up an LLM decision by order identifier.
    pub fn llm_decision(&self, order_id: &str) -> Option<&ReplayLlmDecision> {
        self.llm_decisions.get(order_id)
    }
}

/// A journal writer that appends audit records to an output stream as NDJSON.
pub struct AuditJournalWriter<W: std::io::Write> {
    writer: std::io::BufWriter<W>,
}

impl<W: std::io::Write> AuditJournalWriter<W> {
    /// Creates a writer wrapping `writer`.
    pub fn new(writer: W) -> Self {
        Self {
            writer: std::io::BufWriter::new(writer),
        }
    }

    /// Writes one record as a JSON line.
    pub fn write_record(&mut self, record: &AuditRecord) -> std::io::Result<()> {
        use std::io::Write;
        let val = record.to_json();
        serde_json::to_writer(&mut self.writer, &val)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.writer.write_all(b"\n")
    }

    /// Writes all records from `log` and flushes the writer.
    pub fn write_log(&mut self, log: &AuditLog) -> std::io::Result<()> {
        use std::io::Write;
        for record in log.records() {
            self.write_record(record)?;
        }
        self.writer.flush()
    }
}

/// A journal reader that parses audit records from NDJSON.
pub struct AuditJournalReader<R: std::io::BufRead> {
    reader: R,
}

impl<R: std::io::BufRead> AuditJournalReader<R> {
    /// Creates a reader wrapping `reader`.
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Reads all records into an [`AuditLog`].
    pub fn read_log(&mut self) -> std::io::Result<AuditLog> {
        let mut log = AuditLog::new();
        let mut line = String::new();
        while self.reader.read_line(&mut line)? > 0 {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let val: serde_json::Value = serde_json::from_str(trimmed)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let record = AuditRecord::from_json(&val)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                log.record_with_seq(record.seq, record.kind);
            }
            line.clear();
        }
        Ok(log)
    }
}

/// Header metadata for a Journal v1 stream (ADR 0020, E4-S1).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JournalHeader {
    /// Wire schema version (e.g. ).
    pub schema_version: u32,
    /// Format version of the journal layout (always 1 for v1).
    pub journal_version: u32,
    /// PRNG seed used for the run.
    pub seed: u64,
    /// Hash of the run configuration.
    pub config_hash: String,
    /// Dataset identifier or fingerprint.
    pub dataset_id: String,
    /// Git revision, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_rev: Option<String>,
    /// Trial count / index, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trial_count: Option<u32>,
}

/// A version 1 journal containing a provenance metadata header followed by audit records.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalV1 {
    /// Header metadata.
    pub header: JournalHeader,
    /// The audit log records.
    pub log: AuditLog,
}

impl JournalV1 {
    /// Creates a new journal from header and log.
    pub fn new(header: JournalHeader, log: AuditLog) -> Self {
        Self { header, log }
    }

    /// Creates a new journal from header and a reference to an audit log.
    pub fn from_audit_log(header: JournalHeader, log: &AuditLog) -> Self {
        Self {
            header,
            log: log.clone(),
        }
    }

    /// Writes the journal (header followed by audit records) as NDJSON to writer.
    pub fn write_ndjson<W: std::io::Write>(&self, writer: W) -> std::io::Result<()> {
        use std::io::Write;
        let mut buf_writer = std::io::BufWriter::new(writer);

        let mut header_val = serde_json::to_value(&self.header)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(obj) = header_val.as_object_mut() {
            obj.insert("type".to_string(), serde_json::json!("header"));
        }
        serde_json::to_writer(&mut buf_writer, &header_val)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        buf_writer.write_all(b"\n")?;

        for record in self.log.records() {
            let val = record.to_json();
            serde_json::to_writer(&mut buf_writer, &val)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            buf_writer.write_all(b"\n")?;
        }
        buf_writer.flush()
    }

    /// Reads a version 1 journal from newline-delimited JSON.
    pub fn read_ndjson<R: std::io::BufRead>(mut reader: R) -> std::io::Result<Self> {
        let mut line = String::new();
        // First non-empty line must be the header
        while reader.read_line(&mut line)? > 0 {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                break;
            }
            line.clear();
        }
        if line.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "empty journal stream",
            ));
        }

        let header_val: serde_json::Value = serde_json::from_str(line.trim())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if header_val.get("type").and_then(|v| v.as_str()) != Some("header") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing journal v1 header",
            ));
        }

        let header: JournalHeader = serde_json::from_value(header_val)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if header.journal_version != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported journal version: {}", header.journal_version),
            ));
        }

        let mut log = AuditLog::new();
        line.clear();
        while reader.read_line(&mut line)? > 0 {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let val: serde_json::Value = serde_json::from_str(trimmed)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                let record = AuditRecord::from_json(&val)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                log.record_with_seq(record.seq, record.kind);
            }
            line.clear();
        }
        Ok(Self { header, log })
    }
}

fn order_event_kind_to_str(k: &OrderEventKind) -> &'static str {
    match k {
        OrderEventKind::Submitted => "submitted",
        OrderEventKind::Accepted => "accepted",
        OrderEventKind::Rejected => "rejected",
        OrderEventKind::Fill => "fill",
        OrderEventKind::CancelRequested => "cancel_requested",
        OrderEventKind::Cancelled => "cancelled",
        OrderEventKind::Expired => "expired",
        _ => "accepted",
    }
}

fn str_to_order_event_kind(s: &str) -> OrderEventKind {
    match s {
        "submitted" => OrderEventKind::Submitted,
        "accepted" => OrderEventKind::Accepted,
        "rejected" => OrderEventKind::Rejected,
        "fill" | "partially_filled" | "filled" => OrderEventKind::Fill,
        "cancel_requested" => OrderEventKind::CancelRequested,
        "cancelled" => OrderEventKind::Cancelled,
        "expired" => OrderEventKind::Expired,
        _ => OrderEventKind::Accepted,
    }
}

fn trading_state_to_str(s: &TradingState) -> &'static str {
    match s {
        TradingState::Active => "active",
        TradingState::Reducing => "reducing",
        TradingState::Halted => "halted",
    }
}

fn str_to_trading_state(s: &str) -> TradingState {
    match s {
        "reducing" => TradingState::Reducing,
        "halted" => TradingState::Halted,
        _ => TradingState::Active,
    }
}

impl AuditRecord {
    /// Formats the audit record as a structured JSON value.
    pub fn to_json(&self) -> serde_json::Value {
        match &self.kind {
            AuditKind::EventDispatched { ts_event } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "event_dispatched",
                    "ts_event": ts_event,
                })
            }
            AuditKind::OrderSubmitted {
                order_id,
                instrument,
                side,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "order_submitted",
                    "order_id": order_id,
                    "instrument": instrument,
                    "side": side,
                })
            }
            AuditKind::RiskRefused { order_id, refusal } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "risk_refused",
                    "order_id": order_id,
                    "rule": refusal.rule(),
                    "code": refusal.error_code().as_str(),
                    "context": refusal.context(),
                })
            }
            AuditKind::OrderRejected { order_id, reason } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "order_rejected",
                    "order_id": order_id,
                    "reason": reason,
                })
            }
            AuditKind::CancelRequested { order_id } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "cancel_requested",
                    "order_id": order_id,
                })
            }
            AuditKind::OrderLifecycle {
                order_id,
                event,
                ts_event,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "order_lifecycle",
                    "order_id": order_id,
                    "event": order_event_kind_to_str(event),
                    "ts_event": ts_event,
                })
            }
            AuditKind::IllegalTransition { order_id, error } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "illegal_transition",
                    "order_id": order_id,
                    "error": format!("{error}"),
                })
            }
            AuditKind::VenueOrderIdDrift {
                order_id,
                recorded,
                received,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "venue_order_id_drift",
                    "order_id": order_id,
                    "recorded": recorded.as_str(),
                    "received": received.as_str(),
                })
            }
            AuditKind::FillProduced {
                order_id,
                quantity,
                price,
                ts_event,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "fill_produced",
                    "order_id": order_id,
                    "quantity": quantity,
                    "price": price,
                    "ts_event": ts_event,
                })
            }
            AuditKind::StateChanged { from, to } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "state_changed",
                    "from": trading_state_to_str(from),
                    "to": trading_state_to_str(to),
                })
            }
            AuditKind::DuplicateFillIgnored {
                order_id,
                fingerprint,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "duplicate_fill_ignored",
                    "order_id": order_id,
                    "fingerprint": fingerprint,
                })
            }
            AuditKind::LlmDecisionRecorded {
                order_id,
                decision,
                original_qty,
                decided_qty,
                rationale,
            } => {
                serde_json::json!({
                    "seq": self.seq,
                    "type": "llm_decision_recorded",
                    "order_id": order_id,
                    "decision": decision,
                    "original_qty": original_qty,
                    "decided_qty": decided_qty,
                    "rationale": rationale,
                })
            }
        }
    }

    /// Parses an audit record from a JSON value.
    pub fn from_json(val: &serde_json::Value) -> Result<Self, String> {
        let seq = val
            .get("seq")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "missing seq".to_string())?;
        let kind_type = val
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "missing type".to_string())?;
        let kind = match kind_type {
            "event_dispatched" => {
                let ts_event = val.get("ts_event").and_then(|v| v.as_u64()).unwrap_or(0);
                AuditKind::EventDispatched { ts_event }
            }
            "order_submitted" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let instrument = val
                    .get("instrument")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let side = val
                    .get("side")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::OrderSubmitted {
                    order_id,
                    instrument,
                    side,
                }
            }
            "order_rejected" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let reason = val
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::OrderRejected { order_id, reason }
            }
            "cancel_requested" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::CancelRequested { order_id }
            }
            "order_lifecycle" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let event = str_to_order_event_kind(
                    val.get("event").and_then(|v| v.as_str()).unwrap_or(""),
                );
                let ts_event = val.get("ts_event").and_then(|v| v.as_u64()).unwrap_or(0);
                AuditKind::OrderLifecycle {
                    order_id,
                    event,
                    ts_event,
                }
            }
            "fill_produced" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let quantity = val.get("quantity").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let price = val.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ts_event = val.get("ts_event").and_then(|v| v.as_u64()).unwrap_or(0);
                AuditKind::FillProduced {
                    order_id,
                    quantity,
                    price,
                    ts_event,
                }
            }
            "state_changed" => {
                let from = str_to_trading_state(
                    val.get("from").and_then(|v| v.as_str()).unwrap_or("active"),
                );
                let to = str_to_trading_state(
                    val.get("to").and_then(|v| v.as_str()).unwrap_or("active"),
                );
                AuditKind::StateChanged { from, to }
            }
            "duplicate_fill_ignored" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let fingerprint = val
                    .get("fingerprint")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::DuplicateFillIgnored {
                    order_id,
                    fingerprint,
                }
            }
            "risk_refused" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::RiskRefused {
                    order_id,
                    refusal: honba_risk::RiskRefusal::TradingHalted,
                }
            }
            "venue_order_id_drift" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let recorded = val.get("recorded").and_then(|v| v.as_str()).unwrap_or("");
                let received = val.get("received").and_then(|v| v.as_str()).unwrap_or("");
                AuditKind::VenueOrderIdDrift {
                    order_id,
                    recorded: VenueOrderId::new(recorded),
                    received: VenueOrderId::new(received),
                }
            }
            "illegal_transition" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::IllegalTransition {
                    order_id,
                    error: honba_messages::IllegalTransition::InvalidQuantity { value: 0.0 },
                }
            }
            "llm_decision_recorded" => {
                let order_id = val
                    .get("order_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let decision = val
                    .get("decision")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let original_qty = val
                    .get("original_qty")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let decided_qty = val
                    .get("decided_qty")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let rationale = val
                    .get("rationale")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                AuditKind::LlmDecisionRecorded {
                    order_id,
                    decision,
                    original_qty,
                    decided_qty,
                    rationale,
                }
            }
            other => return Err(format!("unknown record type {other}")),
        };
        Ok(AuditRecord { seq, kind })
    }
}
