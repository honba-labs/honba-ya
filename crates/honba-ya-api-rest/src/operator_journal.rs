//! The operator journal: one NDJSON record per accepted `PUT /trading-state` change
//! (ADR 0018 decision 7; ADR 0023 has the caller's identity).
//!
//! The REST crate's auth audit log records *who called which route* but not bodies, so a
//! trading-state change — the body and the reason — had nowhere to go. This module appends it:
//! `{"ts","event","from","to","reason","token_id"}`, one line per change, `reason` and
//! `token_id` omitted when absent.
//!
//! Append-only and best-effort: a failed write is logged and never fails the request, so an
//! operator can still halt trading when the disk is full. The state is *not* restored from the
//! journal at start-up — ADR 0015 keeps initialisation explicit and in the composition root.
//! A no-op `PUT` (the state is already the requested one) changes nothing and is not written.

use std::io::Write;
use std::path::Path;

use honba_messages::TradingState;
use serde::Serialize;

/// One operator-journal line. Fields are private: it is written, never read back.
#[derive(Serialize)]
struct Change<'a> {
    ts: String,
    event: &'static str,
    from: TradingState,
    to: TradingState,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_id: Option<&'a str>,
}

/// Appends one `trading_state.change` record to `path`, creating it if needed.
///
/// Call only for a real change (`from != to`). A write failure is logged, not returned.
pub(crate) fn record_change(
    path: &Path,
    from: TradingState,
    to: TradingState,
    reason: Option<&str>,
    token_id: Option<&str>,
) {
    let record = Change {
        ts: chrono::Utc::now().to_rfc3339(),
        event: "trading_state.change",
        from,
        to,
        reason: reason.filter(|r| !r.is_empty()),
        token_id,
    };
    let line = match serde_json::to_string(&record) {
        Ok(line) => line,
        Err(error) => {
            tracing::warn!(%error, "cannot serialise an operator journal record");
            return;
        }
    };
    let written = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(format!("{line}\n").as_bytes()));
    if let Err(error) = written {
        tracing::warn!(%error, path = %path.display(), "cannot write the operator journal");
    }
}
