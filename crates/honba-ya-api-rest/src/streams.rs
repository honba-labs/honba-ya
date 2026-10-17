//! Run event stream (SSE) — seq-aware reader and handler (E11-S4, ADR 0021).

use std::io::{self, BufRead, BufReader};
use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Response, Sse},
};
use honba_api::{BacktestEventsQuery, RunKind, RunStatus, StreamFrame};
use honba_messages::{ErrorCode, ErrorDetail, Message};
use serde_json::json;
use tokio::time::interval;

use crate::market::failure;
use crate::AppState;

const DEFAULT_TICK_MS: u64 = 25;
const KEEP_ALIVE_SECS: u64 = 15;

/// Result of reading complete records from the journal starting at `from_seq`.
#[derive(Debug)]
struct JournalChunk {
    records: Vec<(u64, Message)>,
    last_seq: u64,
    at_eof: bool,
}

/// Reads `events.ndjson` records with line index `>= from_seq` from the given file.
/// Returns a chunk of records, the next `from_seq` to use, and whether EOF was reached.
/// A trailing partial line is never returned; the byte offset is preserved so the next
/// read sees the same partial line.
fn read_journal_from(path: &StdPath, from_seq: u64) -> Result<JournalChunk, ErrorDetail> {
    let file = std::fs::File::open(path).map_err(|e| io_error("journal_open", &e))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    // Fast-forward to the start line index by skipping lines.
    let mut current_seq: u64 = 0;
    let mut line = Vec::new();
    while current_seq < from_seq {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| io_error("journal_skip", &e))?;
        if bytes_read == 0 {
            // Reached EOF before `from_seq`; return empty chunk at EOF.
            return Ok(JournalChunk {
                records: Vec::new(),
                last_seq: current_seq,
                at_eof: true,
            });
        }
        if line.last() == Some(&b'\n') {
            current_seq += 1;
        } else {
            // Partial line at EOF before `from_seq` — treat as complete for skip purposes
            // (it will be re-read on the next call).
            break;
        }
    }

    // Now read complete records from `current_seq` until we hit a partial line or EOF.
    let mut records = Vec::new();
    let mut at_eof = false;
    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| io_error("journal_read", &e))?;
        if bytes_read == 0 {
            at_eof = true;
            break;
        }
        if line.last() != Some(&b'\n') {
            // Partial line — stop and preserve it for the next read.
            break;
        }
        let message = decode_record(&line[..line.len() - 1])?;
        records.push((current_seq, message));
        current_seq += 1;
    }

    Ok(JournalChunk {
        records,
        last_seq: current_seq,
        at_eof,
    })
}

/// Decodes one complete journal line into a `Message`, enforcing `SCHEMA_VERSION`.
fn decode_record(line: &[u8]) -> Result<Message, ErrorDetail> {
    serde_json::from_slice::<Message>(line).map_err(|e| {
        let found = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("schema_version").and_then(serde_json::Value::as_u64));
        match found {
            Some(found) if found != u64::from(honba_messages::SCHEMA_VERSION) => ErrorDetail::new(
                ErrorCode::Unsupported,
                "journal written at another schema_version",
            )
            .with_context(json!({"found": found, "expected": honba_messages::SCHEMA_VERSION})),
            _ => ErrorDetail::new(
                ErrorCode::InternalError,
                format!("journal record invalid: {e}"),
            )
            .with_context(json!({"reason": "journal_corrupt"})),
        }
    })
}

/// Converts an `std::io::Error` into an `ErrorDetail`.
fn io_error(op: &str, e: &io::Error) -> ErrorDetail {
    ErrorDetail::new(
        ErrorCode::InternalError,
        format!("journal {op} failed: {e}"),
    )
    .with_context(json!({"reason": "journal_io", "op": op}))
}

/// Creates a `Status` frame for the current manifest state.
fn make_status_frame(manifest: &honba_api::RunManifest, delivered_seq: u64) -> StreamFrame {
    StreamFrame::Status {
        status: manifest.status,
        seq: delivered_seq,
        error: if manifest.status == RunStatus::Failed {
            manifest.error.clone()
        } else {
            None
        },
    }
}

/// SSE handler: `GET /backtests/{id}/events?resume_from_seq=N`
///
/// Streams journal records (as `Record` frames) and manifest status changes (as `Status` frames)
/// as Server-Sent Events. Each SSE event carries `id:` = the frame's `seq` and `data:` = the
/// JSON-serialized `StreamFrame`. The connection is kept alive with periodic keep-alive comments.
pub async fn get_backtest_events(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<BacktestEventsQuery>,
) -> Response {
    let Some(runs) = state.runs.clone() else {
        return failure(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorDetail::new(
                ErrorCode::Unsupported,
                "no journals directory is configured",
            )
            .with_context(json!({"reason": "no_journals_dir"})),
        )
        .into_response();
    };

    // Validate id before any fs access.
    let run_id = match honba_api::RunId::parse(&id) {
        Ok(id) => id,
        Err(_) => {
            return failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(ErrorCode::NotFound, "run not found"),
            )
            .into_response();
        }
    };

    let store = runs.store().clone();
    let manifest = match store.load(run_id.as_str(), RunKind::Backtest) {
        Ok(m) => m,
        Err(_) => {
            return failure(
                StatusCode::NOT_FOUND,
                ErrorDetail::new(ErrorCode::NotFound, "run not found"),
            )
            .into_response();
        }
    };

    let resume_from_seq = query.resume_from_seq.unwrap_or(0);

    let events_path = store.dir(&run_id).join("events.ndjson");

    let tick_ms = DEFAULT_TICK_MS;
    let keep_alive_secs = KEEP_ALIVE_SECS;

    // Build the stream: start with the current manifest status, then replay records,
    // then follow new records and status changes until terminal.
    let stream = async_stream::stream! {
        let mut delivered_seq = resume_from_seq;
        let mut last_status = manifest.status;
        let mut eof_reached = false;

        type EventResult = Result<Event, axum::Error>;

        // 1. Initial status frame (acknowledgment with current seq).
        let initial_frame = make_status_frame(&manifest, delivered_seq);
        yield EventResult::Ok(Event::default()
            .id(delivered_seq.to_string())
            .json_data(initial_frame)?);

        // 2. Replay records from `resume_from_seq`.
        if resume_from_seq > 0 || !eof_reached {
            loop {
                let chunk = match read_journal_from(&events_path, delivered_seq) {
                    Ok(c) => c,
                    Err(e) => {
                        yield EventResult::Ok(Event::default()
                            .id(delivered_seq.to_string())
                            .json_data(StreamFrame::Status {
                                status: RunStatus::Failed,
                                seq: delivered_seq,
                                error: Some(e),
                            })?);
                        break;
                    }
                };
                for (seq, message) in chunk.records {
                    let frame = StreamFrame::Record { seq, message };
                    yield EventResult::Ok(Event::default()
                        .id(seq.to_string())
                        .json_data(frame)?);
                }
                delivered_seq = chunk.last_seq;
                eof_reached = chunk.at_eof;
                if eof_reached {
                    break;
                }
                // If we're still running and not at EOF, we'll pick up the rest in the follow loop.
                if !matches!(manifest.status, RunStatus::Pending | RunStatus::Running) {
                    break;
                }
            }
        }

        // 3. Follow new records and status changes until the run is terminal and all records delivered.
        let mut tick = interval(Duration::from_millis(tick_ms));
        let keep_alive_interval = Duration::from_secs(keep_alive_secs);
        let mut next_keep_alive = tokio::time::Instant::now() + keep_alive_interval;

        while !eof_reached || matches!(manifest.status, RunStatus::Pending | RunStatus::Running) {
            tokio::select! {
                _ = tick.tick() => {
                    // Read new records.
                    if !eof_reached {
                        match read_journal_from(&events_path, delivered_seq) {
                            Ok(chunk) => {
                                for (seq, message) in chunk.records {
                                    let frame = StreamFrame::Record { seq, message };
                                    yield EventResult::Ok(Event::default()
                                        .id(seq.to_string())
                                        .json_data(frame)?);
                                }
                                delivered_seq = chunk.last_seq;
                                eof_reached = chunk.at_eof;
                            }
                            Err(e) => {
                                yield EventResult::Ok(Event::default()
                                    .id(delivered_seq.to_string())
                                    .json_data(StreamFrame::Status {
                                        status: RunStatus::Failed,
                                        seq: delivered_seq,
                                        error: Some(e),
                                    })?);
                                break;
                            }
                        }
                    }

                    // Check manifest status.
                    let fresh = match store.load(run_id.as_str(), RunKind::Backtest) {
                        Ok(m) => m,
                        Err(_) => {
                            // Run evicted or deleted mid-stream — end with failed frame.
                            yield EventResult::Ok(Event::default()
                                .id(delivered_seq.to_string())
                                .json_data(StreamFrame::Status {
                                    status: RunStatus::Failed,
                                    seq: delivered_seq,
                                    error: Some(ErrorDetail::new(
                                        ErrorCode::NotFound,
                                        "run not found",
                                    )),
                                })?);
                            break;
                        }
                    };
                    if fresh.status != last_status {
                        let frame = make_status_frame(&fresh, delivered_seq);
                        yield EventResult::Ok(Event::default()
                            .id(delivered_seq.to_string())
                            .json_data(frame)?);
                        last_status = fresh.status;
                        if fresh.status.is_terminal() && eof_reached {
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_keep_alive) => {
                    // SSE keep-alive: a comment line. `Event::default()` with empty data is a comment.
                    yield EventResult::Ok(Event::default().comment("keep-alive"));
                    next_keep_alive += keep_alive_interval;
                }
            }
        }

        // 4. Ensure a terminal status frame if we haven't sent one yet.
        let final_manifest = store.load(run_id.as_str(), RunKind::Backtest).unwrap_or(manifest);
        if final_manifest.status.is_terminal() && last_status != final_manifest.status {
            let frame = make_status_frame(&final_manifest, delivered_seq);
            yield EventResult::Ok(Event::default()
                .id(delivered_seq.to_string())
                .json_data(frame)?);
        }
    };

    Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::read_journal_from;
    use honba_messages::{Event, Exchange, InstrumentId, Message, QuoteTick, UnixNanos};
    use std::io::Write;
    use std::path::Path;

    fn msg(n: u64) -> Message {
        let quote = QuoteTick::new(
            InstrumentId::new("TCS", Exchange::new("NSE")),
            100.0 + n as f64,
            101.0 + n as f64,
            10.0,
            20.0,
            UnixNanos::from_u64(n),
            UnixNanos::from_u64(n),
        );
        Message::new(Event::Quote(quote), UnixNanos::from_u64(n + 1))
    }

    fn write_events(path: &Path, msgs: &[Message]) {
        let mut file = std::fs::File::create(path).unwrap();
        for m in msgs {
            let line = serde_json::to_vec(m).unwrap();
            file.write_all(&line).unwrap();
            file.write_all(b"\n").unwrap();
        }
    }

    #[test]
    fn read_from_zero_returns_all_complete_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let msgs: Vec<_> = (0..5).map(msg).collect();
        write_events(&path, &msgs);

        let chunk = read_journal_from(&path, 0).unwrap();
        assert_eq!(chunk.records.len(), 5);
        assert_eq!(chunk.last_seq, 5);
        assert!(chunk.at_eof);
    }

    #[test]
    fn read_from_midpoint_returns_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let msgs: Vec<_> = (0..5).map(msg).collect();
        write_events(&path, &msgs);

        let chunk = read_journal_from(&path, 2).unwrap();
        assert_eq!(chunk.records.len(), 3);
        assert_eq!(chunk.records[0].0, 2);
        assert_eq!(chunk.records[2].0, 4);
        assert!(chunk.at_eof);
    }

    #[test]
    fn read_past_end_returns_empty_chunk_at_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let msgs: Vec<_> = (0..3).map(msg).collect();
        write_events(&path, &msgs);

        let chunk = read_journal_from(&path, 5).unwrap();
        assert!(chunk.records.is_empty());
        // When reading past the end, last_seq reflects the actual lines read (3),
        // not the requested from_seq (5).
        assert_eq!(chunk.last_seq, 3);
        assert!(chunk.at_eof);
    }

    #[test]
    fn trailing_partial_line_is_not_returned_and_byte_offset_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let msgs: Vec<_> = (0..3).map(msg).collect();
        write_events(&path, &msgs);

        // Truncate the last newline.
        let content = std::fs::read(&path).unwrap();
        let truncated = &content[..content.len() - 1];
        std::fs::write(&path, truncated).unwrap();

        // First read: should see only the 2 complete records.
        let chunk1 = read_journal_from(&path, 0).unwrap();
        assert_eq!(chunk1.records.len(), 2);
        assert_eq!(chunk1.last_seq, 2);
        assert!(!chunk1.at_eof);

        // Second read from 2: same file (no new complete lines), returns empty.
        let chunk2 = read_journal_from(&path, 2).unwrap();
        assert!(chunk2.records.is_empty());
        assert_eq!(chunk2.last_seq, 2);
        assert!(!chunk2.at_eof);

        // Now append the newline + a new record.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"\n").unwrap();
        let new_msg = msg(3);
        let line = serde_json::to_vec(&new_msg).unwrap();
        file.write_all(&line).unwrap();
        file.write_all(b"\n").unwrap();

        // Third read from 2: should see the previously-partial line (now complete) and the new one.
        let chunk3 = read_journal_from(&path, 2).unwrap();
        assert_eq!(chunk3.records.len(), 2);
        assert_eq!(chunk3.records[0].0, 2);
        assert_eq!(chunk3.records[1].0, 3);
        assert!(chunk3.at_eof);
    }
}
