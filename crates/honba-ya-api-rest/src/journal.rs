//! The NDJSON implementation of the run journal port (`honba_api::JournalWriter`) (ADR 0017 decisions 3 and 8).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use honba_api::JournalWriter;
use honba_messages::{ErrorCode, ErrorDetail, Message};

/// `events.ndjson`: one wire [`Message`] per line, in append order.
pub struct NdjsonJournal<W: Write + Send = File> {
    writer: BufWriter<W>,
    failure: Option<ErrorDetail>,
}

impl<W: Write + Send> NdjsonJournal<W> {
    /// Wraps any writer (a file in production, memory in unit tests).
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
            failure: None,
        }
    }
}

impl NdjsonJournal<File> {
    /// Opens `path` for appending, creating it when missing.
    pub fn open(path: &Path) -> Result<Self, ErrorDetail> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| write_failure(&e))?;
        Ok(Self::new(file))
    }
}

impl<W: Write + Send> NdjsonJournal<W> {
    fn check(&self) -> Result<(), ErrorDetail> {
        self.failure.clone().map_or(Ok(()), Err)
    }

    /// Records the first I/O failure so every later call reports it.
    fn keep(&mut self, result: std::io::Result<()>) -> Result<(), ErrorDetail> {
        result.map_err(|e| {
            let detail = write_failure(&e);
            self.failure = Some(detail.clone());
            detail
        })
    }
}

/// The error for a failed journal write. Names the I/O kind, never a path.
fn write_failure(e: &std::io::Error) -> ErrorDetail {
    ErrorDetail::new(
        ErrorCode::InternalError,
        format!("journal write failed: {}", e.kind()),
    )
    .with_context(serde_json::json!({"reason": "journal_write"}))
}

impl<W: Write + Send> JournalWriter for NdjsonJournal<W> {
    fn append(&mut self, msg: &Message) -> Result<(), ErrorDetail> {
        self.check()?;
        // Serialise first so a failure cannot leave half a record in the buffer.
        let mut line = serde_json::to_vec(msg).map_err(|e| {
            ErrorDetail::new(
                ErrorCode::InternalError,
                format!("journal encode failed: {e}"),
            )
            .with_context(serde_json::json!({"reason": "journal_write"}))
        })?;
        line.push(b'\n');
        let result = self.writer.write_all(&line);
        self.keep(result)
    }

    fn flush(&mut self) -> Result<(), ErrorDetail> {
        self.check()?;
        let result = self.writer.flush();
        self.keep(result)
    }
}
