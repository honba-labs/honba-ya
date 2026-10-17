//! Run ids (ADR 0017 decision 2): ULID-shaped, 26 characters of uppercase Crockford Base32.
//!
//! Pure: [`RunIdGenerator::next`] takes the millisecond timestamp and the 80 bits of entropy
//! as parameters, so this crate reads neither the clock nor the OS. The REST layer supplies
//! both. A [`RunId`] can only be built by [`RunId::parse`] (or deserialization, which calls
//! it) or by the generator, so a value of this type is always safe to use as a path segment.

use std::fmt;
use std::str::FromStr;

use honba_messages::{ErrorCode, ErrorDetail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ID_LEN: usize = 26;
const MAX_UNIX_MS: u64 = (1 << 48) - 1;
const MAX_RANDOM: u128 = (1 << 80) - 1;

/// A text was not a well-formed run id.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[error("not a run id: expected 26 characters matching ^[0-9A-HJKMNP-TV-Z]{{26}}$")]
pub struct RunIdError;

/// Identifier of a backtest run or sweep job (`run_id` / `job_id` on the wire).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RunId(String);

impl RunId {
    /// Validates `text` against `^[0-9A-HJKMNP-TV-Z]{26}$` (hand-written, byte-wise).
    ///
    /// Lowercase, other lengths, `I`/`L`/`O`/`U`, separators and percent-escapes are all
    /// refused, so a parsed id is safe as a directory name.
    pub fn parse(text: &str) -> Result<Self, RunIdError> {
        let bytes = text.as_bytes();
        if bytes.len() == ID_LEN && bytes.iter().all(|b| ALPHABET.contains(b)) {
            Ok(Self(text.to_owned()))
        } else {
            Err(RunIdError)
        }
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RunId {
    type Err = RunIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for RunId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for RunId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// Mints [`RunId`]s with the ULID monotonic rule.
///
/// The first id of a millisecond uses the supplied entropy; further ids in the same
/// millisecond (or after the clock stepped back) increment the previous 80-bit value by one,
/// so ids are strictly increasing in issue order within one generator.
#[derive(Clone, Debug, Default)]
pub struct RunIdGenerator {
    last: Option<(u64, u128)>,
}

impl RunIdGenerator {
    /// Mints the next id.
    ///
    /// `unix_ms` is the wall-clock time in Unix milliseconds and `entropy` is 80 random bits,
    /// both supplied by the caller. Fails with `internal_error` when `unix_ms` does not fit 48
    /// bits or the 80-bit counter would overflow; a failed call leaves the state unchanged.
    pub fn next(&mut self, unix_ms: u64, entropy: [u8; 10]) -> Result<RunId, ErrorDetail> {
        if unix_ms > MAX_UNIX_MS {
            return Err(ErrorDetail::new(
                ErrorCode::InternalError,
                "run id timestamp does not fit 48 bits",
            )
            .with_context(serde_json::json!({"reason": "run_id_timestamp"})));
        }
        let (ms, random) = match self.last {
            Some((last_ms, last_random)) if unix_ms <= last_ms => {
                if last_random >= MAX_RANDOM {
                    return Err(ErrorDetail::new(
                        ErrorCode::InternalError,
                        "run id counter overflowed within one millisecond",
                    )
                    .with_context(serde_json::json!({"reason": "run_id_overflow"})));
                }
                (last_ms, last_random + 1)
            }
            _ => {
                let mut wide = [0u8; 16];
                wide[6..].copy_from_slice(&entropy);
                (unix_ms, u128::from_be_bytes(wide))
            }
        };
        self.last = Some((ms, random));
        Ok(RunId(encode((u128::from(ms) << 80) | random)))
    }
}

fn encode(value: u128) -> String {
    (0..ID_LEN)
        .map(|i| {
            let shift = 5 * (ID_LEN - 1 - i);
            char::from(ALPHABET[((value >> shift) & 31) as usize])
        })
        .collect()
}
