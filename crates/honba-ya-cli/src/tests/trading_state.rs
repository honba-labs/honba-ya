//! Unit tests for `crate::trading_state` (E2-S1 rest): the `set` spellings build the exact
//! `honba_core::TradingState` the CLI prints.

use honba_core::TradingState;

use crate::trading_state::state_command;

#[test]
fn set_spellings_map_to_trading_state() {
    for (raw, expected) in [
        ("active", TradingState::Active),
        ("reducing", TradingState::Paused { resume_at: None }),
        (
            "halted",
            TradingState::Halted {
                reason: "operator halt via `honba trading-state set`".to_string(),
            },
        ),
        ("REDUCING", TradingState::Paused { resume_at: None }),
    ] {
        assert_eq!(state_command(raw).unwrap(), expected, "{raw}");
    }
}

#[test]
fn set_refuses_an_unknown_state() {
    assert!(state_command("paused").is_err());
    assert!(state_command("").is_err());
}
