//! Unit tests for `crate::trading_state` (E2-S1 rest): the `set` spellings build the exact
//! `honba_async::Command::State` the async shell applies.

use honba_async::Command;
use honba_messages::TradingState;

use crate::trading_state::state_command;

#[test]
fn set_spellings_map_to_command_state() {
    for (raw, expected) in [
        ("active", TradingState::Active),
        ("reducing", TradingState::Reducing),
        ("halted", TradingState::Halted),
        ("REDUCING", TradingState::Reducing),
    ] {
        assert_eq!(
            state_command(raw).unwrap(),
            Command::State(expected),
            "{raw}"
        );
    }
}

#[test]
fn set_refuses_an_unknown_state() {
    assert!(state_command("paused").is_err());
    assert!(state_command("").is_err());
}
