use anyhow::Result;
use clap::Subcommand;
use honba_core::TradingState;

#[derive(Subcommand, Debug)]
pub enum TradingStateCommands {
    /// Get the current trading state
    Get,
    /// Set the trading state
    Set {
        /// New trading state (active, reducing, halted)
        state: String,
    },
}

/// The [`TradingState`] a `set` spells, or the `anyhow`-ready message for an unknown state.
///
/// `honba_core::TradingState` has no `Reducing` variant (it only has `Active`,
/// `Halted { reason }` and `Paused { resume_at }`); `"reducing"` is mapped onto
/// `Paused { resume_at: None }` as the closest existing "curtailed but
/// resumable" state, while `"halted"` needs a `reason`, filled in with a
/// generic operator-initiated message.
pub(crate) fn state_command(raw: &str) -> Result<TradingState> {
    let new_state = match raw.to_lowercase().as_str() {
        "active" => TradingState::Active,
        "reducing" => TradingState::Paused { resume_at: None },
        "halted" => TradingState::Halted {
            reason: "operator halt via `honba trading-state set`".to_string(),
        },
        other => {
            anyhow::bail!("unknown trading state {other:?}; expected active, reducing or halted")
        }
    };
    Ok(new_state)
}

pub fn run(command: TradingStateCommands) -> Result<()> {
    match command {
        TradingStateCommands::Get => {
            // For now, just print the default state
            // In a real implementation, this would query the running server
            println!("active");
        }
        TradingStateCommands::Set { state } => {
            println!("{:?}", state_command(&state)?);
        }
    }
    Ok(())
}
