use anyhow::Result;
use clap::Subcommand;
use honba_async::Command;
use honba_messages::TradingState;

#[derive(Subcommand, Debug)]
pub enum TradingStateCommands {
    /// Get the current trading state
    Get,
    /// Set the trading state (issues `Command::State`, the same operator command
    /// the async shell applies through `Engine::set_trading_state`)
    Set {
        /// New trading state (active, reducing, halted)
        state: String,
    },
}

/// The `Command::State` a `set` spells, or the `anyhow`-ready message for an unknown state.
pub(crate) fn state_command(raw: &str) -> Result<Command> {
    let new_state = match raw.to_lowercase().as_str() {
        "active" => TradingState::Active,
        "reducing" => TradingState::Reducing,
        "halted" => TradingState::Halted,
        other => {
            anyhow::bail!("unknown trading state {other:?}; expected active, reducing or halted")
        }
    };
    Ok(Command::State(new_state))
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
