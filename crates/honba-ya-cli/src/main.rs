//! honba CLI entry point.
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "honba", about = "本場 · Indian equities algo trading")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Start the REST API + WebSocket dashboard.
    Serve {
        #[arg(long, default_value = "honba.toml")]
        config: String,
    },
    /// Run a backtest over NSE/BSE historical data.
    Backtest {
        #[arg(long)]
        strategy: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    /// Apply sqlx migrations.
    Migrate,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    match Cli::parse().cmd {
        Cmd::Serve { config } => {
            tracing::info!(%config, "starting honba server");
            let app = honba_ya_api::router();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
            axum::serve(listener, app).await?;
        }
        Cmd::Backtest { strategy, from, to } => {
            tracing::info!(%strategy, %from, %to, "backtest requested");
        }
        Cmd::Migrate => {
            tracing::info!("migrate requested");
        }
    }
    Ok(())
}
