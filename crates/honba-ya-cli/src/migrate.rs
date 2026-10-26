//! Relational database migration subcommand (`honba migrate`).
//! Supports PostgreSQL and SQLite schema migrations via `honba-ya-persistence`.

use anyhow::{Context, Result};
use clap::Args;
use honba_persistence::{Database, migrate, rollback_last};

#[derive(Args, Debug, Clone)]
pub struct MigrateArgs {
    /// Database connection URL (e.g. `sqlite://honba.db`, `sqlite::memory:`, `postgres://user:pass@localhost/honba`)
    #[arg(long, default_value = "sqlite://honba.db")]
    pub database_url: String,
    /// Roll back the most recent migration instead of applying pending ones
    #[arg(long)]
    pub rollback: bool,
}

pub fn run(args: &MigrateArgs) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("starting tokio runtime for migrations")?;

    rt.block_on(async {
        let db = Database::connect(&args.database_url)
            .await
            .with_context(|| format!("connecting to database {}", args.database_url))?;

        if args.rollback {
            println!("Rolling back last migration on {}...", args.database_url);
            rollback_last(&db)
                .await
                .context("rolling back last migration")?;
            println!("Migration rollback completed successfully.");
        } else {
            println!("Applying pending migrations to {}...", args.database_url);
            migrate(&db).await.context("applying database migrations")?;
            println!("Schema migrations applied successfully.");
        }
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}
