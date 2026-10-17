//! sqlx repositories (PostgreSQL / SQLite).
use sqlx::PgPool;

pub struct Repos {
    pub pg: PgPool,
}

impl Repos {
    pub fn from_pg(pg: PgPool) -> Self {
        Self { pg }
    }

    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pg).await?;
        Ok(())
    }
}
