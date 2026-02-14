pub mod mysql;
pub mod postgres;

pub use mysql::MySqlDriver;
pub use postgres::PostgresDriver;

use crate::DatabaseDriver;
use sqlx::{MySqlPool, PgPool};
use std::error::Error;
use crate::core::ForgeConfig;

pub async fn create_driver(url: &str, config:&ForgeConfig) -> Result<Box<dyn DatabaseDriver>, Box<dyn Error>> {
    if url.starts_with("mysql://") {
        let pool = MySqlPool::connect(url).await?;

        let zero_date_on_write = config.mysql
            .as_ref()
            .and_then(|r| r.rules.as_ref())
            .and_then(|r| r.on_write.as_ref())
            .and_then(|w| w.zero_date)
            .unwrap_or(false); // default false, if not in config

        Ok(Box::new(mysql::MySqlDriver { pool, zero_date_on_write }))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPool::connect(url).await?;
        Ok(Box::new(postgres::PostgresDriver { pool }))
    } else {
        Err(format!("Unsupported database protocol in URL: {}", url).into())
    }
}

