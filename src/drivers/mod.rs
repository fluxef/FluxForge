//! Database driver implementations.
//!
//! This module provides concrete implementations of the `DatabaseDriver` trait
//! for MySQL and PostgreSQL databases, along with a factory function for creating
//! driver instances from connection URLs.

pub mod mysql;
pub mod postgres;

pub use mysql::MySqlDriver;
pub use postgres::PostgresDriver;

use crate::core::ForgeConfig;
use crate::DatabaseDriver;
use sqlx::{MySqlPool, PgPool};
use std::error::Error;

/// Creates a database driver from a connection URL.
///
/// Automatically detects the database type from the URL protocol and returns
/// the appropriate driver implementation. Supports MySQL and PostgreSQL.
///
/// # Arguments
///
/// * `url` - Database connection URL (e.g., "mysql://user:pass@host/db" or "postgres://user:pass@host/db")
/// * `config` - Configuration for type mappings and database-specific rules
///
/// # Examples
///
/// ```no_run
/// use fluxforge::{drivers, core::ForgeConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ForgeConfig::default();
///
/// // Create MySQL driver
/// let mysql_driver = drivers::create_driver(
///     "mysql://root:password@localhost:3306/mydb",
///     &config
/// ).await?;
///
/// // Create PostgreSQL driver
/// let pg_driver = drivers::create_driver(
///     "postgres://postgres:password@localhost:5432/mydb",
///     &config
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The URL protocol is not supported (only mysql:// and postgres:// are supported)
/// - Database connection fails (invalid credentials, host unreachable, etc.)
/// - Connection pool cannot be established
pub async fn create_driver(
    url: &str,
    config: &ForgeConfig,
) -> Result<Box<dyn DatabaseDriver>, Box<dyn Error>> {
    if url.starts_with("mysql://") {
        let pool = MySqlPool::connect(url).await?;

        let zero_date_on_write = config
            .mysql
            .as_ref()
            .and_then(|r| r.rules.as_ref())
            .and_then(|r| r.on_write.as_ref())
            .and_then(|w| w.zero_date)
            .unwrap_or(false); // default false, if not in config

        Ok(Box::new(mysql::MySqlDriver {
            pool,
            zero_date_on_write,
        }))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPool::connect(url).await?;
        Ok(Box::new(postgres::PostgresDriver { pool: Some(pool) }))
    } else {
        Err(format!("Unsupported database protocol in URL: {url}").into())
    }
}
