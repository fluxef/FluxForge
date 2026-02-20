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
use crate::drivers::mysql::get_mysql_init_session_sql_mode;
use crate::DatabaseDriver;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::ConnectOptions;
use sqlx::{MySqlPool, PgPool};
use std::error::Error;
use std::str::FromStr;

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
///     &config,
///     true
/// ).await?;
///
/// // Create PostgreSQL driver
/// let pg_driver = drivers::create_driver(
///     "postgres://postgres:password@localhost:5432/mydb",
///     &config,
///     true
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
    is_source_driver: bool,
) -> Result<Box<dyn DatabaseDriver>, Box<dyn Error>> {
    if url.starts_with("mysql://") {
        let zero_date_on_write = config
            .mysql
            .as_ref()
            .and_then(|r| r.rules.as_ref())
            .and_then(|r| r.on_write.as_ref())
            .and_then(|w| w.zero_date)
            .unwrap_or(false); // default false, if not in config

        let sql_mode = get_mysql_init_session_sql_mode(config, is_source_driver);

        if sql_mode == "".to_string() {
            let pool = MySqlPool::connect(url).await?;
            let driver = MySqlDriver {
                pool,
                zero_date_on_write,
            };
            Ok(Box::new(driver))
        } else {
            let sql_command_for_hook = sql_mode.clone(); // copy for outer Closure

            let opts = MySqlConnectOptions::from_str(url)?;

            // create pool with options
            let pool = MySqlPoolOptions::new()
                .max_connections(5)
                .after_connect(move |conn, _meta| {
                    // IMPORTANT: wen need a new copy for every call which is then "moved" into the async block
                    let cmd = sql_command_for_hook.clone();

                    Box::pin(async move {
                        sqlx::query(&cmd).execute(conn).await?;
                        Ok(())
                    })
                })
                .connect_with(opts)
                .await?;
            let driver = MySqlDriver {
                pool,
                zero_date_on_write,
            };
            Ok(Box::new(driver))
        }
    }
    // if mysql
    else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPool::connect(url).await?;
        Ok(Box::new(postgres::PostgresDriver { pool: Some(pool) }))
    } else {
        Err(format!("Unsupported database protocol in URL: {url}").into())
    }
}
