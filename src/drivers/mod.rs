pub mod mysql;
pub mod postgres;

pub use mysql::MySqlDriver;
pub use postgres::PostgresDriver;

use crate::DatabaseDriver;
use sqlx::{MySqlPool, PgPool};
use std::error::Error;


pub async fn create_driver(url: &str) -> Result<Box<dyn DatabaseDriver>, Box<dyn Error>> {
    if url.starts_with("mysql://") {
        let pool = MySqlPool::connect(url).await?;
        Ok(Box::new(mysql::MySqlDriver { pool }))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPool::connect(url).await?;
        Ok(Box::new(postgres::PostgresDriver { pool }))
    } else {
        Err(format!("Unsupported database protocol in URL: {}", url).into())
    }
}

