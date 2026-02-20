//! # FluxForge
//!
//! A database schema converter and migration engine for MySQL and PostgreSQL.
//!
//! FluxForge provides a unified interface for extracting, transforming, and replicating
//! database schemas and data between MySQL and PostgreSQL databases. It supports:
//!
//! - Schema extraction and conversion
//! - Type mapping and transformation
//! - Data replication with verification
//! - Dependency-aware table ordering
//!
//! # Examples
//!
//! ```no_run
//! use fluxforge::{drivers, core::ForgeConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ForgeConfig::default();
//! let driver = drivers::create_driver("mysql://user:pass@localhost/db", &config).await?;
//! let schema = driver.fetch_schema(&config).await?;
//! println!("Extracted {} tables", schema.tables.len());
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod core;
pub mod drivers;
pub mod ops;

// Re-export for easier access
pub use crate::core::ForgeUniversalDataTransferPacket;
pub use crate::core::{ForgeConfig, ForgeError};
pub use crate::core::{ForgeSchema, ForgeSchemaColumn, ForgeSchemaTable};
pub use crate::core::{ForgeUniversalDataField, ForgeUniversalDataRow};

use async_trait::async_trait;
use futures::Stream;
use indexmap::IndexMap;
use std::pin::Pin;

/// Database driver trait for unified database operations.
///
/// This trait provides a common interface for interacting with different database systems
/// (MySQL, PostgreSQL). Implementations handle database-specific operations while presenting
/// a unified API for schema extraction, migration, and data replication.
///
/// # Examples
///
/// ```no_run
/// use fluxforge::{DatabaseDriver, drivers, core::ForgeConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ForgeConfig::default();
/// let driver = drivers::create_driver("postgres://user:pass@localhost/db", &config).await?;
/// let is_empty = driver.db_is_empty().await?;
/// println!("Database is empty: {}", is_empty);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Checks if the database is empty (contains no tables).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::{DatabaseDriver, drivers, core::ForgeConfig};
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// if driver.db_is_empty().await? {
    ///     println!("Database is empty and ready for replication");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the database connection fails or the query cannot be executed.
    async fn db_is_empty(&self) -> Result<bool, Box<dyn std::error::Error>>;

    /// Fetches the complete database schema including tables, columns, indices, and foreign keys.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for type mappings and transformation rules
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::{DatabaseDriver, core::ForgeConfig};
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ForgeConfig::default();
    /// let schema = driver.fetch_schema(&config).await?;
    /// for table in &schema.tables {
    ///     println!("Table: {} with {} columns", table.name, table.columns.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - Schema metadata cannot be queried
    /// - Type mapping configuration is invalid
    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>>;

    /// Compares source schema with target database and applies necessary changes.
    ///
    /// # Arguments
    ///
    /// * `schema` - The source schema to apply
    /// * `config` - Configuration for type mappings and transformation rules
    /// * `dry_run` - If true, returns SQL statements without executing them
    /// * `verbose` - Enable verbose output
    /// * `destructive` - If true, allows dropping tables and columns not in source schema
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::{DatabaseDriver, ForgeSchema, core::ForgeConfig};
    /// # async fn example(driver: &dyn DatabaseDriver, schema: &ForgeSchema) -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ForgeConfig::default();
    /// let statements = driver.diff_and_apply_schema(
    ///     schema,
    ///     &config,
    ///     true,  // dry_run
    ///     false, // verbose
    ///     false  // destructive
    /// ).await?;
    /// for sql in statements {
    ///     println!("Would execute: {}", sql);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database connection fails
    /// - SQL statements cannot be generated or executed
    /// - Schema conflicts cannot be resolved
    async fn diff_and_apply_schema(
        &self,
        schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        verbose: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    /// Streams all rows from a table as universal values.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to stream
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::DatabaseDriver;
    /// # use futures::StreamExt;
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stream = driver.stream_table_data("users").await?;
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     println!("Row: {:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - Database connection fails
    /// - Row data cannot be decoded
    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalDataField>, ForgeError>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn std::error::Error>,
    >;

    /// Streams rows from a table ordered by specified columns.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to stream
    /// * `order_by` - Column names to order by
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::DatabaseDriver;
    /// # use futures::StreamExt;
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stream = driver.stream_table_data_ordered(
    ///     "users",
    ///     &["id".to_string()]
    /// ).await?;
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     println!("Row: {:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - Order by columns do not exist
    /// - Database connection fails
    async fn stream_table_data_ordered(
        &self,
        table_name: &str,
        order_by: &[String],
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalDataField>, ForgeError>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn std::error::Error>,
    >;

    /// Inserts a batch of rows into a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the target table
    /// * `dry_run` - If true, prints SQL without executing
    /// * `halt_on_error` - If true, stops on first error; if false, logs errors and continues
    /// * `chunk` - Vector of rows to insert
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::{DatabaseDriver, core::ForgeUniversalDataField};
    /// # use indexmap::IndexMap;
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut row = IndexMap::new();
    /// row.insert("id".to_string(), ForgeUniversalDataField::Integer(1));
    /// row.insert("name".to_string(), ForgeUniversalDataField::Text("Alice".to_string()));
    /// driver.insert_chunk("users", false, true, vec![row]).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - Column types are incompatible
    /// - Database constraints are violated
    /// - `halt_on_error` is true and any insert fails
    async fn insert_chunk(
        &self,
        table_name: &str,
        dry_run: bool,
        halt_on_error: bool,
        chunk: Vec<IndexMap<String, ForgeUniversalDataField>>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// Gets the total number of rows in a table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use fluxforge::DatabaseDriver;
    /// # async fn example(driver: &dyn DatabaseDriver) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = driver.get_table_row_count("users").await?;
    /// println!("Table has {} rows", count);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - Database connection fails
    async fn get_table_row_count(
        &self,
        table_name: &str,
    ) -> Result<u64, Box<dyn std::error::Error>>;
}
