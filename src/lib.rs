pub mod config;
pub mod core;
pub mod drivers;
pub mod ops;

// Re-export for easier access
pub use crate::core::{ForgeColumn, ForgeSchema, ForgeTable};
use crate::core::{ForgeConfig, ForgeError, ForgeUniversalValue};
use async_trait::async_trait;
use futures::Stream;
use indexmap::IndexMap;
use std::pin::Pin;

#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// checks if database exists and is empty
    async fn db_is_empty(&self) -> Result<bool, Box<dyn std::error::Error>>;

    // fetches a schema into internal data structures
    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>>;

    /// applies schema diff to target
    async fn diff_and_apply_schema(
        &self,
        schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        verbose: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    /// read data-row stream from source
    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, ForgeError>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn std::error::Error>,
    >;

    /// read ordered data-row stream from source
    async fn stream_table_data_ordered(
        &self,
        table_name: &str,
        order_by: &[String],
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, ForgeError>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn std::error::Error>,
    >;

    /// write data-row stream into target-db
    async fn insert_chunk(
        &self,
        table_name: &str,
        dry_run: bool,
        halt_on_error: bool,
        chunk: Vec<IndexMap<String, ForgeUniversalValue>>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// gets row count for a table
    async fn get_table_row_count(
        &self,
        table_name: &str,
    ) -> Result<u64, Box<dyn std::error::Error>>;
}
