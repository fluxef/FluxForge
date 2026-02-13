pub mod config;
pub mod core;
pub mod drivers;
pub mod ops;

// Re-export für bequemeren Zugriff: use fluxforge::ForgeSchema;
pub use crate::core::{ForgeColumn, ForgeSchema, ForgeTable};
use crate::core::{ForgeConfig, ForgeUniversalValue};
use async_trait::async_trait;
// Empfohlen für asynchrone Traits
use futures::Stream;
use indexmap::IndexMap;
use std::pin::Pin;

#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Prüft, ob in den Tabellen des Schemas bereits Daten im Ziel existieren.
    async fn db_is_empty(&self) -> Result<bool, Box<dyn std::error::Error>>;

    // ... fetch_schema, apply_schema ...
    // Liest das gesamte Schema ein
    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>>;

    // Schreibt die Struktur (wichtig für Migrate/Dry-Run)
    async fn create_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn diff_schema(
        &self,
        schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, sqlx::Error>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn std::error::Error>,
    >;

    // Schreibt ein Paket von Zeilen in die Ziel-DB
    async fn insert_chunk(
        &self,
        table_name: &str,
        dry_run: bool,
        halt_on_error: bool,
        chunk: Vec<IndexMap<String, ForgeUniversalValue>>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
