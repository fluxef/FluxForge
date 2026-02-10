
pub mod core;
pub mod config;
pub mod drivers;

// Re-export für bequemeren Zugriff: use fluxforge::ForgeSchema;
use async_trait::async_trait; // Empfohlen für asynchrone Traits
pub use crate::core::{ForgeSchema, ForgeTable, ForgeColumn};
use futures::Stream;
use std::pin::Pin;
use crate::core::ForgeConfig;

#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    // ... fetch_schema, apply_schema ...
    // Liest das gesamte Schema ein
    async fn fetch_schema(&self, config: &ForgeConfig) -> Result<ForgeSchema, Box<dyn std::error::Error>>;

    // Schreibt die Struktur (wichtig für Migrate/Dry-Run)
    async fn apply_schema(&self, schema: &ForgeSchema, dry_run: bool) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    // Gibt einen Stream von Zeilen zurück
    async fn stream_table_data(&self, table_name: &str)
                               -> Result<Pin<Box<dyn Stream<Item = Result<serde_json::Value, sqlx::Error>> + Send>>, Box<dyn std::error::Error>>;

    // Schreibt ein Paket von Zeilen in die Ziel-DB
    async fn insert_chunk(&self, table_name: &str, chunk: Vec<serde_json::Value>)
                          -> Result<(), Box<dyn std::error::Error>>;

    // Streamt Daten von einem Treiber zum anderen
    // (Hier nutzen wir einen vereinfachten Ansatz für das Beispiel)
    // async fn stream_rows(&self, table: &str) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>>;
    // async fn insert_rows(&self, table: &str, rows: Vec<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>>;
}

