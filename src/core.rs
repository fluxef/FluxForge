use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Konfigurations-Strukturen (für mapping.toml) ---

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeConfig {
    pub general: Option<GeneralConfig>,
    pub types: Option<HashMap<String, String>>,
    pub rules: Option<RulesConfig>,
    pub tables: Option<TableConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct GeneralConfig {
    pub on_missing_type: Option<String>,
    pub default_charset: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct RulesConfig {
    pub unsigned_int_to_bigint: Option<bool>,
    pub nullify_invalid_dates: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TableConfig {
    pub renames: Option<HashMap<String, String>>,
    pub column_overrides: Option<HashMap<String, HashMap<String, String>>>,
}

// --- Schema-Strukturen (für interne Repräsentation / JSON) ---

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchema {
    pub metadata: ForgeMetadata,
    pub tables: Vec<ForgeTable>,
}

impl ForgeSchema {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeMetadata {
    pub source_system: String,
    pub source_database_name: String,
    pub created_at: String,
    pub forge_version: String,
    pub config_file: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeTable {
    pub name: String,
    pub columns: Vec<ForgeColumn>,
    pub indices: Vec<ForgeIndex>,
    pub foreign_keys: Vec<ForgeForeignKey>,
    pub comment: Option<String>,
}

impl ForgeTable {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeColumn {
    pub name: String,
    pub data_type: String,
    pub length: Option<u32>,
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub auto_increment: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
    pub enum_values: Option<Vec<String>>, // Speichert ["active", "inactive", etc.]
}

impl ForgeColumn {
    pub fn new(name: &str, data_type: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeForeignKey {
    pub name: String,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

