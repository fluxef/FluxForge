use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Konfigurations-Strukturen (für mapping.toml) ---

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForgeConfig {
    pub general: GeneralConfig,
    pub types: HashMap<String, String>,
    pub rules: RulesConfig,
    pub tables: Option<TableConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GeneralConfig {
    pub on_missing_type: String,
    pub default_charset: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RulesConfig {
    pub unsigned_int_to_bigint: bool,
    pub nullify_invalid_dates: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableConfig {
    pub renames: HashMap<String, String>,
    pub column_overrides: HashMap<String, HashMap<String, String>>,
}

// --- Schema-Strukturen (für interne Repräsentation / JSON) ---

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForgeSchema {
    pub metadata: SchemaMetadata,
    pub tables: Vec<ForgeTable>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaMetadata {
    pub source_system: String,
    pub created_at: String,
    pub forge_version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForgeTable {
    pub name: String,
    pub columns: Vec<ForgeColumn>,
    pub indices: Vec<ForgeIndex>,
    pub foreign_keys: Vec<ForgeForeignKey>,
    pub comment: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForgeIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForgeForeignKey {
    pub name: String,
    pub column: String,
    pub ref_table: String,
    pub ref_column: String,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

