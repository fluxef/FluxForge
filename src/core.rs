use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// config structures for mapping.toml

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeConfig {
    pub general: Option<ForgeGeneralConfig>,
    pub mysql: Option<ForgeDbConfig>,
    pub postgres: Option<ForgeDbConfig>,
    pub rules: Option<ForgeRulesConfig>,
    pub tables: Option<ForgeTableConfig>,
}

impl ForgeConfig {
    /// get mappings for a database-type and direction
    pub fn get_type_list(
        &self,
        db_name: &str,
        direction: &str,
    ) -> Option<&HashMap<String, String>> {
        // database section
        let db_cfg = match db_name {
            "mysql" => self.mysql.as_ref(),
            "postgres" => self.postgres.as_ref(),
            _ => None,
        }?;

        // types
        let types = db_cfg.types.as_ref()?;

        // direction ("on_read" or "on_write")
        match direction {
            "on_read" => types.on_read.as_ref(),
            "on_write" => types.on_write.as_ref(),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeDbConfig {
    pub types: Option<ForgeTypeDirectionConfig>,
    pub rules: Option<ForgeRulesDirectionConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeTypeConfig {
    pub types: Option<ForgeTypeDirectionConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeTypeDirectionConfig {
    pub on_read: Option<HashMap<String, String>>,
    pub on_write: Option<HashMap<String, String>>,
}
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeRulesDirectionConfig {
    pub on_read: Option<ForgeRuleGeneralConfig>,
    pub on_write: Option<ForgeRuleGeneralConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeRuleGeneralConfig {
    pub unsigned_int_to_bigint: Option<bool>,
    pub nullify_invalid_dates: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeGeneralConfig {
    pub on_missing_type: Option<String>,
    pub default_charset: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeRulesConfig {
    pub rules: Option<ForgeRulesDirectionConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeTableConfig {
    pub renames: Option<HashMap<String, String>>,
    pub column_overrides: Option<HashMap<String, HashMap<String, String>>>,
}

// Schema-Structures for internal representation of schema

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
    pub is_unsigned: bool,
    pub auto_increment: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
    pub on_update: Option<String>,
    pub enum_values: Option<Vec<String>>,
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

// --- UNIVERSAL-Intermediate data types ---
#[derive(Debug, Clone)]
pub enum ForgeUniversalValue {
    Integer(i64),
    UnsignedInteger(u64),
    Float(f64),
    Text(String),
    Binary(Vec<u8>),
    Boolean(bool),
    Year(i32),
    Time(NaiveTime),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Json(serde_json::Value),
    Null,
}
