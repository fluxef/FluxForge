//! Core data structures for schema representation and configuration.
//!
//! This module defines the fundamental types used throughout FluxForge:
//! - Configuration structures for type mappings and transformation rules
//! - Schema representation (tables, columns, indices, foreign keys)
//! - Universal value types for cross-database data representation
//! - Error types for database operations

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use indexmap::IndexMap;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

// config structures for mapping.toml

/// Main configuration structure for FluxForge.
///
/// Defines type mappings, transformation rules, and database-specific settings
/// for schema conversion and data migration.
///
/// # Examples
///
/// ```
/// use fluxforge::core::ForgeConfig;
///
/// let config = ForgeConfig::default();
/// // Or load from TOML:
/// // let config: ForgeConfig = toml::from_str(toml_content)?;
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeConfig {
    /// General settings (charset, error handling)
    pub general: Option<ForgeGeneralConfig>,
    /// MySQL-specific type mappings and rules
    pub mysql: Option<ForgeDbConfig>,
    /// PostgreSQL-specific type mappings and rules
    pub postgres: Option<ForgeDbConfig>,
    /// Global transformation rules
    pub rules: Option<ForgeRulesConfig>,
    /// Table-specific overrides and renames
    pub tables: Option<ForgeSchemaTableConfig>,
}

impl ForgeConfig {
    /// Gets type mappings for a specific database and direction.
    ///
    /// # Arguments
    ///
    /// * `db_name` - Database name ("mysql" or "postgres")
    /// * `direction` - Mapping direction ("on_read" or "on_write")
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxforge::core::ForgeConfig;
    ///
    /// let config = ForgeConfig::default();
    /// if let Some(mappings) = config.get_type_list("mysql", "on_read") {
    ///     println!("Found {} type mappings", mappings.len());
    /// }
    /// ```
    #[must_use]
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
    pub zero_date: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeGeneralConfig {
    pub on_missing_type: Option<String>,
    pub default_charset: Option<String>,
    pub verify_after_write: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeRulesConfig {
    pub rules: Option<ForgeRulesDirectionConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ForgeSchemaTableConfig {
    pub renames: Option<HashMap<String, String>>,
    pub column_overrides: Option<HashMap<String, HashMap<String, String>>>,
}

// Schema-Structures for internal representation of schema

/// Complete database schema representation.
///
/// Contains metadata about the source database and a list of all tables
/// with their columns, indices, and foreign keys.
///
/// # Examples
///
/// ```
/// use fluxforge::core::ForgeSchema;
///
/// let schema = ForgeSchema::new();
/// println!("Schema has {} tables", schema.tables.len());
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchema {
    /// Metadata about the source database and extraction
    pub metadata: ForgeSchemaMetadata,
    /// List of all tables in the schema
    pub tables: Vec<ForgeSchemaTable>,
}

impl ForgeSchema {
    /// Creates a new empty schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxforge::core::ForgeSchema;
    ///
    /// let schema = ForgeSchema::new();
    /// assert_eq!(schema.tables.len(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Metadata about a schema extraction.
///
/// Tracks the source database, extraction time, and configuration used.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchemaMetadata {
    /// Source database type ("mysql" or "postgres")
    pub source_system: String,
    /// Name of the source database
    pub source_database_name: String,
    /// ISO 8601 timestamp of schema extraction
    pub created_at: String,
    /// FluxForge version used for extraction
    pub forge_version: String,
    /// Path to configuration file used
    pub config_file: String,
}

/// Represents a database table with all its components.
///
/// # Examples
///
/// ```
/// use fluxforge::core::ForgeSchemaTable;
///
/// let table = ForgeSchemaTable::new("users");
/// assert_eq!(table.name, "users");
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchemaTable {
    /// Table name
    pub name: String,
    /// List of columns in the table
    pub columns: Vec<ForgeSchemaColumn>,
    /// List of indices defined on the table
    pub indices: Vec<ForgeSchemaIndex>,
    /// List of foreign key constraints
    pub foreign_keys: Vec<ForgeSchemaForeignKey>,
    /// Optional table comment
    pub comment: Option<String>,
}

impl ForgeSchemaTable {
    /// Creates a new table with the given name.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxforge::core::ForgeSchemaTable;
    ///
    /// let table = ForgeSchemaTable::new("products");
    /// assert_eq!(table.name, "products");
    /// assert_eq!(table.columns.len(), 0);
    /// ```
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }
}

/// Represents a table column with all its properties.
///
/// # Examples
///
/// ```
/// use fluxforge::core::ForgeSchemaColumn;
///
/// let mut col = ForgeSchemaColumn::new("id", "integer");
/// col.is_primary_key = true;
/// col.auto_increment = true;
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchemaColumn {
    /// Column name
    pub name: String,
    /// Data type (mapped according to configuration)
    pub data_type: String,
    /// Length for character types (VARCHAR, CHAR)
    pub length: Option<u32>,
    /// Precision for numeric types (DECIMAL, NUMERIC)
    pub precision: Option<u32>,
    /// Scale for numeric types (DECIMAL, NUMERIC)
    pub scale: Option<u32>,
    /// Whether the column allows NULL values
    pub is_nullable: bool,
    /// Whether this column is part of the primary key
    pub is_primary_key: bool,
    /// Whether this is an unsigned integer (MySQL)
    pub is_unsigned: bool,
    /// Whether this column auto-increments
    pub auto_increment: bool,
    /// Default value expression
    pub default: Option<String>,
    /// Column comment
    pub comment: Option<String>,
    /// ON UPDATE expression (e.g., CURRENT_TIMESTAMP)
    pub on_update: Option<String>,
    /// Enum/Set values for ENUM and SET types
    pub enum_values: Option<Vec<String>>,
}

impl ForgeSchemaColumn {
    /// Creates a new column with the given name and data type.
    ///
    /// # Examples
    ///
    /// ```
    /// use fluxforge::core::ForgeSchemaColumn;
    ///
    /// let col = ForgeSchemaColumn::new("email", "varchar");
    /// assert_eq!(col.name, "email");
    /// assert_eq!(col.data_type, "varchar");
    /// ```
    #[must_use]
    pub fn new(name: &str, data_type: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: data_type.to_string(),
            ..Default::default()
        }
    }
}

/// Represents a database index.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchemaIndex {
    /// Index name
    pub name: String,
    /// Columns included in the index
    pub columns: Vec<String>,
    /// Whether this is a unique index
    pub is_unique: bool,
    /// Index type (e.g., "BTREE", "FULLTEXT")
    pub index_type: Option<String>,
    /// Prefix lengths for indexed columns (MySQL)
    pub column_prefixes: Option<Vec<Option<u32>>>,
}

/// Represents a foreign key constraint.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ForgeSchemaForeignKey {
    /// Constraint name
    pub name: String,
    /// Column in this table
    pub column: String,
    /// Referenced table name
    pub ref_table: String,
    /// Referenced column name
    pub ref_column: String,
    /// ON DELETE action (CASCADE, SET NULL, etc.)
    pub on_delete: Option<String>,
    /// ON UPDATE action (CASCADE, SET NULL, etc.)
    pub on_update: Option<String>,
}

// --- UNIVERSAL-Intermediate data types ---

/// Universal value type for cross-database data representation.
///
/// Provides a common representation for values from different database systems,
/// allowing type-safe conversion and comparison during replication and verification.
///
/// # Examples
///
/// ```
/// use fluxforge::core::ForgeUniversalDataField;
///
/// let int_val = ForgeUniversalDataField::Integer(42);
/// let text_val = ForgeUniversalDataField::Text("hello".to_string());
/// let null_val = ForgeUniversalDataField::Null;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ForgeUniversalDataField {
    /// Signed 64-bit integer
    Integer(i64),
    /// Unsigned 64-bit integer
    UnsignedInteger(u64),
    /// 64-bit floating point
    Float(f64),
    /// UTF-8 text string
    Text(String),
    /// Binary data (BLOB, BYTEA)
    Binary(Vec<u8>),
    /// Boolean value
    Boolean(bool),
    /// Year value (MySQL YEAR type)
    Year(i32),
    /// Time without date
    Time(NaiveTime),
    /// Date without time
    Date(NaiveDate),
    /// Date and time without timezone
    DateTime(NaiveDateTime),
    /// Arbitrary precision decimal
    Decimal(Decimal),
    /// JSON value
    Json(serde_json::Value),
    /// UUID value
    Uuid(sqlx::types::Uuid),
    /// IP network address (PostgreSQL INET/CIDR)
    Inet(sqlx::types::ipnetwork::IpNetwork),
    /// NULL value
    Null,
    /// MySQL zero datetime (0000-00-00 00:00:00)
    ZeroDateTime,
}

/// Represents a Database row with Universal Data columns
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ForgeUniversalDataRow {
    pub columns: Vec<ForgeUniversalDataField>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ForgeUniversalDataTransferPacket {
    pub t: String,                                    // table name
    pub r: IndexMap<String, ForgeUniversalDataField>, //data row
}

/// Error types for FluxForge operations.
///
/// Provides detailed error information for database operations, type conversions,
/// and data decoding issues.
#[derive(Error, Debug)]
pub enum ForgeError {
    /// Standard database error from sqlx.
    ///
    /// Automatically converted from `sqlx::Error` via the `?` operator.
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// PostgreSQL type not yet supported.
    ///
    /// Indicates that a PostgreSQL-specific type needs to be added to the type mapping.
    #[error("PostgreSQL-Error in Columns '{column}': Type '{type_info}' is not supported yet.")]
    UnsupportedPostgresType {
        /// Column name where the unsupported type was encountered
        column: String,
        /// PostgreSQL type name
        type_info: String,
    },

    /// MySQL type not yet supported.
    ///
    /// Indicates that a MySQL-specific type needs to be added to the type mapping.
    #[error("MySQL-Error in Columns '{column}': Type '{type_info}' is not yet supported.")]
    UnsupportedMySQLType {
        /// Column name where the unsupported type was encountered
        column: String,
        /// MySQL type name
        type_info: String,
    },

    /// Column data decoding error.
    ///
    /// The type is recognized but the actual data cannot be decoded, possibly due to
    /// data corruption or unexpected format.
    #[error("Error decoding Columns '{column}' (Typ: {type_info}): {source}")]
    ColumnDecode {
        /// Column name where decoding failed
        column: String,
        /// Expected type
        type_info: String,
        /// Underlying sqlx error
        source: sqlx::Error,
    },

    /// General internal error.
    ///
    /// Indicates an unexpected internal state that should not occur during normal operation.
    #[error("General Internal Error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::str::FromStr;

    #[test]
    fn test_forge_universal_data_transfer_packet_serialization() {
        let mut row = IndexMap::new();

        row.insert("id".to_string(), ForgeUniversalDataField::Integer(1));
        row.insert(
            "uint".to_string(),
            ForgeUniversalDataField::UnsignedInteger(100),
        );
        row.insert("float".to_string(), ForgeUniversalDataField::Float(47.11));
        row.insert(
            "text".to_string(),
            ForgeUniversalDataField::Text("Hello FluxForge".to_string()),
        );
        row.insert(
            "binary".to_string(),
            ForgeUniversalDataField::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );
        row.insert("bool".to_string(), ForgeUniversalDataField::Boolean(true));
        row.insert("year".to_string(), ForgeUniversalDataField::Year(2024));
        row.insert(
            "time".to_string(),
            ForgeUniversalDataField::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()),
        );
        row.insert(
            "date".to_string(),
            ForgeUniversalDataField::Date(NaiveDate::from_ymd_opt(2024, 2, 20).unwrap()),
        );
        row.insert(
            "datetime".to_string(),
            ForgeUniversalDataField::DateTime(
                NaiveDateTime::parse_from_str("2024-02-20 12:34:56", "%Y-%m-%d %H:%M:%S").unwrap(),
            ),
        );
        row.insert(
            "decimal".to_string(),
            ForgeUniversalDataField::Decimal(Decimal::new(12345, 2)),
        );
        row.insert(
            "json".to_string(),
            ForgeUniversalDataField::Json(json!({"key": "value", "list": [1, 2, 3]})),
        );
        row.insert(
            "uuid".to_string(),
            ForgeUniversalDataField::Uuid(
                sqlx::types::Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            ),
        );
        row.insert(
            "inet".to_string(),
            ForgeUniversalDataField::Inet(
                sqlx::types::ipnetwork::IpNetwork::from_str("192.168.1.1/24").unwrap(),
            ),
        );
        row.insert("null_field".to_string(), ForgeUniversalDataField::Null);
        row.insert("zero_dt".to_string(), ForgeUniversalDataField::ZeroDateTime);

        let packet = ForgeUniversalDataTransferPacket {
            t: "test_table".to_string(),
            r: row,
        };

        // Serialize to JSON
        let serialized = serde_json::to_string(&packet).expect("Failed to serialize packet");
        println!("Serialized: {}", serialized);

        // Deserialize from JSON
        let deserialized: ForgeUniversalDataTransferPacket =
            serde_json::from_str(&serialized).expect("Failed to deserialize packet");

        // Assert equality
        assert_eq!(packet.t, deserialized.t);
        assert_eq!(packet.r.len(), deserialized.r.len());

        for (key, original_val) in &packet.r {
            let deserialized_val = deserialized
                .r
                .get(key)
                .expect("Key missing in deserialized");
            assert_eq!(
                original_val, deserialized_val,
                "Value mismatch for key: {}",
                key
            );
        }

        assert_eq!(packet, deserialized);
    }
}
