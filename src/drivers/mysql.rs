use async_trait::async_trait;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use sqlx::{
    mysql::{MySqlPool, MySqlRow}, Column, Row, TypeInfo,
    ValueRef,
};
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;

use crate::core::{
    ForgeConfig, ForgeError, ForgeForeignKey, ForgeIndex, ForgeMetadata, ForgeSchema, ForgeTable,
    ForgeUniversalValue,
};
use crate::ops::log_error_to_file;
use crate::{DatabaseDriver, ForgeColumn};

pub struct MySqlDriver {
    pub pool: MySqlPool,
    pub zero_date_on_write: bool,
}

impl MySqlDriver {
    // only visible in module, not part of public trait

    pub fn bind_universal<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        val: &'q ForgeUniversalValue,
    ) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
        match val {
            ForgeUniversalValue::Integer(i) => query.bind(i),
            ForgeUniversalValue::UnsignedInteger(u) => query.bind(u),
            ForgeUniversalValue::Float(f) => query.bind(f),
            ForgeUniversalValue::Text(s) => query.bind(s),
            ForgeUniversalValue::Binary(bin) => query.bind(bin),
            ForgeUniversalValue::Boolean(b) => query.bind(b),
            ForgeUniversalValue::Year(y) => query.bind(y),
            ForgeUniversalValue::Time(t) => query.bind(t),
            ForgeUniversalValue::Date(d) => query.bind(d),
            ForgeUniversalValue::DateTime(dt) => query.bind(dt),
            ForgeUniversalValue::Decimal(d) => query.bind(d),
            ForgeUniversalValue::Json(j) => query.bind(j),
            ForgeUniversalValue::Uuid(u) => query.bind(u.to_string()),
            ForgeUniversalValue::Inet(i) => query.bind(i.to_string()),
            ForgeUniversalValue::Null => query.bind(None::<String>),
            ForgeUniversalValue::ZeroDateTime => {
                if self.zero_date_on_write {
                    query.bind("0000-00-00 00:00:00")
                } else {
                    query.bind(None::<String>)
                }
            }
        }
    }

    pub async fn fetch_tables(&self) -> Result<Vec<ForgeTable>, Box<dyn std::error::Error>> {
        // SHOW TABLE STATUS gives also name and comment
        let rows = sqlx::query("SHOW TABLE STATUS")
            .fetch_all(&self.pool)
            .await?;

        let mut tables = Vec::new();

        for row in rows {
            // Index 0 is "Name", Index 1 is "Engine", Index 17 is "Comment" usw.
            // TODO find values from their names
            let table_name = self.get_string_at_index(&row, 0).unwrap_or_default();
            let comment = self.get_string_at_index(&row, 17); // Index für Comment in SHOW TABLE STATUS

            if table_name.is_empty() {
                continue;
            }

            tables.push(ForgeTable {
                name: table_name,
                columns: Vec::new(),
                indices: Vec::new(),
                foreign_keys: Vec::new(),
                comment,
            });
        }

        Ok(tables)
    }

    #[must_use]
    pub fn map_mysql_type(
        &self,
        mysql_column_type: &str,
        mysql_data_type: &str,
        is_unsigned: bool,
        config: &ForgeConfig,
    ) -> String {
        let target_types = config.get_type_list("mysql", "on_read");
        let mysql_column_type_lower = mysql_column_type.to_lowercase();
        let mysql_data_type_lower = mysql_data_type.to_lowercase();

        let mut target_type = target_types
            .and_then(|t| {
                t.get(&mysql_column_type_lower)
                    .or_else(|| t.get(&mysql_data_type_lower))
            })
            .cloned()
            .unwrap_or(mysql_data_type_lower.clone());

        // if unsigned rule is set we convert to bigint and dont set is_unsigned (would be obsolete/confusing with bigint)
        if let Some(mysql_cfg) = &config.mysql
            && let Some(rules) = &mysql_cfg.rules
            && let Some(on_read) = &rules.on_read
            && on_read.unsigned_int_to_bigint.unwrap_or(false)
            && is_unsigned
            && mysql_data_type_lower.contains("int")
        {
            target_type = "bigint".to_string();
        }

        target_type
    }

    pub async fn fetch_columns(
        &self,
        table_name: &str,
        config: &ForgeConfig,
    ) -> Result<Vec<ForgeColumn>, Box<dyn std::error::Error>> {
        // SHOW FULL FIELDS gives:
        // Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
        let query = format!("SHOW FULL FIELDS FROM `{table_name}`");
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut columns = Vec::new();
        let target_types = config.get_type_list("mysql", "on_read");

        let unsigned_int_to_bigint = config
            .mysql
            .as_ref()
            .and_then(|c| c.rules.as_ref())
            .and_then(|r| r.on_read.as_ref())
            .and_then(|o| o.unsigned_int_to_bigint)
            .unwrap_or(false);

        for row in rows {
            // helper for reliable reading because mysql gives metadata as (VAR)BINARY
            let get_s = |col: &str| -> String {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };

            let col_name = get_s("Field").clone();
            let mysql_column_type = get_s("Type"); // i.e. "int(11) unsigned" or "enum('a','b')"

            // extract pure data type. "int (11) unsigned" -> "int",  or "enum('a','b')" -> "enum"
            let mysql_data_type = mysql_column_type
                .split(['(', ' '])
                .next()
                .unwrap_or(&mysql_column_type)
                .to_lowercase();

            // mapping logic from config file, if set
            let mut target_data_type = target_types
                .and_then(|t| {
                    t.get(&mysql_column_type)
                        .or_else(|| t.get(&mysql_data_type))
                })
                .cloned()
                .unwrap_or(mysql_data_type.clone());

            // special case for unsigned
            // if unsigned_int_to_bigint in config is set, we convert unsigned always to bigint and set is_unsigned to false
            // because a set is_unsigned would be obsolete/confusing with bigint
            let mut is_unsigned = mysql_column_type.to_lowercase().contains("unsigned");
            if mysql_data_type.contains("int") && is_unsigned && unsigned_int_to_bigint {
                target_data_type = "bigint".to_string();
                is_unsigned = false;
            }

            // extract enum values
            let enum_values = if mysql_data_type == "enum" || mysql_data_type == "set" {
                Some(self.parse_mysql_enum_values(&mysql_column_type))
            } else {
                None
            };

            // extract extra info like AUTO_INCREMENT, ON UPDATE ...
            let extra = get_s("Extra");
            // if extra starts with  "ON UPDATE " we use the remaining and assign it to on_update variable
            let on_update = if extra.len() >= 10 && extra[..10].eq_ignore_ascii_case("ON UPDATE ") {
                Some(extra[10..].to_string())
            } else {
                None
            };

            // extract length (from strings) or precision/scale from numbers
            let mut length: Option<u32> = None;
            let mut precision: Option<u32> = None;
            let mut scale: Option<u32> = None;

            if let Some(start) = mysql_column_type.find('(')
                && let Some(end_rel) = mysql_column_type[start + 1..].find(')')
            {
                let inside = &mysql_column_type[start + 1..start + 1 + end_rel];
                let inside_clean = inside.replace(' ', "");

                if mysql_data_type.eq_ignore_ascii_case("char")
                    || mysql_data_type.eq_ignore_ascii_case("varchar")
                    || mysql_data_type.eq_ignore_ascii_case("binary")
                    || mysql_data_type.eq_ignore_ascii_case("varbinary")
                    || mysql_data_type.eq_ignore_ascii_case("bit")
                    || mysql_data_type.eq_ignore_ascii_case("datetime")
                    || mysql_data_type.eq_ignore_ascii_case("timestamp")
                    || mysql_data_type.eq_ignore_ascii_case("time")
                {
                    if let Ok(l) = inside_clean.parse::<u32>() {
                        length = Some(l);
                    }
                } else if mysql_data_type.eq_ignore_ascii_case("float")
                    || mysql_data_type.eq_ignore_ascii_case("decimal")
                {
                    let parts: Vec<&str> = inside_clean.split(',').collect();
                    if let Some(p0) = parts.first()
                        && let Ok(p) = p0.parse::<u32>()
                    {
                        precision = Some(p);
                    }
                    if let Some(p1) = parts.get(1)
                        && let Ok(s) = p1.parse::<u32>()
                    {
                        scale = Some(s);
                    }
                }
            }

            columns.push(ForgeColumn {
                name: col_name,
                data_type: target_data_type,
                length,
                precision,
                scale,
                is_nullable: get_s("Null") == "YES",
                is_primary_key: get_s("Key") == "PRI",
                is_unsigned,
                auto_increment: extra.contains("auto_increment"),
                default: row
                    .try_get::<Option<Vec<u8>>, _>("Default")
                    .ok()
                    .flatten()
                    .map(|b| String::from_utf8_lossy(&b).into_owned()),
                comment: Some(get_s("Comment")),
                on_update,
                enum_values,
            });
        }
        Ok(columns)
    }

    // extracts 'bla','fasel' from enum('bla','fasel') / set('a','b')
    #[must_use]
    pub fn parse_mysql_enum_values(&self, col_type: &str) -> Vec<String> {
        let trimmed = col_type.trim();
        let lower = trimmed.to_lowercase();
        let without_prefix = if lower.starts_with("enum(") || lower.starts_with("set(") {
            trimmed.split_once('(').map_or(trimmed, |x| x.1)
        } else {
            trimmed
        };

        without_prefix
            .trim_end_matches(')')
            .split(',')
            .map(|v| v.trim_matches('\'').to_string())
            .collect()
    }

    pub async fn fetch_indices(
        &self,
        table_name: &str,
    ) -> Result<Vec<ForgeIndex>, Box<dyn std::error::Error>> {
        // SHOW INDEX FROM `table` gives:
        // Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, ...
        let query = format!("SHOW INDEX FROM `{table_name}`");
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut indices_map: HashMap<String, ForgeIndex> = HashMap::new();

        for row in rows {
            // helper for reliable reading of metadata
            let get_s = |col: &str| -> String {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };

            let index_name = get_s("Key_name");
            let column_name = get_s("Column_name");
            let index_type = get_s("Index_type");
            let seq_in_index = row.try_get::<u32, _>("Seq_in_index").unwrap_or(1);

            let seq_index = if seq_in_index > 0 {
                (seq_in_index - 1) as usize
            } else {
                0
            };

            let sub_part = row
                .try_get::<Option<i64>, _>("Sub_part")
                .ok()
                .flatten()
                .map(|v| v as u32);

            // Non_unique is usually Integer (0 = Unique/PK, 1 = Normal)
            let is_unique = row.try_get::<i64, _>("Non_unique").unwrap_or(1) == 0;

            // we ignore primary, because is it covered by  ForgeColumn.is_primary_key
            if index_name == "PRIMARY" {
                continue;
            }

            // find index in map else create
            let entry = indices_map.entry(index_name.clone()).or_insert(ForgeIndex {
                name: index_name,
                columns: Vec::new(),
                is_unique,
                index_type: None,
                column_prefixes: None,
            });

            if entry.index_type.is_none() && !index_type.is_empty() {
                entry.index_type = Some(index_type);
            }

            if entry.columns.len() <= seq_index {
                entry.columns.resize(seq_index + 1, String::new());
            }
            entry.columns[seq_index] = column_name;

            if sub_part.is_some() || entry.column_prefixes.is_some() {
                let prefixes = entry
                    .column_prefixes
                    .get_or_insert_with(|| vec![None; entry.columns.len()]);
                if prefixes.len() < entry.columns.len() {
                    prefixes.resize(entry.columns.len(), None);
                }
                prefixes[seq_index] = sub_part;
            }
        }

        // convert map into Vec
        Ok(indices_map.into_values().collect())
    }

    pub async fn fetch_foreign_keys(
        &self,
        _table_name: &str,
    ) -> Result<Vec<ForgeForeignKey>, Box<dyn std::error::Error>> {
        // TODO implement after first release
        Ok(Vec::new())
    }

    #[must_use]
    pub fn field_migration_sql(&self, field: ForgeColumn, config: &ForgeConfig) -> String {
        let target_types = config.get_type_list("mysql", "on_write");

        let data_type_lower = field.data_type.to_lowercase();
        let sql_type = target_types
            .and_then(|t| t.get(&data_type_lower))
            .cloned()
            .unwrap_or(data_type_lower);

        let mut ret = String::new();

        //  Name
        ret.push_str(&format!("`{}`", field.name));

        // Type & Parameters
        ret.push_str(&format!(" {sql_type}"));

        match sql_type.as_str() {
            "decimal" => {
                if let (Some(p), Some(s)) = (field.precision, field.scale) {
                    ret.push_str(&format!("({p},{s})"));
                } else if let Some(p) = field.precision {
                    ret.push_str(&format!("({p})"));
                }
            }
            "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
                if field.is_unsigned {
                    ret.push_str(" unsigned");
                }
            }

            "varchar" | "char" | "binary" | "varbinary" | "bit" | "datetime" | "timestamp"
            | "time" => {
                if let Some(l) = field.length {
                    ret.push_str(&format!("({l})"));
                }
            }
            "enum" | "set" => {
                if let Some(ref vals) = field.enum_values {
                    let formatted_vals: Vec<String> =
                        vals.iter().map(|v| format!("'{v}'")).collect();
                    ret.push_str(&format!("({})", formatted_vals.join(",")));
                }
            }
            _ => {}
        }

        let sql_type_lower = sql_type.to_lowercase();
        let skip_default = sql_type_lower.contains("text")
            || sql_type_lower.contains("blob")
            || sql_type_lower == "json";

        // Nullable & Default NULL
        if field.is_nullable {
            ret.push_str(" NULL");
            if field.default.is_none() && !skip_default {
                ret.push_str(" DEFAULT NULL");
            }
        } else {
            ret.push_str(" NOT NULL");
        }

        // Default Value
        if let Some(ref def) = field.default
            && !skip_default
        {
            if def.to_lowercase() == "current_timestamp" {
                ret.push_str(" DEFAULT CURRENT_TIMESTAMP");
            } else {
                ret.push_str(&format!(" DEFAULT '{def}'"));
            }
        }

        // Auto Increment
        if field.auto_increment {
            ret.push_str(" AUTO_INCREMENT");
        }

        // On Update
        if let Some(ref on_upd) = field.on_update {
            ret.push_str(&format!(" ON UPDATE {on_upd}"));
        }

        ret
    }

    /// builds CREATE TABLE Statement for `MySQL`
    #[must_use]
    pub fn build_mysql_create_table_sql(&self, table: &ForgeTable, config: &ForgeConfig) -> String {
        let mut col_defs = Vec::new();
        let mut pks = Vec::new();

        for col in &table.columns {
            let def = self.field_migration_sql(col.clone(), config);
            col_defs.push(def);

            if col.is_primary_key {
                pks.push(format!("`{}`", col.name));
            }
        }

        if !pks.is_empty() {
            col_defs.push(format!("  PRIMARY KEY ({})", pks.join(", ")));
        }

        format!(
            "CREATE TABLE `{}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
            table.name,
            col_defs.join(",\n")
        )
    }

    pub fn create_table_migration_sql(
        &self,
        dst_table: &ForgeTable,
        config: &ForgeConfig,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut stmts = Vec::new();
        let sql = self.build_mysql_create_table_sql(dst_table, config);
        stmts.push(sql);
        // after table is created, create all non-primary-key indices
        for index in &dst_table.indices {
            let idx_sql = self.build_mysql_create_index_sql(&dst_table.name, index);
            stmts.push(idx_sql);
        }
        Ok(stmts)
    }

    pub fn delete_table_migration_sql(
        &self,
        dst_table: &ForgeTable,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let sql = format!("DROP TABLE `{}`;", dst_table.name);
        Ok(vec![sql])
    }
    pub fn alter_table_migration_sql(
        &self,
        src_table: &ForgeTable,
        dst_table: &ForgeTable,
        config: &ForgeConfig,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut all_statements = Vec::new();

        // ---- Columns ----
        let mut src_cols: HashMap<String, &ForgeColumn> = HashMap::new();
        for col in &src_table.columns {
            src_cols.insert(col.name.clone(), col);
        }

        let mut dst_cols: HashMap<String, &ForgeColumn> = HashMap::new();
        for col in &dst_table.columns {
            dst_cols.insert(col.name.clone(), col);
        }

        // src is desired state (from source)
        // dst is actual state (of target that should be modified)

        // Check all columns in src
        for src_col in &src_table.columns {
            if let Some(dst_col) = dst_cols.get(&src_col.name) {
                // In both -> MODIFY if changed
                let sql = self.modify_column_migration(
                    &src_table.name,
                    src_col,
                    dst_col,
                    config,
                    destructive,
                );
                if !sql.is_empty() {
                    all_statements.push(sql);
                }
            } else {
                // In SRC but NOT in DST -> ADD
                all_statements.push(self.add_column_migration(&src_table.name, src_col, config));
            }
        }

        // Check all columns in DST (current state)
        if destructive {
            for dst_col in &dst_table.columns {
                if !src_cols.contains_key(&dst_col.name) {
                    // In DST but NOT in SRC -> DROP (if destructive)
                    all_statements.push(self.drop_column_migration(&dst_table.name, &dst_col.name));
                }
            }
        }

        // ---- Indices ----
        let mut src_idx_map: HashMap<String, &ForgeIndex> = HashMap::new();
        for idx in &src_table.indices {
            src_idx_map.insert(idx.name.clone(), idx);
        }
        let mut dst_idx_map: HashMap<String, &ForgeIndex> = HashMap::new();
        for idx in &dst_table.indices {
            dst_idx_map.insert(idx.name.clone(), idx);
        }

        // Check all indices in SRC (desired state)
        for (name, src_idx) in &src_idx_map {
            match dst_idx_map.get(name) {
                None => {
                    // In SRC but NOT in DST -> CREATE
                    let sql = self.build_mysql_create_index_sql(&src_table.name, src_idx);
                    all_statements.push(sql);
                }
                Some(dst_idx) => {
                    // In both -> replace if changed
                    if !self.indices_equal(dst_idx, src_idx) {
                        let drop_sql = self.build_mysql_drop_index_sql(&src_table.name, name);
                        let create_sql =
                            self.build_mysql_create_index_sql(&src_table.name, src_idx);
                        all_statements.push(drop_sql);
                        all_statements.push(create_sql);
                    }
                }
            }
        }

        // Check all indices in DST (current state)
        if destructive {
            for name in dst_idx_map.keys() {
                if !src_idx_map.contains_key(name) {
                    // In DST but NOT in SRC -> DROP (if destructive)
                    let sql = self.build_mysql_drop_index_sql(&dst_table.name, name);
                    all_statements.push(sql);
                }
            }
        }

        Ok(all_statements)
    }

    #[must_use]
    pub fn add_column_migration(
        &self,
        table_name: &str,
        src_col: &ForgeColumn,
        config: &ForgeConfig,
    ) -> String {
        self.build_mysql_add_column_sql(table_name, src_col, config)
    }

    #[must_use]
    pub fn drop_column_migration(&self, table_name: &str, col_name: &str) -> String {
        format!("ALTER TABLE `{table_name}` DROP COLUMN `{col_name}`;")
    }

    #[must_use]
    pub fn modify_column_migration(
        &self,
        table_name: &str,
        src_col: &ForgeColumn, //
        dst_col: &ForgeColumn,
        config: &ForgeConfig,
        _destructive: bool,
    ) -> String {
        // src is desired state (from source)
        // dst is actual state (of target that should be modified)

        let mut changed = src_col.data_type != dst_col.data_type
            || src_col.length != dst_col.length
            || src_col.is_nullable != dst_col.is_nullable;

        // special handling for FLOAT: numerical comparison of default values
        if !changed {
            if src_col.data_type.eq_ignore_ascii_case("float") {
                let src_def_f = src_col.default.as_ref().and_then(|s| s.parse::<f64>().ok());
                let dst_def_f = dst_col.default.as_ref().and_then(|s| s.parse::<f64>().ok());

                if src_def_f != dst_def_f {
                    changed = true;
                }
            } else if src_col.default != dst_col.default {
                changed = true;
            }
        }

        if changed {
            let sql_def = self.field_migration_sql(src_col.clone(), config);
            return format!("ALTER TABLE `{table_name}` MODIFY COLUMN {sql_def};");
        }
        String::new()
    }

    /// builds ALTER TABLE ADD COLUMN Statement
    #[must_use]
    pub fn build_mysql_add_column_sql(
        &self,
        table_name: &str,
        col: &ForgeColumn,
        config: &ForgeConfig,
    ) -> String {
        let sql_def = self.field_migration_sql(col.clone(), config);
        format!("ALTER TABLE `{table_name}` ADD COLUMN {sql_def};")
    }

    /// builds CREATE INDEX Statement
    #[must_use]
    pub fn build_mysql_create_index_sql(&self, table_name: &str, index: &ForgeIndex) -> String {
        let index_type = index.index_type.as_deref().unwrap_or("").to_uppercase();
        let is_fulltext = index_type == "FULLTEXT";
        let is_spatial = index_type == "SPATIAL";
        let type_prefix = if is_fulltext {
            "FULLTEXT "
        } else if is_spatial {
            "SPATIAL "
        } else {
            ""
        };
        let unique = if index.is_unique && !is_fulltext && !is_spatial {
            "UNIQUE "
        } else {
            ""
        };
        let cols = index
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let prefix = index
                    .column_prefixes
                    .as_ref()
                    .and_then(|p| p.get(i))
                    .and_then(|v| *v);
                if let Some(len) = prefix {
                    format!("`{c}`({len})")
                } else {
                    format!("`{c}`")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE {}{}INDEX `{}` ON `{}` ({});",
            unique, type_prefix, index.name, table_name, cols
        )
    }

    /// builds DROP INDEX Statement
    #[must_use]
    pub fn build_mysql_drop_index_sql(&self, table_name: &str, index_name: &str) -> String {
        format!("DROP INDEX `{index_name}` ON `{table_name}`;")
    }

    /// comparison if two indexes are identical (without names, thats already checked via map-key)
    #[must_use]
    pub fn indices_equal(&self, a: &ForgeIndex, b: &ForgeIndex) -> bool {
        if a.is_unique != b.is_unique {
            return false;
        }
        if !a
            .index_type
            .as_deref()
            .unwrap_or("")
            .eq_ignore_ascii_case(b.index_type.as_deref().unwrap_or(""))
        {
            return false;
        }
        if a.columns.len() != b.columns.len() {
            return false;
        }
        let a_prefixes = a
            .column_prefixes
            .clone()
            .unwrap_or_else(|| vec![None; a.columns.len()]);
        let b_prefixes = b
            .column_prefixes
            .clone()
            .unwrap_or_else(|| vec![None; b.columns.len()]);
        if a_prefixes.len() != b_prefixes.len() {
            return false;
        }
        for (i, col) in a.columns.iter().enumerate() {
            if b.columns.get(i) != Some(col) {
                return false;
            }
            if a_prefixes.get(i) != b_prefixes.get(i) {
                return false;
            }
        }
        true
    }

    /// read a column from an index as string
    /// helper if `MySQL` VARBINARY or BLOB are returned from mysql.
    pub fn get_string_at_index(&self, row: &sqlx::mysql::MySqlRow, index: usize) -> Option<String> {
        let bytes: Vec<u8> = row.try_get(index).unwrap_or_default();

        if bytes.is_empty() {
            return None;
        }

        // convert into UTF8, ingnore invalid chars
        Some(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// special handling for empty mysql date and datetime values that start with 0000-00-00
    /// postgres does not understand those "0000-00-00" values
    pub fn handle_datetime(
        &self,
        row: &MySqlRow,
        index: usize,
    ) -> Result<ForgeUniversalValue, sqlx::Error> {
        let column = &row.columns()[index];
        let type_name = column.type_info().name();

        // try normal decode
        if type_name == "TIMESTAMP" {
            // with TIMESTAMP tries sqlx often UTC
            if let Ok(dt_utc) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(index) {
                return Ok(ForgeUniversalValue::DateTime(dt_utc.naive_utc()));
            }
        } else if let Ok(dt) = row.try_get::<chrono::NaiveDateTime, _>(index) {
            return Ok(ForgeUniversalValue::DateTime(dt));
        }

        // special case for MySQL "Zero-Dates" or decode error
        let raw_value = row.try_get_raw(index)?; // returns sqlx::Error zurück, if it fails

        if raw_value.is_null() {
            return Ok(ForgeUniversalValue::Null);
        }

        // sqlx could not decode it into NaiveDateTime
        // we check for MySQL "0000-00-00" pattern as String-Fallback.
        if let Ok(s) = row.try_get::<String, _>(index) {
            if s.starts_with("0000-00-00") {
                return Ok(ForgeUniversalValue::ZeroDateTime);
            }
            // valid string, but no Zero-Date,
            // we save it as string
            return Ok(ForgeUniversalValue::Text(s));
        }

        // if everythin else fails, mayb binary trash in columns, we return it as decoding-error
        Err(sqlx::Error::Decode(
            "Invalid DateTime/Timestamp or corrupted Zero-Date".into(),
        ))
    }

    /// maps a MySQL-row into intermediate and DB-neutral ForgeUniversalValue-Structure
    pub fn map_row_to_universal_values(
        &self,
        row: &MySqlRow,
    ) -> Result<Vec<ForgeUniversalValue>, ForgeError> {
        row.columns()
            .iter()
            .map(|col| {
                let i = col.ordinal();
                let type_name = col.type_info().name().to_uppercase();
                let col_name = col.name();

                // local error adapter
                let to_err = |e| ForgeError::ColumnDecode {
                    column: col_name.to_string(),
                    type_info: type_name.clone(),
                    source: e,
                };

                // clean NULL check beforehand
                if row.try_get_raw(i).map(|v| v.is_null()).unwrap_or(true) {
                    return Ok(ForgeUniversalValue::Null);
                }

                let val = match type_name.as_str() {
                    "DATETIME" | "TIMESTAMP" => self.handle_datetime(row, i).map_err(to_err)?,

                    "DATE" => ForgeUniversalValue::Date(
                        row.try_get::<chrono::NaiveDate, _>(i).map_err(to_err)?,
                    ),

                    "TIME" => ForgeUniversalValue::Time(
                        row.try_get::<chrono::NaiveTime, _>(i).map_err(to_err)?,
                    ),

                    "YEAR" => {
                        let year = match row.try_get::<u16, _>(i) {
                            Ok(val) => i32::from(val),
                            Err(_) => {
                                if let Ok(val) = row.try_get::<i16, _>(i) {
                                    i32::from(val)
                                } else {
                                    let raw = row.try_get::<String, _>(i).map_err(to_err)?;
                                    raw.parse::<i32>().map_err(|_| {
                                        to_err(sqlx::Error::Decode("Invalid YEAR value".into()))
                                    })?
                                }
                            }
                        };
                        ForgeUniversalValue::Year(year)
                    }

                    "TINYINT(1)" | "BOOLEAN" | "BOOL" => {
                        ForgeUniversalValue::Boolean(row.try_get::<bool, _>(i).map_err(to_err)?)
                    }

                    "TINYINT" | "SMALLINT" | "INT" | "INTEGER" | "MEDIUMINT" | "BIGINT"
                    | "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED"
                    | "BIGINT UNSIGNED" => {
                        let is_unsigned = type_name.contains("UNSIGNED");

                        if is_unsigned {
                            ForgeUniversalValue::UnsignedInteger(
                                row.try_get::<u64, _>(i).map_err(to_err)?,
                            )
                        } else {
                            ForgeUniversalValue::Integer(row.try_get::<i64, _>(i).map_err(to_err)?)
                        }
                    }

                    "JSON" => ForgeUniversalValue::Json(
                        row.try_get::<serde_json::Value, _>(i).map_err(to_err)?,
                    ),

                    "DOUBLE" | "FLOAT" => {
                        ForgeUniversalValue::Float(row.try_get::<f64, _>(i).map_err(to_err)?)
                    }

                    "DECIMAL" => ForgeUniversalValue::Decimal(
                        row.try_get::<rust_decimal::Decimal, _>(i).map_err(to_err)?,
                    ),

                    "BLOB" | "VARBINARY" | "BINARY" => {
                        ForgeUniversalValue::Binary(row.try_get::<Vec<u8>, _>(i).map_err(to_err)?)
                    }
                    "BIT" => {
                        let v = row.try_get::<u64, _>(i).map_err(to_err)?;
                        ForgeUniversalValue::Binary(v.to_be_bytes().to_vec())
                    }

                    // String-Fallback for VARCHAR, TEXT etc.
                    "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT"
                    | "ENUM" | "SET" => {
                        ForgeUniversalValue::Text(row.try_get::<String, _>(i).map_err(to_err)?)
                    }

                    // Catch-All with error reporting for completely unknown types
                    _ => {
                        return Err(ForgeError::UnsupportedMySQLType {
                            column: col_name.to_string(),
                            type_info: type_name.clone(),
                        });
                    }
                };

                Ok(val)
            })
            .collect() // Sammelt Result<Vec<ForgeUniversalValue>, ForgeError>
    }
}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    async fn db_is_empty(&self) -> Result<bool, Box<dyn Error>> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count == 0)
    }

    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
        // get database name from database
        let db_name: String = sqlx::query_scalar("SELECT DATABASE()")
            .fetch_one(&self.pool)
            .await?;

        // get all basic table structures
        let mut tables = self.fetch_tables().await?;

        // get details of all table
        for table in &mut tables {
            // fetch all columns with applying mapping config
            table.columns = self.fetch_columns(&table.name, config).await?;

            // fetch all indices (no mapping conf for them)
            table.indices = self.fetch_indices(&table.name).await?;

            // fetch all foreign keys (no mapping conf for them)
            table.foreign_keys = self.fetch_foreign_keys(&table.name).await?;
        }

        Ok(ForgeSchema {
            metadata: ForgeMetadata {
                source_system: "mysql".to_string(),
                source_database_name: db_name,
                created_at: chrono::Local::now().to_rfc3339(),
                forge_version: env!("CARGO_PKG_VERSION").to_string(),
                config_file: String::new(),
            },
            tables,
        })
    }

    async fn diff_and_apply_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        verbose: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        // source = new schema (from source db)
        // target = actual schema (of target that will be changed)

        let target_schema = self.fetch_schema(config).await?;
        let mut all_statements = Vec::new();

        let mut source_tables: HashMap<String, &ForgeTable> = HashMap::new();
        for table in &source_schema.tables {
            source_tables.insert(table.name.clone(), table);
        }

        let mut target_tables: HashMap<String, &ForgeTable> = HashMap::new();
        for table in &target_schema.tables {
            target_tables.insert(table.name.clone(), table);
        }

        // compare all tables that are in source_schema
        for source_table in &source_schema.tables {
            if let Some(target_table) = target_tables.get(&source_table.name) {
                // if in source and target -> alter_table_migration_sql()
                let stmts = self.alter_table_migration_sql(
                    source_table,
                    target_table,
                    config,
                    destructive,
                )?;
                all_statements.extend(stmts);
            } else {
                // if in source but not in target -> create_table_migration_sql()
                let stmts = self.create_table_migration_sql(source_table, config)?;
                all_statements.extend(stmts);
            }
        }

        // if in target, but not in source AND destructive -> delete_table_migration_sql()
        if destructive {
            for table in &target_schema.tables {
                if !source_tables.contains_key(&table.name) {
                    let stmts = self.delete_table_migration_sql(table)?;
                    all_statements.extend(stmts);
                }
            }
        }

        if !dry_run {
            let mut success_count = 0;
            for sql in &all_statements {
                sqlx::query(sql).execute(&self.pool).await?;
                success_count += 1;
            }
            if verbose {
                println!("{success_count} SQL-Statements executed.");
            }
        }

        Ok(all_statements)
    }

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
    > {
        let query_string = format!("SELECT * FROM `{table_name}`");

        let stream = async_stream::try_stream! {
            let mut rows = sqlx::query(&query_string).fetch(&self.pool);

            while let Some(row) = rows.next().await {
                let row: sqlx::mysql::MySqlRow = row?;
                let values = self.map_row_to_universal_values(&row)?;

                let mut row_map = IndexMap::new();
                for (col, val) in row.columns().iter().zip(values) {
                    row_map.insert(col.name().to_string(), val);
                }

                yield row_map;
            }
        };

        Ok(Box::pin(stream))
    }

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
    > {
        let order_clause = if order_by.is_empty() {
            String::new()
        } else {
            let columns = order_by
                .iter()
                .map(|col| format!("`{col}`"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {columns}")
        };

        let query_string = format!("SELECT * FROM `{table_name}`{order_clause}");

        let stream = async_stream::try_stream! {
            let mut rows = sqlx::query(&query_string).fetch(&self.pool);

            while let Some(row) = rows.next().await {
                let row: sqlx::mysql::MySqlRow = row?;
                let values = self.map_row_to_universal_values(&row)?;

                let mut row_map = IndexMap::new();
                for (col, val) in row.columns().iter().zip(values) {
                    row_map.insert(col.name().to_string(), val);
                }

                yield row_map;
            }
        };

        Ok(Box::pin(stream))
    }

    async fn insert_chunk(
        &self,
        table_name: &str,
        dry_run: bool,
        halt_on_error: bool,
        chunk: Vec<IndexMap<String, ForgeUniversalValue>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if chunk.is_empty() {
            return Ok(());
        }

        // extract column names from first record
        let first_row = chunk.first().ok_or("Chunk is empty")?;
        let columns: Vec<String> = first_row.keys().cloned().collect();
        let column_names = columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");

        // prepare SQL-Statement
        let mut sql = format!("INSERT INTO `{table_name}` ({column_names}) VALUES ");

        let mut placeholders = Vec::new();
        for _ in 0..chunk.len() {
            let row_placeholders = vec!["?"; columns.len()].join(", ");
            placeholders.push(format!("({row_placeholders})"));
        }
        sql.push_str(&placeholders.join(", "));

        if dry_run {
            println!("Dry run SQL = {sql}");
        } else {
            // create query and bind values
            let mut query = sqlx::query(&sql);

            for row in &chunk {
                for col in &columns {
                    // value from IndexMap holen, Fallback to Null
                    let val = row.get(col).unwrap_or(&ForgeUniversalValue::Null);

                    // binding based on UniveralEnums
                    query = self.bind_universal(query, val);
                }
            }

            if let Err(e) = query.execute(&self.pool).await {
                eprintln!(
                    "Batch insert failed for table `{table_name}`. Retrying row-by-row for logging..."
                );

                // we build SQL for one row at a time: INSERT INTO `table` (`col1`) VALUES (?)
                let single_sql = format!(
                    "INSERT INTO `{}` ({}) VALUES ({})",
                    table_name,
                    columns
                        .iter()
                        .map(|c| format!("`{c}`"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    vec!["?"; columns.len()].join(", ")
                );

                for row_map in &chunk {
                    let mut single_query = sqlx::query(&single_sql);

                    for col in &columns {
                        let val = row_map.get(col).unwrap_or(&ForgeUniversalValue::Null);
                        single_query = self.bind_universal(single_query, val);
                    }

                    // execute one row
                    if let Err(single_err) = single_query.execute(&self.pool).await {
                        let row_data = format!("{row_map:?}");
                        let err_msg = single_err.to_string();

                        // now we can log the error of one row
                        eprintln!("Error in Row: {row_data} | Error: {err_msg}");
                        log_error_to_file(table_name, &row_data, &err_msg);
                    }
                }
                if halt_on_error {
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    async fn get_table_row_count(
        &self,
        table_name: &str,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let query = format!("SELECT COUNT(*) FROM `{table_name}`");
        let row: (i64,) = sqlx::query_as(&query).fetch_one(&self.pool).await?;
        Ok(row.0 as u64)
    }
}
