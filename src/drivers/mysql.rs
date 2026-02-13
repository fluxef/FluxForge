use crate::core::{
    ForgeConfig, ForgeForeignKey, ForgeIndex, ForgeMetadata, ForgeSchema, ForgeTable,
    ForgeUniversalValue,
};
use crate::{DatabaseDriver, ForgeColumn};
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use sqlx::{
    Column, Row, TypeInfo,
    mysql::{MySqlPool, MySqlRow},
};
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;
use serde_json::Value;

pub struct MySqlDriver {
    pub pool: MySqlPool,
}

impl MySqlDriver {
    // Diese Funktionen sind nur innerhalb dieses Moduls sichtbar
    // und gehören NICHT zum öffentlichen Trait.
    pub async fn fetch_tables(&self) -> Result<Vec<ForgeTable>, Box<dyn std::error::Error>> {
        // SHOW TABLE STATUS gibt uns Name und Comment
        let rows = sqlx::query("SHOW TABLE STATUS")
            .fetch_all(&self.pool)
            .await?;

        let mut tables = Vec::new();

        for row in rows {
            // Index 0 ist "Name", Index 1 ist "Engine", Index 17 ist "Comment" usw.
            // Sicherer ist es jedoch, den Index einmalig über den Namen zu suchen:
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

        // Unsigned Regel anwenden (basierend auf der neuen Struktur)
        if let Some(mysql_cfg) = &config.mysql {
            if let Some(rules) = &mysql_cfg.rules {
                if let Some(on_read) = &rules.on_read {
                    if on_read.unsigned_int_to_bigint.unwrap_or(false)
                        && is_unsigned
                        && mysql_data_type_lower.contains("int")
                    {
                        target_type = "bigint".to_string();
                    }
                }
            }
        }

        target_type
    }

    async fn fetch_columns(
        &self,
        table_name: &str,
        config: &ForgeConfig,
    ) -> Result<Vec<ForgeColumn>, Box<dyn std::error::Error>> {
        // SHOW FULL FIELDS liefert:
        // Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment
        let query = format!("SHOW FULL FIELDS FROM `{}`", table_name);
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
            // Hilfsfunktion zum sicheren Lesen von (VAR)BINARY Metadaten
            let get_s = |col: &str| -> String {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };

            let col_name = get_s("Field").to_string();
            let mysql_column_type = get_s("Type"); // z.B. "int(11) unsigned" oder "enum('a','b')"

            // Extrahiere den reinen Datentyp für Enum-Check etc.
            let mysql_data_type = mysql_column_type
                .split(|c| c == '(' || c == ' ')
                .next()
                .unwrap_or(&mysql_column_type)
                .to_lowercase();

            // START MAPPING LOGIK
            let mut target_data_type = target_types
                .and_then(|t| {
                    t.get(&mysql_column_type)
                        .or_else(|| t.get(&mysql_data_type))
                })
                .cloned()
                .unwrap_or(mysql_data_type.clone());
            // END MAPPING LOGIK

            // START Unsigned Regel anwenden
            // WENN wir auf bigint mappen, setzen wir is_unsigned auf false
            // weil wir das nicht mehr brauchen
            // nur wenn wir nicht auf bigint mappen behalten wir es, weil wir dann
            // nicht nach postgres (internes format) gehen
            // und das evtl. wieder für das zurückschreiben nach mysql brauchen
            let mut is_unsigned = mysql_column_type.to_lowercase().contains("unsigned");

            if mysql_data_type.contains("int") && is_unsigned && unsigned_int_to_bigint {
                target_data_type = "bigint".to_string();
                is_unsigned = false;
            }
            // ENDE Unsigned Regel anwenden

            // Enum-Werte extrahieren, falls vorhanden
            let enum_values = if mysql_data_type == "enum" {
                Some(self.parse_mysql_enum_values(&mysql_column_type))
            } else {
                None
            };

            // Extra-Informationen auswerten (AUTO_INCREMENT, ON UPDATE ...)
            let extra = get_s("Extra");
            // Wenn Extra mit "ON UPDATE " beginnt, Rest als on_update wert übernehmen (case-insensitive)
            let on_update = if extra.len() >= 10 && extra[..10].eq_ignore_ascii_case("ON UPDATE ") {
                Some(extra[10..].to_string())
            } else {
                None
            };

            // Parse length/precision/scale from column type string
            let mut length: Option<u32> = None;
            let mut precision: Option<u32> = None;
            let mut scale: Option<u32> = None;

            if let Some(start) = mysql_column_type.find('(') {
                if let Some(end_rel) = mysql_column_type[start + 1..].find(')') {
                    let inside = &mysql_column_type[start + 1..start + 1 + end_rel];
                    let inside_clean = inside.replace(' ', "");

                    if mysql_data_type.eq_ignore_ascii_case("char")
                        || mysql_data_type.eq_ignore_ascii_case("varchar")
                    {
                        if let Ok(l) = inside_clean.parse::<u32>() {
                            length = Some(l);
                        }
                    } else if mysql_data_type.eq_ignore_ascii_case("float")
                        || mysql_data_type.eq_ignore_ascii_case("decimal")
                    {
                        let parts: Vec<&str> = inside_clean.split(',').collect();
                        if let Some(p0) = parts.get(0) {
                            if let Ok(p) = p0.parse::<u32>() {
                                precision = Some(p);
                            }
                        }
                        if let Some(p1) = parts.get(1) {
                            if let Ok(s) = p1.parse::<u32>() {
                                scale = Some(s);
                            }
                        }
                    }
                }
            }

            columns.push(ForgeColumn {
                name: col_name,
                data_type: target_data_type,
                // Length/Precision/Scale parsed from MySQL column type if present
                length: length,
                precision: precision,
                scale: scale,
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

    /// Extrahiert 'val1','val2' aus enum('val1','val2')
    pub fn parse_mysql_enum_values(&self, col_type: &str) -> Vec<String> {
        col_type
            .trim_start_matches("enum(")
            .trim_end_matches(')')
            .split(',')
            .map(|v| v.trim_matches('\'').to_string())
            .collect()
    }

    async fn fetch_indices(
        &self,
        table_name: &str,
    ) -> Result<Vec<ForgeIndex>, Box<dyn std::error::Error>> {
        // SHOW INDEX FROM `table` liefert:
        // Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, ...
        let query = format!("SHOW INDEX FROM `{}`", table_name);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut indices_map: HashMap<String, ForgeIndex> = HashMap::new();

        for row in rows {
            // Hilfsfunktion für sicheres Lesen der Metadaten
            let get_s = |col: &str| -> String {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };

            let index_name = get_s("Key_name");
            let column_name = get_s("Column_name");

            // Non_unique ist meist ein Integer (0 = Unique/PK, 1 = Normal)
            let is_unique = row.try_get::<i64, _>("Non_unique").unwrap_or(1) == 0;

            // Primärschlüssel ignorieren (da in ForgeColumn.is_primary_key abgedeckt)
            if index_name == "PRIMARY" {
                continue;
            }

            // Index in der Map finden oder neu anlegen
            let entry = indices_map.entry(index_name.clone()).or_insert(ForgeIndex {
                name: index_name,
                columns: Vec::new(),
                is_unique,
            });

            // Spalte hinzufügen (SHOW INDEX liefert die Spalten bereits in der richtigen Reihenfolge)
            entry.columns.push(column_name);
        }

        // Map in Vektor umwandeln
        Ok(indices_map.into_values().collect())
    }

    async fn fetch_foreign_keys(
        &self,
        table_name: &str,
    ) -> Result<Vec<ForgeForeignKey>, Box<dyn std::error::Error>> {
        // TODO
        Ok(Vec::new())
    }

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
        ret.push_str(&format!(" {}", sql_type));

        match sql_type.as_str() {
            "decimal" => {
                if let (Some(p), Some(s)) = (field.precision, field.scale) {
                    ret.push_str(&format!("({},{})", p, s));
                } else if let Some(p) = field.precision {
                    ret.push_str(&format!("({})", p));
                }
            }
            "int" | "integer" | "bigint" => {
                if field.is_unsigned {
                    ret.push_str(" unsigned");
                }
            }

            "varchar" | "char" => {
                if let Some(l) = field.length {
                    ret.push_str(&format!("({})", l));
                }
            }
            "enum" => {
                if let Some(ref vals) = field.enum_values {
                    let formatted_vals: Vec<String> =
                        vals.iter().map(|v| format!("'{}'", v)).collect();
                    ret.push_str(&format!("({})", formatted_vals.join(",")));
                }
            }
            _ => {}
        }

        // Nullable & Default NULL
        if !field.is_nullable {
            ret.push_str(" NOT NULL");
        } else {
            ret.push_str(" NULL");
            if field.default.is_none() {
                ret.push_str(" DEFAULT NULL");
            }
        }

        // Default Value
        if let Some(ref def) = field.default {
            if def.to_lowercase() == "current_timestamp" {
                ret.push_str(" DEFAULT CURRENT_TIMESTAMP");
            } else {
                ret.push_str(&format!(" DEFAULT '{}'", def));
            }
        }

        // Auto Increment
        if field.auto_increment {
            ret.push_str(" AUTO_INCREMENT");
        }

        // On Update
        if let Some(ref on_upd) = field.on_update {
            if let Some(ref def) = field.default {
                if def.to_lowercase() == "current_timestamp" {
                    ret.push_str(" ON UPDATE CURRENT_TIMESTAMP");
                } else {
                    ret.push_str(&format!(" ON UPDATE {}", on_upd));
                }
            }
        }

        ret
    }

    /// Generiert das CREATE TABLE Statement für MySQL
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
        // Nach dem Erstellen der Tabelle auch alle Nicht-PK-Indizes anlegen
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

        // 1. Spalten vergleichen, die in dst_table sind (was wir haben wollen)
        for dst_col in &dst_table.columns {
            if let Some(src_col) = src_cols.get(&dst_col.name) {
                // WENN eine ForgeColumn in beiden vorkommt -> modify_column_migration_sql()
                let sql = self.modify_column_migration(
                    &dst_table.name,
                    src_col,
                    dst_col,
                    config,
                    destructive,
                );
                if !sql.is_empty() {
                    all_statements.push(sql);
                }
            } else {
                // WENN eine ForgeColumn NICHT in src_table.columns ABER in dst_table.columns -> add_column_migration_sql()
                all_statements.push(self.add_column_migration(&dst_table.name, dst_col, config));
            }
        }

        // 2. Spalten prüfen, die in src_table sind, aber nicht in dst_table
        if destructive {
            for src_col in &src_table.columns {
                if !dst_cols.contains_key(&src_col.name) {
                    // WENN eine ForgeColumn in src_table.columns ABER NICHT in dst_table.columns UND destructiv -> drop_column_migration_sql()
                    all_statements.push(self.drop_column_migration(&dst_table.name, src_col));
                }
            }
        }

        // ---- Indizes ----
        let mut src_idx_map: HashMap<String, &ForgeIndex> = HashMap::new();
        for idx in &src_table.indices {
            src_idx_map.insert(idx.name.clone(), idx);
        }
        let mut dst_idx_map: HashMap<String, &ForgeIndex> = HashMap::new();
        for idx in &dst_table.indices {
            dst_idx_map.insert(idx.name.clone(), idx);
        }

        // a) Fehlende Indizes in Quelle -> anlegen
        for (name, dst_idx) in &dst_idx_map {
            match src_idx_map.get(name) {
                None => {
                    // Index existiert in Ziel-Schema, aber nicht in aktueller DB -> CREATE INDEX
                    let sql = self.build_mysql_create_index_sql(&dst_table.name, dst_idx);
                    all_statements.push(sql);
                }
                Some(src_idx) => {
                    // In beiden vorhanden -> Gleichheit prüfen, ggf. ersetzen
                    if !self.indices_equal(src_idx, dst_idx) {
                        // Alten durch neuen ersetzen: DROP + CREATE (Ersetzen unabhängig von destructive)
                        let drop_sql = self.build_mysql_drop_index_sql(&dst_table.name, name);
                        let create_sql =
                            self.build_mysql_create_index_sql(&dst_table.name, dst_idx);
                        all_statements.push(drop_sql);
                        all_statements.push(create_sql);
                    }
                }
            }
        }

        // b) Zusätzliche Indizes in aktueller DB -> ggf. löschen (nur wenn destructive)
        if destructive {
            for (name, src_idx) in &src_idx_map {
                if !dst_idx_map.contains_key(name) {
                    let drop_sql = self.build_mysql_drop_index_sql(&dst_table.name, &src_idx.name);
                    all_statements.push(drop_sql);
                }
            }
        }

        Ok(all_statements)
    }

    pub fn add_column_migration(
        &self,
        table_name: &str,
        dst_col: &ForgeColumn,
        config: &ForgeConfig,
    ) -> String {
        self.build_mysql_add_column_sql(table_name, dst_col, config)
    }

    pub fn drop_column_migration(&self, table_name: &str, src_col: &ForgeColumn) -> String {
        format!(
            "ALTER TABLE `{}` DROP COLUMN `{}`;",
            table_name, src_col.name
        )
    }

    pub fn modify_column_migration(
        &self,
        table_name: &str,
        src_col: &ForgeColumn,
        dst_col: &ForgeColumn,
        config: &ForgeConfig,
        _destructive: bool,
    ) -> String {
        let mut changed = src_col.data_type != dst_col.data_type
            || src_col.length != dst_col.length
            || src_col.is_nullable != dst_col.is_nullable;

        // Spezialbehandlung für FLOAT: Numerischer Vergleich der Default-Werte
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
            let col_copy = dst_col.clone();
            let sql_def = self.field_migration_sql(col_copy, config);
            return format!("ALTER TABLE `{}` MODIFY COLUMN {};", table_name, sql_def);
        }
        "".to_string()
    }

    /// Generiert ein ALTER TABLE ADD COLUMN Statement
    pub fn build_mysql_add_column_sql(
        &self,
        table_name: &str,
        col: &ForgeColumn,
        config: &ForgeConfig,
    ) -> String {
        let sql_def = self.field_migration_sql(col.clone(), config);
        format!("ALTER TABLE `{}` ADD COLUMN {};", table_name, sql_def)
    }

    /// Generiert ein CREATE INDEX Statement
    pub fn build_mysql_create_index_sql(&self, table_name: &str, index: &ForgeIndex) -> String {
        let unique = if index.is_unique { "UNIQUE " } else { "" };
        let cols = index
            .columns
            .iter()
            .map(|c| format!("`{}`", c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE {}INDEX `{}` ON `{}` ({});",
            unique, index.name, table_name, cols
        )
    }

    /// Generiert ein DROP INDEX Statement
    pub fn build_mysql_drop_index_sql(&self, table_name: &str, index_name: &str) -> String {
        format!("DROP INDEX `{}` ON `{}`;", index_name, table_name)
    }

    /// Prüft, ob zwei Indizes identisch sind (Name außen vor, da über Map-Schlüssel geprüft)
    pub fn indices_equal(&self, a: &ForgeIndex, b: &ForgeIndex) -> bool {
        if a.is_unique != b.is_unique {
            return false;
        }
        if a.columns.len() != b.columns.len() {
            return false;
        }
        for (i, col) in a.columns.iter().enumerate() {
            if b.columns.get(i) != Some(col) {
                return false;
            }
        }
        true
    }

    /// Liest eine Spalte an einem bestimmten Index sicher als String,
    /// auch wenn MySQL VARBINARY oder BLOB zurückgibt.
    pub fn get_string_at_index(&self, row: &sqlx::mysql::MySqlRow, index: usize) -> Option<String> {
        // Versuche, die Spalte als Byte-Vektor zu lesen
        let bytes: Vec<u8> = row.try_get(index).unwrap_or_default();

        if bytes.is_empty() {
            return None;
        }

        // Wandle Bytes in UTF-8 um, ignoriere ungültige Zeichen
        Some(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Behandelt MySQL-Datumswerte und fängt 0000-00-00 Fehler ab
    pub fn handle_datetime(&self, row: &MySqlRow, index: usize) -> ForgeUniversalValue {
        match row.try_get::<Option<NaiveDateTime>, _>(index) {
            Ok(Some(dt)) => ForgeUniversalValue::DateTime(dt),
            Ok(None) => ForgeUniversalValue::Null,
            Err(_) => {
                // Check auf MySQL "Zero-Date"
                let raw: Option<String> = row.try_get(index).ok();
                match raw {
                    Some(s) if s.starts_with("0000-00-00") => ForgeUniversalValue::Null,
                    _ => ForgeUniversalValue::Null,
                }
            }
        }
    }

    /// Mappt eine MySQL-Zeile inForgeUniversalValue-Struktur
    fn map_row_to_universal_values(&self, row: &MySqlRow) -> Vec<ForgeUniversalValue> {
        row.columns()
            .iter()
            .map(|col| {
                let i = col.ordinal();

                let type_name = col.type_info().name().to_uppercase();

                match type_name.as_str() {
                    "DATETIME" | "TIMESTAMP" => self.handle_datetime(row, i),

                    "DATE" => row
                        .try_get::<Option<chrono::NaiveDate>, _>(i)
                        .ok()
                        .flatten()
                        .map(ForgeUniversalValue::Date)
                        .unwrap_or(ForgeUniversalValue::Null),

                    "TIME" => {
                        row.try_get::<Option<chrono::NaiveTime>, _>(i)
                            .ok()
                            .flatten()
                            .map(ForgeUniversalValue::Time) // Falls dein Enum das hat
                            .unwrap_or(ForgeUniversalValue::Null)
                    }

                    "YEAR" => {
                        // YEAR wird als i32 behandelt
                        row.try_get::<Option<i32>, _>(i)
                            .ok()
                            .flatten()
                            .map(ForgeUniversalValue::Year)
                            .unwrap_or(ForgeUniversalValue::Null)
                    }

                    "TINYINT(1)" | "BOOLEAN" | "BOOL" => row
                        .try_get::<Option<bool>, _>(i)
                        .ok()
                        .flatten()
                        .map(ForgeUniversalValue::Boolean)
                        .unwrap_or(ForgeUniversalValue::Null),

                    "TINYINT" | "SMALLINT" | "INT" | "INTEGER" | "MEDIUMINT" | "BIGINT"
                    | "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "INT UNSIGNED"
                    | "BIGINT UNSIGNED" => {
                        let is_unsigned =
                            col.type_info().name().to_uppercase().contains("UNSIGNED");

                        if is_unsigned {
                            row.try_get::<Option<u64>, _>(i)
                                .ok()
                                .flatten()
                                .map(ForgeUniversalValue::UnsignedInteger) // Nutzt u64
                                .unwrap_or(ForgeUniversalValue::Null)
                        } else {
                            row.try_get::<Option<i64>, _>(i)
                                .ok()
                                .flatten()
                                .map(ForgeUniversalValue::Integer) // Nutzt i64
                                .unwrap_or(ForgeUniversalValue::Null)
                        }
                    }

                    "JSON" => row
                        .try_get::<Option<serde_json::Value>, _>(i)
                        .ok()
                        .flatten()
                        .map(ForgeUniversalValue::Json)
                        .unwrap_or(ForgeUniversalValue::Null),

                    "DOUBLE" | "FLOAT" | "DECIMAL" => row
                        .try_get::<Option<f64>, _>(i)
                        .ok()
                        .flatten()
                        .map(ForgeUniversalValue::Float)
                        .unwrap_or(ForgeUniversalValue::Null),
                    "BLOB" | "VARBINARY" | "BINARY" => row
                        .try_get::<Option<Vec<u8>>, _>(i)
                        .ok()
                        .flatten()
                        .map(ForgeUniversalValue::Binary)
                        .unwrap_or(ForgeUniversalValue::Null),

                    //  Alles andere (String-Fallback für VARCHAR, TEXT, ENUM, etc.)
                    _ => {
                        if let Ok(Some(s)) = row.try_get::<Option<String>, _>(i) {
                            ForgeUniversalValue::Text(s)
                        } else if let Some(s) = self.get_string_at_index(row, i) {
                            // Fallback über deine Methode, falls try_get fehlschlägt
                            ForgeUniversalValue::Text(s)
                        } else if let Ok(Some(bin)) = row.try_get::<Option<Vec<u8>>, _>(i) {
                            // Letzter Versuch: Falls es kein valider String war, als Binary retten
                            ForgeUniversalValue::Binary(bin)
                        } else {
                            ForgeUniversalValue::Null
                        }
                    }
                }
            })
            .collect()
    }

    /// Konvertiert eine MySqlRow-Spalte in ein serde_json::Value
    pub fn sqlx_value_to_json(&self, row: &MySqlRow, index: usize) -> serde_json::Value {
        use sqlx::TypeInfo;

        let column = &row.columns()[index];
        let type_name = column.type_info().name().to_uppercase();

        // NULL-Check
        // try_get::<Option<String>, _> ist ein einfacher Weg um auf NULL zu prüfen
        // aber wir können auch direkt schauen ob der Wert vorhanden ist.
        // SQLx gibt bei NULL meistens einen Error zurück wenn man nicht Option nutzt.

        match type_name.as_str() {
            "TINYINT" | "SMALLINT" | "INT" | "INTEGER" | "MEDIUMINT" | "BIGINT" => {
                if let Ok(val) = row.try_get::<Option<i64>, _>(index) {
                    return val
                        .map(|v| serde_json::Value::Number(v.into()))
                        .unwrap_or(serde_json::Value::Null);
                }
                // Unsigned Handling
                if let Ok(val) = row.try_get::<Option<u64>, _>(index) {
                    return val
                        .map(|v| serde_json::Value::Number(v.into()))
                        .unwrap_or(serde_json::Value::Null);
                }
            }
            "DECIMAL" | "FLOAT" | "DOUBLE" => {
                if let Ok(val) = row.try_get::<Option<f64>, _>(index) {
                    return val
                        .map(|v| {
                            serde_json::Number::from_f64(v)
                                .map(serde_json::Value::Number)
                                .unwrap_or(serde_json::Value::Null)
                        })
                        .unwrap_or(serde_json::Value::Null);
                }
            }
            "BIT" | "BOOLEAN" | "BOOL" => {
                if let Ok(val) = row.try_get::<Option<bool>, _>(index) {
                    return val
                        .map(serde_json::Value::Bool)
                        .unwrap_or(serde_json::Value::Null);
                }
            }
            "DATE" | "DATETIME" | "TIMESTAMP" | "TIME" | "YEAR" => {
                // Diese Typen als String extrahieren (sqlx konvertiert sie oft automatisch)
                if let Ok(val) = row.try_get::<Option<String>, _>(index) {
                    return val
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null);
                }
                // Fallback: Wenn String-Konvertierung nicht direkt geht, über Bytes
                if let Some(s) = self.get_string_at_index(row, index) {
                    return serde_json::Value::String(s);
                }
            }
            "JSON" => {
                if let Ok(val) = row.try_get::<Option<serde_json::Value>, _>(index) {
                    return val.unwrap_or(serde_json::Value::Null);
                }
            }
            _ => {
                // Alles andere als String (VARCHAR, TEXT, BLOB, ENUM, ...)
                if let Ok(val) = row.try_get::<Option<String>, _>(index) {
                    return val
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null);
                }
                // Fallback für binäre Typen
                if let Some(s) = self.get_string_at_index(row, index) {
                    return serde_json::Value::String(s);
                }
            }
        }

        serde_json::Value::Null
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
        // 0. Datenbankname ermitteln
        let db_name: String = sqlx::query_scalar("SELECT DATABASE()")
            .fetch_one(&self.pool)
            .await?;

        // Alle Tabellen-Hüllen mit Kommentaren holen
        let mut tables = self.fetch_tables().await?;

        if tables.is_empty() {
            println!("Keine Tabellen in der Datenbank gefunden.");
        }

        // Details für jede Tabelle nachladen
        for table in &mut tables {
            // Spalten laden und Mapping-Config anwenden
            table.columns = self.fetch_columns(&table.name, config).await?;

            // Indizes laden
            table.indices = self.fetch_indices(&table.name).await?;

            // Foreign Keys laden
            table.foreign_keys = self.fetch_foreign_keys(&table.name).await?;
        }

        // In das ForgeSchema einbetten
        Ok(ForgeSchema {
            metadata: ForgeMetadata {
                source_system: "mysql".to_string(),
                source_database_name: db_name,
                created_at: chrono::Local::now().to_rfc3339(),
                forge_version: env!("CARGO_PKG_VERSION").to_string(),
                config_file: "".to_string(),
            },
            tables,
        })
    }

    async fn create_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        unimplemented!("obsolete")
    }

    async fn diff_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
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

        // 1. Tabellen vergleichen, die in source_schema sind
        for table in &source_schema.tables {
            if let Some(target_table) = target_tables.get(&table.name) {
                // WENN tabelle in source_schema und target_schema -> alter_table_migration_sql()
                let stmts =
                    self.alter_table_migration_sql(target_table, table, config, destructive)?;
                all_statements.extend(stmts);
            } else {
                // WENN tabelle in target_schema, aber nicht in source_schema -> create_table_migration_sql()
                // (Anmerkung: In der Logik des Codes ist source_schema das Ziel-Schema,
                //  daher ist eine Tabelle, die in source aber nicht in target ist, neu zu erstellen)
                let stmts = self.create_table_migration_sql(table, config)?;
                all_statements.extend(stmts);
            }
        }

        // 2. Tabellen prüfen, die in target_schema sind, aber nicht in source_schema
        // WENN tabelle in source_schema, aber nicht in target_schema UND destructive -> delete_table_migration_sql()
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

            println!("{} SQL-Statements erfolgreich ausgeführt.", success_count);
        }

        Ok(all_statements)
    }

    async fn stream_table_dataa(&self, table_name: &str) -> Result<Pin<Box<dyn Stream<Item=Result<Value, sqlx::Error>> + Send>>, Box<dyn Error>> {
        todo!()
    }

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, sqlx::Error>> + Send + '_>>,
        Box<dyn std::error::Error>,
    > {
        let query_string = format!("SELECT * FROM `{}`", table_name);

        let stream = async_stream::try_stream! {
                let mut rows = sqlx::query(&query_string).fetch(&self.pool);

                while let Some(row) = rows.next().await {
                    let row: sqlx::mysql::MySqlRow = row?;
                    let values = self.map_row_to_universal_values(&row);

                    let mut row_map = IndexMap::new();
                    for (col, val) in row.columns().iter().zip(values) {
                        row_map.insert(col.name().to_string(), val);
                    }

                    yield row_map;
                }
            };

        Ok(Box::pin(stream))
    }

    async fn insert_chunk(&self, table_name: &str, dry_run: bool, chunk: Vec<IndexMap<String, ForgeUniversalValue>>) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    async fn insert_chunka(
        &self,
        table_name: &str,
        dry_run: bool,
        chunk: Vec<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if chunk.is_empty() {
            return Ok(());
        }

        // 1. Spaltennamen aus dem ersten Datensatz extrahieren
        let first_row = chunk
            .first()
            .ok_or("Chunk is empty")?
            .as_object()
            .ok_or("Invalid row format")?;
        let columns: Vec<String> = first_row.keys().cloned().collect();
        let column_names = columns
            .iter()
            .map(|c| format!("`{}`", c))
            .collect::<Vec<_>>()
            .join(", ");

        // 2. SQL-Statement vorbereiten: INSERT INTO table (col1, col2) VALUES (?, ?), (?, ?) ...
        let mut sql = format!("INSERT INTO `{}` ({}) VALUES ", table_name, column_names);

        let mut placeholders = Vec::new();
        for _ in 0..chunk.len() {
            let row_placeholders = vec!["?"; columns.len()].join(", ");
            placeholders.push(format!("({})", row_placeholders));
        }
        sql.push_str(&placeholders.join(", "));

        if dry_run {
            println!("SQL = {}", sql);
        } else {
            // 3. Query-Objekt erstellen und Werte binden
            let mut query = sqlx::query(&sql);

            for row in &chunk {
                let obj = row.as_object().ok_or("Invalid row format")?;
                for col in &columns {
                    let val = obj.get(col).cloned().unwrap_or(serde_json::Value::Null);

                    // Wir binden die Werte basierend auf ihrem JSON-Typ
                    query = match val {
                        serde_json::Value::Null => query.bind(None::<String>),
                        serde_json::Value::Bool(b) => query.bind(b),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                query.bind(i)
                            } else if let Some(u) = n.as_u64() {
                                query.bind(u)
                            } else {
                                query.bind(n.as_f64())
                            }
                        }
                        serde_json::Value::String(s) => query.bind(s),
                        _ => query.bind(val.to_string()), // Fallback für Arrays/Objekte als String
                    };
                }
            }
            // 4. In MySQL ausführen
            if let Err(e) = query.execute(&self.pool).await {
                eprintln!("Error executing INSERT on table `{}`: {}", table_name, e);
                eprintln!("Columns: {}", columns.join(", "));
                for (i, row) in chunk.iter().enumerate() {
                    eprintln!("Row {}: {}", i, row);
                }
                return Err(e.into());
            }
        }

        Ok(())
    }
}
