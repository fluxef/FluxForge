use crate::core::{
    ForgeConfig, ForgeForeignKey, ForgeIndex, ForgeSchema, ForgeTable, SchemaMetadata,
};
use crate::{DatabaseDriver, ForgeColumn};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use sqlx::{Column, MySqlPool, Row};
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;

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
        for row in rows {
            // Hilfsfunktion zum sicheren Lesen von (VAR)BINARY Metadaten
            let get_s = |col: &str| -> String {
                row.try_get::<Vec<u8>, _>(col)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default()
            };

            let col_name = get_s("Field");
            let mysql_column_type = get_s("Type"); // z.B. "int(11) unsigned" oder "enum('a','b')"

            // Extrahiere den reinen Datentyp (alles vor der ersten Klammer oder Leerstelle)
            let mysql_data_type = mysql_column_type
                .split(|c| c == '(' || c == ' ')
                .next()
                .unwrap_or(&mysql_column_type)
                .to_string();

            // --- Mapping Logik ---
            let mut target_type = config
                .types
                .get(&mysql_column_type.to_lowercase())
                .or_else(|| config.types.get(&mysql_data_type.to_lowercase()))
                .cloned()
                .unwrap_or(mysql_data_type.clone());

            // Unsigned Regel anwenden
            if config.rules.unsigned_int_to_bigint && mysql_column_type.contains("unsigned") {
                if mysql_data_type.contains("int") {
                    target_type = "bigint".to_string();
                }
            }

            // Enum-Werte extrahieren, falls vorhanden
            let enum_values = if mysql_column_type.starts_with("enum") {
                Some(self.parse_mysql_enum_values(&mysql_column_type))
            } else {
                None
            };

            columns.push(ForgeColumn {
                name: col_name,
                data_type: target_type.to_uppercase(),
                // Längen/Precision/Scale müssten bei SHOW FIELDS komplexer aus dem String geparst werden
                // Falls du diese exakt brauchst, ist information_schema.columns überlegen.
                length: None,
                precision: None,
                scale: None,
                is_nullable: get_s("Null") == "YES",
                is_primary_key: get_s("Key") == "PRI",
                auto_increment: get_s("Extra").contains("auto_increment"),
                default: row
                    .try_get::<Option<Vec<u8>>, _>("Default")
                    .ok()
                    .flatten()
                    .map(|b| String::from_utf8_lossy(&b).into_owned()),
                comment: Some(get_s("Comment")),
                enum_values,
            });
        }
        Ok(columns)
    }

    /// Extrahiert 'val1','val2' aus enum('val1','val2')
    fn parse_mysql_enum_values(&self, col_type: &str) -> Vec<String> {
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

    /// Generiert das CREATE TABLE Statement für MySQL
    fn build_mysql_create_table_sql(&self, table: &ForgeTable) -> String {
        let mut col_defs = Vec::new();
        let mut pks = Vec::new();

        for col in &table.columns {
            let mut def = format!("  `{}` {}", col.name, col.data_type);

            if let Some(len) = col.length {
                def.push_str(&format!("({})", len));
            }
            if !col.is_nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(default) = &col.default {
                def.push_str(&format!(" DEFAULT '{}'", default));
            }
            if col.auto_increment {
                def.push_str(" AUTO_INCREMENT");
            }

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

    fn create_table_migration_sql(
        &self,
        dst_table: &ForgeTable,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        todo!()
    }

    fn delete_table_migration_sql(
        &self,
        dst_table: &ForgeTable,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        todo!()
    }
    fn alter_table_migration_sql(
        &self,
        src_table: &ForgeTable,
        dst_table: &ForgeTable,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        todo!()
    }

    /// Generiert ein ALTER TABLE ADD COLUMN Statement
    fn build_mysql_add_column_sql(&self, table_name: &str, col: &ForgeColumn) -> String {
        let mut def = format!(
            "ALTER TABLE `{}` ADD COLUMN `{}` {}",
            table_name, col.name, col.data_type
        );
        if let Some(len) = col.length {
            def.push_str(&format!("({})", len));
        }
        if !col.is_nullable {
            def.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default {
            def.push_str(&format!(" DEFAULT '{}'", default));
        }
        format!("{};", def)
    }

    /// Generiert ein CREATE INDEX Statement
    fn build_mysql_create_index_sql(&self, table_name: &str, index: &ForgeIndex) -> String {
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

    /// Liest eine Spalte an einem bestimmten Index sicher als String,
    /// auch wenn MySQL VARBINARY oder BLOB zurückgibt.
    fn get_string_at_index(&self, row: &sqlx::mysql::MySqlRow, index: usize) -> Option<String> {
        // Versuche, die Spalte als Byte-Vektor zu lesen
        let bytes: Vec<u8> = row.try_get(index).unwrap_or_default();

        if bytes.is_empty() {
            return None;
        }

        // Wandle Bytes in UTF-8 um, ignoriere ungültige Zeichen
        Some(String::from_utf8_lossy(&bytes).into_owned())
    }
}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    async fn has_data(&self, schema: &ForgeSchema) -> Result<bool, Box<dyn Error>> {
        todo!()
    }
    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
        // 1. Alle Tabellen-Hüllen mit Kommentaren holen
        let mut tables = self.fetch_tables().await?;

        if tables.is_empty() {
            println!("⚠️ Keine Tabellen in der Datenbank gefunden.");
        }

        // 2. Details für jede Tabelle nachladen
        for table in &mut tables {
            // Spalten laden und Mapping-Config anwenden
            table.columns = self.fetch_columns(&table.name, config).await?;

            // Indizes laden
            table.indices = self.fetch_indices(&table.name).await?;

            // Foreign Keys laden
            table.foreign_keys = self.fetch_foreign_keys(&table.name).await?;
        }

        // 3. In das ForgeSchema einbetten
        Ok(ForgeSchema {
            metadata: SchemaMetadata {
                source_system: "mysql".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                forge_version: env!("CARGO_PKG_VERSION").to_string(),
            },
            tables,
        })
    }

    async fn apply_schema(
        &self,
        schema: &ForgeSchema,
        execute: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut all_statements = Vec::new();

        for table in &schema.tables {
            // 1. Prüfen, ob die Tabelle bereits existiert
            let table_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)"
            )
                .bind(&table.name)
                .fetch_one(&self.pool)
                .await?;

            if !table_exists {
                // FALL: Tabelle komplett neu anlegen
                let sql = self.build_mysql_create_table_sql(table);
                all_statements.push(sql.clone());
                if execute {
                    sqlx::query(&sql).execute(&self.pool).await?;
                }
            } else {
                // FALL: Tabelle existiert -> Fehlende Spalten ergänzen
                let current_cols: Vec<String> = sqlx::query_scalar(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ?"
                )
                    .bind(&table.name)
                    .fetch_all(&self.pool)
                    .await?;

                for col in &table.columns {
                    if !current_cols.contains(&col.name) {
                        let sql = self.build_mysql_add_column_sql(&table.name, col);
                        all_statements.push(sql.clone());
                        if execute {
                            sqlx::query(&sql).execute(&self.pool).await?;
                        }
                    }
                }
            }

            // 2. Indizes hinzufügen (falls nicht vorhanden)
            // MySQL hat kein "CREATE INDEX IF NOT EXISTS" (vor Version 8.0.30),
            // daher prüfen wir manuell über information_schema
            for index in &table.indices {
                let index_exists: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?)"
                )
                    .bind(&table.name)
                    .bind(&index.name)
                    .fetch_one(&self.pool)
                    .await?;

                if !index_exists {
                    let sql = self.build_mysql_create_index_sql(&table.name, index);
                    all_statements.push(sql.clone());
                    if execute {
                        sqlx::query(&sql).execute(&self.pool).await?;
                    }
                }
            }
        }

        Ok(all_statements)
    }

    async fn diff_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        execute: bool,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let target_schema = self.fetch_schema(config).await?;
        let mut all_statements = Vec::new();

        for table in &source_schema.tables {

            // TODO
            // WENN tabelle in source_schema, aber nicht in target_schema UND destructive -> delete_table_migration_sql()
            // WENN tabelle in target_schema, aber nicht in source_schema -> create_table_migration_sql()
            // WENN tabelle in source_schema und target_schema -> alter_table_migration_sql()
        }

        Ok(all_statements)
    }

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<serde_json::Value, sqlx::Error>> + Send>>,
        Box<dyn std::error::Error>,
    > {
        // Wir nutzen Backticks für MySQL-Tabellennamen (Reserved Words Schutz)
        let query = format!("SELECT * FROM `{}`", table_name);

        let stream = sqlx::query(&query)
            .fetch(&self.pool)
            .map(|row_result| {
                row_result.map(|row: sqlx::mysql::MySqlRow| {
                    let mut map = serde_json::Map::new();
                    for col in row.columns() {
                        let name = col.name();
                        let val: serde_json::Value =
                            row.try_get(name).unwrap_or(serde_json::Value::Null);
                        map.insert(name.to_string(), val);
                    }
                    serde_json::Value::Object(map)
                })
            })
            .collect::<Vec<_>>()
            .await;

        Ok(Box::pin(futures::stream::iter(stream)))
    }

    async fn insert_chunk(
        &self,
        table_name: &str,
        chunk: Vec<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if chunk.is_empty() {
            return Ok(());
        }

        // 1. Spaltennamen aus dem ersten Datensatz extrahieren
        let first_row = chunk
            .first()
            .unwrap()
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

        // 3. Query-Objekt erstellen und Werte binden
        let mut query = sqlx::query(&sql);

        for row in chunk {
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
        query.execute(&self.pool).await?;

        Ok(())
    }
}
