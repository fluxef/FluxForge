use std::collections::HashMap;
use crate::core::{ForgeConfig, ForgeForeignKey, ForgeIndex, ForgeSchema, ForgeTable, SchemaMetadata};
use crate::{DatabaseDriver, ForgeColumn};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use sqlx::{Column, MySqlPool, Row};
use std::pin::Pin;

pub struct MySqlDriver {
    pub pool: MySqlPool,
}

impl MySqlDriver {
    // Diese Funktionen sind nur innerhalb dieses Moduls sichtbar
    // und gehören NICHT zum öffentlichen Trait.


    async fn fetch_indices(&self, table_name: &str) -> Result<Vec<ForgeIndex>, Box<dyn std::error::Error>> {
        let rows = sqlx::query(
            "SELECT index_name, column_name, non_unique
             FROM information_schema.statistics
             WHERE table_schema = DATABASE() AND table_name = ?
             ORDER BY index_name, seq_in_index"
        )
            .bind(table_name)
            .fetch_all(&self.pool)
            .await?;

        let mut indices_map: HashMap<String, ForgeIndex> = HashMap::new();

        for row in rows {
            let index_name: String = row.get("index_name");
            let column_name: String = row.get("column_name");
            let is_unique = row.get::<i64, _>("non_unique") == 0;

            // Primärschlüssel ignorieren wir hier oft, da sie bereits in ForgeColumn markiert sind
            if index_name == "PRIMARY" { continue; }

            let entry = indices_map.entry(index_name.clone()).or_insert(ForgeIndex {
                name: index_name,
                columns: Vec::new(),
                is_unique,
            });
            entry.columns.push(column_name);
        }

        Ok(indices_map.into_values().collect())
    }

    async fn fetch_foreign_keys(&self, table_name: &str) -> Result<Vec<ForgeForeignKey>, Box<dyn std::error::Error>> {
        let rows = sqlx::query(
            "SELECT
                k.constraint_name, k.column_name, k.referenced_table_name,
                k.referenced_column_name, r.delete_rule, r.update_rule
             FROM information_schema.key_column_usage k
             JOIN information_schema.referential_constraints r
               ON k.constraint_name = r.constraint_name
               AND k.table_schema = r.constraint_schema
             WHERE k.table_schema = DATABASE()
               AND k.table_name = ?
               AND k.referenced_table_name IS NOT NULL"
        )
            .bind(table_name)
            .fetch_all(&self.pool)
            .await?;

        let mut fks = Vec::new();
        for row in rows {
            fks.push(ForgeForeignKey {
                name: row.get("constraint_name"),
                column: row.get("column_name"),
                ref_table: row.get("referenced_table_name"),
                ref_column: row.get("referenced_column_name"),
                on_delete: Some(row.get("delete_rule")),
                on_update: Some(row.get("update_rule")),
            });
        }

        Ok(fks)
    }




}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    async fn fetch_schema(
        &self,
        config: &ForgeConfig,
    ) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
        // 1. Alle Tabellen der aktuellen Datenbank abrufen
        let table_rows = sqlx::query(
            "SELECT table_name, table_comment
             FROM information_schema.tables
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut tables = Vec::new();

        for t_row in table_rows {
            let table_name: String = t_row.get("table_name");
            let table_comment: Option<String> = t_row.get("table_comment");

            // 2. Spalten für diese Tabelle abrufen
            let column_rows = sqlx::query(
                "SELECT column_name, data_type, column_type, is_nullable,
                        column_default, extra, character_maximum_length,
                        numeric_precision, numeric_scale, column_key, column_comment
                 FROM information_schema.columns
                 WHERE table_schema = DATABASE() AND table_name = ?",
            )
            .bind(&table_name)
            .fetch_all(&self.pool)
            .await?;

            let mut columns = Vec::new();
            for c_row in column_rows {
                let col_name: String = c_row.get("column_name");
                let mysql_data_type: String = c_row.get("data_type"); // z.B. "int"
                let mysql_column_type: String = c_row.get("column_type"); // z.B. "int(11) unsigned" oder "tinyint(1)"

                // --- MAPPING LOGIK ---
                // Prüfe zuerst, ob der exakte column_type (z.B. tinyint(1)) gemappt werden soll
                let mut target_type = config
                    .types
                    .get(&mysql_column_type.to_lowercase())
                    .or_else(|| config.types.get(&mysql_data_type.to_lowercase()))
                    .cloned()
                    .unwrap_or(mysql_data_type.clone());

                // Regel für Unsigned Integers anwenden
                if config.rules.unsigned_int_to_bigint && mysql_column_type.contains("unsigned") {
                    if mysql_data_type.contains("int") {
                        target_type = "bigint".to_string();
                    }
                }

                columns.push(ForgeColumn {
                    name: col_name,
                    data_type: target_type.to_uppercase(),
                    length: c_row
                        .get::<Option<i64>, _>("character_maximum_length")
                        .map(|l| l as u32),
                    precision: c_row
                        .get::<Option<i64>, _>("numeric_precision")
                        .map(|p| p as u32),
                    scale: c_row
                        .get::<Option<i64>, _>("numeric_scale")
                        .map(|s| s as u32),
                    is_nullable: c_row.get::<String, _>("is_nullable") == "YES",
                    is_primary_key: c_row.get::<String, _>("column_key") == "PRI",
                    auto_increment: c_row.get::<String, _>("extra").contains("auto_increment"),
                    default: c_row.get("column_default"),
                    comment: c_row.get("column_comment"),
                });
            }

            // 3. Foreign Keys und Indizes abrufen (Analog zu Spalten, hier verkürzt)
            let indices = self.fetch_indices(&table_name).await?;
            let foreign_keys = self.fetch_foreign_keys(&table_name).await?;

            tables.push(ForgeTable {
                name: table_name,
                columns,
                indices,
                foreign_keys,
                comment: table_comment,
            });
        }

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
        _schema: &ForgeSchema,
        _dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        unimplemented!("MySQL ist primär als Quelle gedacht")
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
