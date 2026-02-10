use crate::core::{ForgeSchema, ForgeTable, SchemaMetadata};
use crate::DatabaseDriver;
use async_trait::async_trait;
use futures::Stream;
use sqlx::{Executor, MySqlPool, Row};
use std::pin::Pin;

pub struct MySqlDriver {
    pub pool: MySqlPool,
}

#[async_trait]
impl DatabaseDriver for MySqlDriver {
    async fn fetch_schema(&self) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
        // Beispiel: Nur Tabellen-Namen abrufen
        let rows = sqlx::query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut tables = Vec::new();
        for row in rows {
            let table_name: String = row.get(0);
            // Hier würde die Logik folgen, um Spalten (ForgeColumn) für jede Tabelle zu laden
            tables.push(ForgeTable {
                name: table_name,
                columns: vec![], // Details hier laden...
                indices: vec![],
                foreign_keys: vec![],
                comment: None,
            });
        }

        Ok(ForgeSchema {
            metadata: SchemaMetadata {
                source_system: "mysql".to_string(),
                created_at: "now".to_string(),
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

        let stream = sqlx::query(&query).fetch(&self.pool).map(|row_result| {
            row_result.map(|row| {
                let mut map = serde_json::Map::new();
                for col in row.columns() {
                    let name = col.name();
                    // MySQL Typen zu JSON konvertieren
                    // sqlx bietet hierfür oft automatische Konvertierung zu Value
                    let val: serde_json::Value =
                        row.try_get(name).unwrap_or(serde_json::Value::Null);
                    map.insert(name.to_string(), val);
                }
                serde_json::Value::Object(map)
            })
        });

        Ok(Box::pin(stream))
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
        let columns: Vec<&String> = first_row.keys().collect();
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
                let val = obj.get(*col).unwrap_or(&serde_json::Value::Null);

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
