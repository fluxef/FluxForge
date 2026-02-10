use crate::core::ForgeSchema;
use crate::DatabaseDriver;
use async_trait::async_trait;
use sqlx::{Executor, PgPool};
use std::io::Write;
use std::pin::Pin;
use futures::Stream;

pub struct PostgresDriver {
    pub pool: PgPool,
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn fetch_schema(&self) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
        unimplemented!("Postgres kann auch Quelle sein, Fokus liegt aber auf Target")
    }

    async fn apply_schema(
        &self,
        schema: &ForgeSchema,
        dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut sql_statements = Vec::new();

        for table in &schema.tables {
            // Sehr vereinfachtes Beispiel für DDL-Generierung
            let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (", table.name);
            // ... Spalten-Logik hier ...
            sql.push_str(");");

            sql_statements.push(sql.clone());

            if !dry_run {
                sqlx::query(&sql).execute(&self.pool).await?;
            }
        }

        Ok(sql_statements)
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
        let column_list = columns.join(", ");

        // 2. Postgres COPY Kommando vorbereiten
        let copy_query = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER false)",
            table_name, column_list
        );

        // 3. Verbindung aus dem Pool holen
        let mut conn = self.pool.acquire().await?;

        // 4. COPY-Stream öffnen
        let mut writer = conn.copy_in_raw(&copy_query).await?;

        // 5. JSON-Rows in CSV-Buffer schreiben
        let mut buffer = Vec::new();
        {
            let mut csv_writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(&mut buffer);

            for row in chunk {
                let obj = row.as_object().ok_or("Invalid row format")?;
                // Werte in der richtigen Reihenfolge der Spalten extrahieren
                let record: Vec<String> = columns
                    .iter()
                    .map(|col| {
                        match obj.get(col) {
                            Some(serde_json::Value::Null) => "".to_string(), // Postgres CSV NULL
                            Some(v) => v.as_str().unwrap_or(&v.to_string()).to_string(),
                            None => "".to_string(),
                        }
                    })
                    .collect();
                csv_writer.write_record(&record)?;
            }
            csv_writer.flush()?;
        }

        // 6. Buffer in den Postgres-Stream schieben und abschließen
        writer.send(buffer).await?;
        writer.finish().await?;

        Ok(())
    }

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<serde_json::Value, sqlx::Error>> + Send>>,
        Box<dyn std::error::Error>,
    > {
        // Postgres nutzt Double-Quotes für Case-Sensitivity Schutz
        let query = format!("SELECT * FROM \"{}\"", table_name);

        let stream = sqlx::query(&query).fetch(&self.pool).map(|row_result| {
            row_result.map(|row| {
                let mut map = serde_json::Map::new();
                for col in row.columns() {
                    let name = col.name();
                    let val: serde_json::Value =
                        row.try_get(name).unwrap_or(serde_json::Value::Null);
                    map.insert(name.to_string(), val);
                }
                serde_json::Value::Object(map)
            })
        });

        Ok(Box::pin(stream))
    }
}
