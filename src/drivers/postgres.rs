use std::error::Error;
use crate::core::{ForgeConfig, ForgeForeignKey, ForgeIndex, ForgeSchema, ForgeUniversalValue};
use crate::{DatabaseDriver, ForgeColumn, ForgeTable};
use async_trait::async_trait;
use sqlx::{PgPool, Row, Column};
use std::pin::Pin;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;

pub struct PostgresDriver {
    pub pool: PgPool,
}

impl PostgresDriver {
    /// Erzeugt das CREATE TABLE Statement inkl. Columns, PKs und Defaults

    fn build_create_table_sql(&self, table: &ForgeTable) -> String {
        let mut column_definitions = Vec::new();
        let mut primary_keys = Vec::new();

        for col in &table.columns {
            let mut def = format!("  \"{}\" {}", col.name, col.data_type);

            // Default Werte behandeln
            if let Some(default_val) = &col.default {
                // MySQL nutzt oft 'CURRENT_TIMESTAMP' - Postgres versteht das auch,
                // aber andere Defaults müssen evtl. in Quotes
                if default_val.to_uppercase() == "CURRENT_TIMESTAMP" {
                    def.push_str(" DEFAULT CURRENT_TIMESTAMP");
                } else {
                    def.push_str(&format!(" DEFAULT '{}'", default_val));
                }
            }

            // Nullability
            if !col.is_nullable {
                def.push_str(" NOT NULL");
            }

            column_definitions.push(def);

            if col.is_primary_key {
                primary_keys.push(format!("\"{}\"", col.name));
            }
        }

        // Primary Key Constraint am Ende der Spaltenliste hinzufügen
        if !primary_keys.is_empty() {
            column_definitions.push(format!("  PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" (\n{}\n);",
            table.name,
            column_definitions.join(",\n")
        )
    }

    /// Erzeugt den Index-String
    fn build_create_index_sql(&self, table_name: &str, index: &ForgeIndex) -> String {
        let unique = if index.is_unique { "UNIQUE " } else { "" };
        let cols = index.columns.iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "CREATE {}INDEX IF NOT EXISTS \"{}\" ON \"{}\" ({});",
            unique, index.name, table_name, cols
        )
    }

    /// Erzeugt das Foreign Key Alter Table Statement
    fn build_create_fk_sql(&self, table_name: &str, fk: &ForgeForeignKey) -> String {
        let mut sql = format!(
            "ALTER TABLE \"{}\" ADD CONSTRAINT \"{}\" FOREIGN KEY (\"{}\") REFERENCES \"{}\" (\"{}\")",
            table_name, fk.name, fk.column, fk.ref_table, fk.ref_column
        );

        if let Some(on_delete) = &fk.on_delete {
            sql.push_str(&format!(" ON DELETE {}", on_delete));
        }

        sql.push(';');
        sql
    }

    /// Generiert CREATE TYPE Statements für Enums
    /// Generiert CREATE TYPE Statements für MySQL-Enums in Postgres.
    /// Nutzt einen PL/pgSQL Block, um Fehler zu vermeiden, falls der Typ schon existiert.
    fn build_enum_types_sql(&self, table: &ForgeTable) -> Vec<String> {
        let mut statements = Vec::new();

        for col in &table.columns {
            // Prüfen, ob enum_values vorhanden und nicht leer sind
            if let Some(values) = &col.enum_values {
                if !values.is_empty() {
                    // Eindeutiger Name für den Enum-Typ in Postgres
                    // Format: t_tabellenname_spaltenname
                    let type_name = format!("t_{}_{}", table.name, col.name).to_lowercase();

                    // Die Werte in Single-Quotes fassen und mit Komma trennen
                    let vals = values.iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''"))) // Einfache Quotes escapen
                        .collect::<Vec<String>>()
                        .join(", ");

                    // PL/pgSQL Block: Erstellt den Typ nur, wenn er noch nicht existiert
                    let sql = format!(
                        "DO $$ \
                        BEGIN \
                            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN \
                                CREATE TYPE \"{type_name}\" AS ENUM ({vals}); \
                            END IF; \
                        END $$;",
                        type_name = type_name,
                        vals = vals
                    );

                    statements.push(sql);
                }
            }
        }
        statements
    }

    /// Modifizierte Spalten-Definition für Enums
    fn get_pg_type(&self, table_name: &str, col: &ForgeColumn) -> String {
        if col.enum_values.is_some() {
            // Verweis auf den zuvor erstellten Typ
            format!("\"t_{}_{}\"", table_name, col.name)
        } else {
            col.data_type.clone()
        }
    }

}


#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn fetch_schema(&self, config: &ForgeConfig) -> Result<ForgeSchema, Box<dyn std::error::Error>>{
        todo!("Postgres kann auch Quelle sein, Fokus liegt aber auf Target")
    }

    async fn create_schema(
        &self,
        schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut all_statements = Vec::new();

        // PHASE 0: Custom Types (Enums)
        for table in &schema.tables {
            let enum_sqls = self.build_enum_types_sql(table);
            for sql in enum_sqls {
                all_statements.push(sql.clone());
                if !dry_run { sqlx::query(&sql).execute(&self.pool).await?; }
            }
        }


        // Schritt 1: Alle Tabellen erstellen (ohne FKs)
        for table in &schema.tables {
            let sql = self.build_create_table_sql(table);
            all_statements.push(sql.clone());
            if !dry_run {
                sqlx::query(&sql).execute(&self.pool).await?;
            }
        }

        // Schritt 2: Alle Indizes erstellen
        for table in &schema.tables {
            for index in &table.indices {
                let sql = self.build_create_index_sql(&table.name, index);
                all_statements.push(sql.clone());
                if !dry_run {
                    sqlx::query(&sql).execute(&self.pool).await?;
                }
            }
        }

        // Schritt 3: Alle Foreign Keys per ALTER TABLE hinzufügen
        for table in &schema.tables {
            for fk in &table.foreign_keys {
                let sql = self.build_create_fk_sql(&table.name, fk);
                all_statements.push(sql.clone());
                if !dry_run {
                    // Falls der FK schon existiert, könnte Postgres einen Fehler werfen.
                    // Man kann 'IF NOT EXISTS' bei Constraints leider nicht nutzen,
                    // daher ist ein try-catch oder Check sinnvoll.
                    let _ = sqlx::query(&sql).execute(&self.pool).await;
                }
            }
        }

        Ok(all_statements)
    }

    async fn insert_chunk_old(
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

    async fn stream_table_data_old(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<serde_json::Value, sqlx::Error>> + Send>>,
        Box<dyn std::error::Error>,
    > {
        // Postgres nutzt Double-Quotes für Case-Sensitivity Schutz
        let query = format!("SELECT * FROM \"{}\"", table_name);

        let stream = sqlx::query(&query)
            .fetch(&self.pool)
            .map(|row_result| {
                row_result.map(|row: sqlx::postgres::PgRow| {
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
    async fn db_is_empty(&self) -> Result<bool, Box<dyn Error>> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count == 0)
    }

    async fn diff_schema(&self, schema: &ForgeSchema, config: &ForgeConfig, execute: bool, destructive: bool) -> Result<Vec<String>, Box<dyn Error>> {
        todo!()
    }

    async fn stream_table_data(
        &self,
        table_name: &str,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<Item = Result<IndexMap<String, ForgeUniversalValue>, sqlx::Error>>
                    + Send
                    + '_,
            >,
        >,
        Box<dyn Error>,
    > {
        todo!()
    }

    async fn insert_chunk(&self, table_name: &str, dry_run: bool, halt_on_error: bool, chunk: Vec<IndexMap<String, ForgeUniversalValue>>) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
