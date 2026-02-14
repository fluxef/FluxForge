use crate::core::{
    ForgeConfig, ForgeError, ForgeForeignKey, ForgeIndex, ForgeMetadata, ForgeSchema, ForgeTable,
    ForgeUniversalValue,
};
use crate::ops::log_error_to_file;
use crate::{DatabaseDriver, ForgeColumn};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use sqlx::postgres::PgRow;
use sqlx::{Column, PgPool, Row, TypeInfo, ValueRef};
use std::collections::HashMap;
use std::error::Error;
use std::pin::Pin;

pub struct PostgresDriver {
    pub pool: Option<PgPool>,
}

impl PostgresDriver {
    pub fn bind_universal<'q>(
        &self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        val: &'q ForgeUniversalValue,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        match val {
            ForgeUniversalValue::Integer(i) => query.bind(i),
            ForgeUniversalValue::UnsignedInteger(u) => query.bind(*u as i64), // Postgres lacks unsigned
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
            ForgeUniversalValue::Uuid(u) => query.bind(u),
            ForgeUniversalValue::Inet(i) => query.bind(i),
            ForgeUniversalValue::Null => query.bind(None::<String>),
            ForgeUniversalValue::ZeroDateTime => query.bind(None::<String>), // Postgres doesn't support 0000-00-00
        }
    }

    pub async fn fetch_tables(&self) -> Result<Vec<ForgeTable>, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let rows = sqlx::query(
            "SELECT table_name, NULL as table_comment 
             FROM information_schema.tables 
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
        )
        .fetch_all(pool)
        .await?;

        let mut tables = Vec::new();
        for row in rows {
            let table_name: String = row.get(0);
            tables.push(ForgeTable {
                name: table_name,
                columns: Vec::new(),
                indices: Vec::new(),
                foreign_keys: Vec::new(),
                comment: None,
            });
        }
        Ok(tables)
    }

    pub fn map_postgres_type(&self, pg_type: &str, config: &ForgeConfig) -> String {
        let target_types = config.get_type_list("postgres", "on_read");
        let pg_type_lower = pg_type.to_lowercase();

        target_types
            .and_then(|t| t.get(&pg_type_lower))
            .cloned()
            .unwrap_or(pg_type_lower)
    }

    pub async fn fetch_columns(
        &self,
        table_name: &str,
        config: &ForgeConfig,
    ) -> Result<Vec<ForgeColumn>, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let sql = "
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length, 
                numeric_precision, 
                numeric_scale, 
                is_nullable, 
                column_default,
                udt_name
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position";

        let rows = sqlx::query(sql).bind(table_name).fetch_all(pool).await?;

        let mut columns = Vec::new();

        for row in rows {
            let name: String = row.get("column_name");
            let udt_name: String = row.get("udt_name");
            let data_type: String = row.get("data_type");

            let effective_type = if data_type == "USER-DEFINED" {
                &udt_name
            } else {
                &data_type
            };
            let mapped_type = self.map_postgres_type(effective_type, config);

            let length: Option<i32> = row.get("character_maximum_length");
            let precision: Option<i32> = row.get("numeric_precision");
            let scale: Option<i32> = row.get("numeric_scale");
            let is_nullable: String = row.get("is_nullable");
            let default: Option<String> = row.get("column_default");

            columns.push(ForgeColumn {
                name,
                data_type: mapped_type,
                length: length.map(|l| l as u32),
                precision: precision.map(|p| p as u32),
                scale: scale.map(|s| s as u32),
                is_nullable: is_nullable == "YES",
                is_primary_key: false, // Will be updated in fetch_indices or similar logic
                is_unsigned: false,    // Postgres has no unsigned
                auto_increment: default.as_deref().map_or(false, |d| d.contains("nextval")),
                default,
                comment: None,
                on_update: None,
                enum_values: None,
            });
        }

        Ok(columns)
    }

    pub async fn fetch_indices(&self, table_name: &str) -> Result<Vec<ForgeIndex>, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let sql = "
            SELECT
                i.relname as index_name,
                a.attname as column_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a
            WHERE
                t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relname = $1
            ORDER BY
                t.relname,
                i.relname";

        let rows = sqlx::query(sql).bind(table_name).fetch_all(pool).await?;

        let mut indices_map: IndexMap<String, ForgeIndex> = IndexMap::new();

        for row in rows {
            let index_name: String = row.get("index_name");
            let column_name: String = row.get("column_name");
            let is_unique: bool = row.get("is_unique");

            let entry = indices_map.entry(index_name.clone()).or_insert(ForgeIndex {
                name: index_name,
                columns: Vec::new(),
                is_unique,
            });
            entry.columns.push(column_name);
        }

        Ok(indices_map.into_iter().map(|(_, v)| v).collect())
    }

    pub async fn fetch_foreign_keys(
        &self,
        table_name: &str,
    ) -> Result<Vec<ForgeForeignKey>, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let sql = "
            SELECT
                tc.constraint_name, 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name=$1";

        let rows = sqlx::query(sql).bind(table_name).fetch_all(pool).await?;

        let mut fks = Vec::new();
        for row in rows {
            fks.push(ForgeForeignKey {
                name: row.get("constraint_name"),
                column: row.get("column_name"),
                ref_table: row.get("foreign_table_name"),
                ref_column: row.get("foreign_column_name"),
                on_delete: None,
                on_update: None,
            });
        }
        Ok(fks)
    }

    pub fn field_migration_sql(&self, field: &ForgeColumn, _config: &ForgeConfig) -> String {
        let mut sql = format!("{} {}", field.name, field.data_type);

        if let Some(len) = field.length {
            sql.push_str(&format!("({})", len));
        } else if let (Some(p), Some(s)) = (field.precision, field.scale) {
            sql.push_str(&format!("({},{})", p, s));
        }

        if !field.is_nullable {
            sql.push_str(" NOT NULL");
        }

        if let Some(def) = &field.default {
            sql.push_str(&format!(" DEFAULT {}", def));
        }

        sql
    }

    pub fn build_postgres_create_table_sql(
        &self,
        table: &ForgeTable,
        config: &ForgeConfig,
    ) -> String {
        let cols: Vec<String> = table
            .columns
            .iter()
            .map(|c| self.field_migration_sql(c, config))
            .collect();

        format!("CREATE TABLE {} (\n  {}\n)", table.name, cols.join(",\n  "))
    }

    pub fn create_table_migration_sql(
        &self,
        target_table: &ForgeTable,
        config: &ForgeConfig,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let mut statements = Vec::new();
        statements.push(self.build_postgres_create_table_sql(target_table, config));

        for index in &target_table.indices {
            statements.push(self.build_postgres_create_index_sql(&target_table.name, index));
        }

        Ok(statements)
    }

    pub fn delete_table_migration_sql(
        &self,
        target_table: &ForgeTable,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        Ok(vec![format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            target_table.name
        )])
    }

    pub fn alter_table_migration_sql(
        &self,
        source_table: &ForgeTable,
        target_table: &ForgeTable,
        config: &ForgeConfig,
        destructive: bool,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let mut statements = Vec::new();

        let mut source_cols = HashMap::new();
        for col in &source_table.columns {
            source_cols.insert(col.name.clone(), col);
        }

        let mut target_cols = HashMap::new();
        for col in &target_table.columns {
            target_cols.insert(col.name.clone(), col);
        }

        // Add or modify columns
        for source_col in &source_table.columns {
            if let Some(target_col) = target_cols.get(&source_col.name) {
                if source_col.data_type != target_col.data_type
                    || source_col.is_nullable != target_col.is_nullable
                {
                    statements.push(format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}, ALTER COLUMN {} {} NULL",
                        source_table.name,
                        source_col.name,
                        source_col.data_type,
                        source_col.name,
                        if source_col.is_nullable {
                            "DROP"
                        } else {
                            "SET"
                        }
                    ));
                }
            } else {
                statements.push(format!(
                    "ALTER TABLE {} ADD COLUMN {}",
                    source_table.name,
                    self.field_migration_sql(source_col, config)
                ));
            }
        }

        if destructive {
            for target_col in &target_table.columns {
                if !source_cols.contains_key(&target_col.name) {
                    statements.push(format!(
                        "ALTER TABLE {} DROP COLUMN {}",
                        source_table.name, target_col.name
                    ));
                }
            }
        }

        // Indices
        let mut source_indices = HashMap::new();
        for idx in &source_table.indices {
            source_indices.insert(idx.name.clone(), idx);
        }

        let mut target_indices = HashMap::new();
        for idx in &target_table.indices {
            target_indices.insert(idx.name.clone(), idx);
        }

        for source_idx in &source_table.indices {
            if !target_indices.contains_key(&source_idx.name) {
                statements
                    .push(self.build_postgres_create_index_sql(&source_table.name, source_idx));
            }
        }

        if destructive {
            for target_idx in &target_table.indices {
                if !source_indices.contains_key(&target_idx.name) {
                    statements.push(format!("DROP INDEX IF EXISTS {}", target_idx.name));
                }
            }
        }

        Ok(statements)
    }

    pub fn build_postgres_create_index_sql(&self, table_name: &str, index: &ForgeIndex) -> String {
        let unique = if index.is_unique { "UNIQUE " } else { "" };
        format!(
            "CREATE {}INDEX {} ON {} ({})",
            unique,
            index.name,
            table_name,
            index.columns.join(", ")
        )
    }

    pub fn map_row_to_universal_values(
        &self,
        row: &PgRow,
    ) -> Result<Vec<ForgeUniversalValue>, ForgeError> {
        let mut values = Vec::with_capacity(row.columns().iter().count());

        for (i, col) in row.columns().iter().enumerate() {
            let type_name = col.type_info().name();
            let col_name = col.name();

            // local error adapter
            let to_decode_err = |e: sqlx::Error| ForgeError::ColumnDecode {
                column: col_name.to_string(),
                type_info: type_name.to_string(),
                source: e,
            };

            // Prüfung auf NULL
            if row.try_get_raw(i).map(|v| v.is_null()).unwrap_or(true) {
                values.push(ForgeUniversalValue::Null);
                continue;
            }

            let val = match type_name {
                "INT2" | "SMALLINT" | "SMALLSERIAL" => {
                    ForgeUniversalValue::Integer(row.try_get::<i16, _>(i).map_err(to_decode_err)? as i64)
                }
                "INT4" | "INTEGER" | "SERIAL" => {
                    ForgeUniversalValue::Integer(row.try_get::<i32, _>(i).map_err(to_decode_err)? as i64)
                }
                "INT8" | "BIGINT" | "BIGSERIAL" => {
                    ForgeUniversalValue::Integer(row.try_get::<i64, _>(i).map_err(to_decode_err)?)
                }
                "FLOAT4" | "REAL" => ForgeUniversalValue::Float(row.try_get::<f32, _>(i).map_err(to_decode_err)? as f64),
                "FLOAT8" | "DOUBLE PRECISION" => ForgeUniversalValue::Float(row.get::<f64, _>(i)),
                "TEXT" | "VARCHAR" | "CHAR" | "BPCHAR" | "NAME" => {
                    ForgeUniversalValue::Text(row.try_get::<String, _>(i).map_err(to_decode_err)?)
                }
                "BYTEA" => ForgeUniversalValue::Binary(row.try_get::<Vec<u8>, _>(i).map_err(to_decode_err)?),
                "BOOL" | "BOOLEAN" => ForgeUniversalValue::Boolean(row.try_get::<bool, _>(i).map_err(to_decode_err)?),
                "DATE" => ForgeUniversalValue::Date(row.try_get::<chrono::NaiveDate, _>(i).map_err(to_decode_err)?),
                "TIME" | "TIMETZ" => {
                    ForgeUniversalValue::Time(row.try_get::<chrono::NaiveTime, _>(i).map_err(to_decode_err)?)
                }
                "TIMESTAMP" | "TIMESTAMPTZ" => {
                    ForgeUniversalValue::DateTime(row.try_get::<chrono::NaiveDateTime, _>(i).map_err(to_decode_err)?)
                }
                "NUMERIC" | "DECIMAL" => {
                    ForgeUniversalValue::Decimal(row.try_get::<rust_decimal::Decimal, _>(i).map_err(to_decode_err)?)
                }
                "JSON" | "JSONB" => {
                    ForgeUniversalValue::Json(row.try_get::<serde_json::Value, _>(i).map_err(to_decode_err)?)
                }
                "UUID" => ForgeUniversalValue::Uuid(row.try_get::<sqlx::types::Uuid, _>(i).map_err(to_decode_err)?),
                "INET" | "CIDR" => {
                    ForgeUniversalValue::Inet(row.try_get::<ipnetwork::IpNetwork, _>(i).map_err(to_decode_err)?)
                }
                _ => return Err(ForgeError::UnsupportedPostgresType {
                    column: col_name.parse().unwrap(),
                    type_info: type_name.parse().unwrap(),
                }),
            };
            values.push(val);
        }
        Ok(values)
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn db_is_empty(&self) -> Result<bool, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'",
        )
        .fetch_one(pool)
        .await?;
        Ok(count == 0)
    }

    async fn fetch_schema(&self, config: &ForgeConfig) -> Result<ForgeSchema, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let db_name: String = sqlx::query_scalar("SELECT current_database()")
            .fetch_one(pool)
            .await?;

        let mut tables = self.fetch_tables().await?;
        for table in &mut tables {
            table.columns = self.fetch_columns(&table.name, config).await?;
            table.indices = self.fetch_indices(&table.name).await?;
            table.foreign_keys = self.fetch_foreign_keys(&table.name).await?;
        }

        Ok(ForgeSchema {
            metadata: ForgeMetadata {
                source_system: "postgres".to_string(),
                source_database_name: db_name,
                created_at: chrono::Local::now().to_rfc3339(),
                forge_version: env!("CARGO_PKG_VERSION").to_string(),
                config_file: "".to_string(),
            },
            tables,
        })
    }

    async fn diff_and_apply_schema(
        &self,
        source_schema: &ForgeSchema,
        config: &ForgeConfig,
        dry_run: bool,
        _verbose: bool,
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

        for source_table in &source_schema.tables {
            if let Some(target_table) = target_tables.get(&source_table.name) {
                let stmts = self.alter_table_migration_sql(
                    source_table,
                    target_table,
                    config,
                    destructive,
                )?;
                all_statements.extend(stmts);
            } else {
                let stmts = self.create_table_migration_sql(source_table, config)?;
                all_statements.extend(stmts);
            }
        }

        if destructive {
            for table in &target_schema.tables {
                if !source_tables.contains_key(&table.name) {
                    let stmts = self.delete_table_migration_sql(table)?;
                    all_statements.extend(stmts);
                }
            }
        }

        if !dry_run {
            let pool = self.pool.as_ref().ok_or("No database pool available")?;
            for sql in &all_statements {
                sqlx::query(sql).execute(pool).await?;
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
        Box<dyn Error>,
    > {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let query_string = format!("SELECT * FROM {}", table_name);

        let stream = async_stream::try_stream! {
            let mut rows = sqlx::query(&query_string).fetch(pool);

            while let Some(row) = rows.next().await {
                let row: PgRow = row?;
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
    ) -> Result<(), Box<dyn Error>> {
        if chunk.is_empty() {
            return Ok(());
        }

        let first_row = chunk.first().unwrap();
        let columns: Vec<String> = first_row.keys().cloned().collect();
        let column_names = columns.join(", ");

        let mut placeholders = Vec::new();
        let mut arg_count = 1;
        for _ in 0..chunk.len() {
            let mut row_placeholders = Vec::new();
            for _ in 0..columns.len() {
                row_placeholders.push(format!("${}", arg_count));
                arg_count += 1;
            }
            placeholders.push(format!("({})", row_placeholders.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            table_name,
            column_names,
            placeholders.join(", ")
        );

        if dry_run {
            println!("Dry run SQL: {}", sql);
        } else {
            let pool = self.pool.as_ref().ok_or("No database pool available")?;
            let mut query = sqlx::query(&sql);
            for row in &chunk {
                for col in &columns {
                    let val = row.get(col).unwrap_or(&ForgeUniversalValue::Null);
                    query = self.bind_universal(query, val);
                }
            }

            if let Err(e) = query.execute(pool).await {
                if halt_on_error {
                    return Err(Box::new(e));
                }
                // Row by row retry for better error logging (simplified for brevity)
                for row_map in &chunk {
                    let mut single_sql =
                        format!("INSERT INTO {} ({}) VALUES (", table_name, column_names);
                    let mut row_placeholders = Vec::new();
                    for i in 1..=columns.len() {
                        row_placeholders.push(format!("${}", i));
                    }
                    single_sql.push_str(&row_placeholders.join(", "));
                    single_sql.push(')');

                    let mut single_query = sqlx::query(&single_sql);
                    for col in &columns {
                        let val = row_map.get(col).unwrap_or(&ForgeUniversalValue::Null);
                        single_query = self.bind_universal(single_query, val);
                    }
                    if let Err(se) = single_query.execute(pool).await {
                        log_error_to_file(table_name, &format!("{:?}", row_map), &se.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_table_row_count(&self, table_name: &str) -> Result<u64, Box<dyn Error>> {
        let pool = self.pool.as_ref().ok_or("No database pool available")?;
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table_name))
            .fetch_one(pool)
            .await?;
        Ok(count as u64)
    }
}
