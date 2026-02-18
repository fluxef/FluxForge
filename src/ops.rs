//! Database operations for replication, verification, and dependency resolution.
//!
//! This module provides high-level operations for:
//! - Data replication between databases
//! - Schema dependency analysis and topological sorting
//! - Data verification after replication
//! - Error logging for failed operations

use crate::{DatabaseDriver, ForgeSchema, ForgeTable, ForgeUniversalValue};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

fn order_by_columns(table: &ForgeTable) -> Vec<String> {
    let primary_keys: Vec<String> = table
        .columns
        .iter()
        .filter(|col| col.is_primary_key)
        .map(|col| col.name.clone())
        .collect();

    if primary_keys.is_empty() {
        table.columns.iter().map(|col| col.name.clone()).collect()
    } else {
        primary_keys
    }
}

fn values_equal(left: &ForgeUniversalValue, right: &ForgeUniversalValue) -> bool {
    use ForgeUniversalValue::{
        Binary, Boolean, Date, DateTime, Decimal, Float, Inet, Integer, Json, Null, Text, Time,
        UnsignedInteger, Uuid, Year, ZeroDateTime,
    };

    match (left, right) {
        (Null, Null) | (ZeroDateTime, ZeroDateTime) => true,
        (Null, ZeroDateTime) | (ZeroDateTime, Null) => true,
        (Integer(a), Integer(b)) => a == b,
        (UnsignedInteger(a), UnsignedInteger(b)) => a == b,
        (Integer(a), UnsignedInteger(b)) => *a >= 0 && (*a as u64) == *b,
        (UnsignedInteger(a), Integer(b)) => *b >= 0 && *a == (*b as u64),
        (Float(a), Float(b)) => a == b,
        (Text(a), Text(b)) => a == b,
        (Binary(a), Binary(b)) => a == b,
        (Boolean(a), Boolean(b)) => a == b,
        (Year(a), Year(b)) => a == b,
        (Year(a), Integer(b)) => i64::from(*a) == *b,
        (Integer(a), Year(b)) => *a == i64::from(*b),
        (Time(a), Time(b)) => a == b,
        (Date(a), Date(b)) => a == b,
        (DateTime(a), DateTime(b)) => a == b,
        (Decimal(a), Decimal(b)) => a == b,
        (Json(a), Json(b)) => a == b,
        (Uuid(a), Uuid(b)) => a == b,
        (Inet(a), Inet(b)) => a == b,
        _ => false,
    }
}

fn rows_equal(
    columns: &[String],
    source_row: &indexmap::IndexMap<String, ForgeUniversalValue>,
    target_row: &indexmap::IndexMap<String, ForgeUniversalValue>,
) -> Result<(), String> {
    for column in columns {
        let source_value = source_row.get(column).unwrap_or(&ForgeUniversalValue::Null);
        let target_value = target_row.get(column).unwrap_or(&ForgeUniversalValue::Null);
        if !values_equal(source_value, target_value) {
            return Err(format!(
                "Mismatch in column `{column}`: expected {source_value:?} but got {target_value:?}"
            ));
        }
    }

    Ok(())
}

async fn verify_table_data(
    source: &dyn DatabaseDriver,
    target: &dyn DatabaseDriver,
    table: &ForgeTable,
    multi: &MultiProgress,
    style: &ProgressStyle,
) -> Result<(), Box<dyn std::error::Error>> {
    let order_by = order_by_columns(table);
    let column_names: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();

    let src_count = source.get_table_row_count(&table.name).await.unwrap_or(0);
    let tgt_count = target.get_table_row_count(&table.name).await.unwrap_or(0);
    println!(
        "Verifying '{}' | order_by={:?} | src_count={} | tgt_count={}",
        table.name, order_by, src_count, tgt_count
    );

    let pb = multi.add(ProgressBar::new(tgt_count));
    pb.set_style(style.clone());
    pb.set_message(format!("Verifying table: {}", table.name));

    let mut source_stream = source
        .stream_table_data_ordered(&table.name, &order_by)
        .await?;
    let mut target_stream = target
        .stream_table_data_ordered(&table.name, &order_by)
        .await?;
    let mut verified_rows = 0u64;

    loop {
        let source_next = source_stream.next().await;
        let target_next = target_stream.next().await;

        match (source_next, target_next) {
            (None, None) => break,
            (Some(Err(err)), _) | (_, Some(Err(err))) => return Err(Box::new(err)),
            (Some(Ok(source_row)), Some(Ok(target_row))) => {
                if let Err(message) = rows_equal(&column_names, &source_row, &target_row) {
                    return Err(format!(
                        "Verification failed for table `{}`: {}",
                        table.name, message
                    )
                    .into());
                }
                verified_rows += 1;
                pb.set_position(verified_rows);
            }
            _ => {
                return Err(format!(
                    "Verification failed for table `{}`: row count mismatch",
                    table.name
                )
                .into());
            }
        }
    }

    pb.finish_with_message(format!("Verified: {} ({} rows)", table.name, verified_rows));

    Ok(())
}

/// Replicates data from source to target database with optional verification.
///
/// Streams data from the source database and inserts it into the target database
/// in chunks of 1000 rows. Optionally verifies that all data was correctly replicated
/// by comparing source and target row-by-row.
///
/// # Arguments
///
/// * `source` - Source database driver
/// * `target` - Target database driver
/// * `schema` - Schema defining tables to replicate
/// * `dry_run` - If true, prints SQL without executing
/// * `_verbose` - Verbose output (currently unused)
/// * `halt_on_error` - If true, stops on first error; if false, logs and continues
/// * `verify_after_write` - If true, verifies data after each table is replicated
///
/// # Examples
///
/// ```no_run
/// use fluxforge::{ops, drivers, core::ForgeConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ForgeConfig::default();
/// let source = drivers::create_driver("mysql://user:pass@localhost/source", &config).await?;
/// let target = drivers::create_driver("postgres://user:pass@localhost/target", &config).await?;
/// let schema = source.fetch_schema(&config).await?;
///
/// ops::replicate_data(
///     source.as_ref(),
///     target.as_ref(),
///     &schema,
///     false, // dry_run
///     false, // verbose
///     true,  // halt_on_error
///     true   // verify_after_write
/// ).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - Database connection fails
/// - Data cannot be read from source
/// - Data cannot be written to target
/// - Verification fails (data mismatch)
/// - `halt_on_error` is true and any insert fails
pub async fn replicate_data(
    source: &dyn DatabaseDriver,
    target: &dyn DatabaseDriver,
    schema: &ForgeSchema,
    dry_run: bool,
    _verbose: bool,
    halt_on_error: bool,
    verify_after_write: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let multi = MultiProgress::new();

    // style for progress bar
    let style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} rows ({msg}) {per_sec}"
    )?
        .progress_chars("#>-");

    // if verbose {
    println!("Starting data replication");
    // }

    for table in &schema.tables {
        let row_count = source.get_table_row_count(&table.name).await.unwrap_or(0);
        let pb = multi.add(ProgressBar::new(row_count));
        pb.set_style(style.clone());
        pb.set_message(format!("Forging table: {}", table.name));

        let mut data_stream = source.stream_table_data(&table.name).await?;
        let mut chunk = Vec::with_capacity(1000);
        let mut total_rows = 0;

        while let Some(row_result) = data_stream.next().await {
            let row = row_result?;
            chunk.push(row);
            total_rows += 1;

            if chunk.len() >= 1000 {
                target
                    .insert_chunk(&table.name, dry_run, halt_on_error, chunk)
                    .await?;
                chunk = Vec::with_capacity(1000);
                pb.set_position(total_rows);
            }
        }

        // last remaining chunk
        if !chunk.is_empty() {
            target
                .insert_chunk(&table.name, dry_run, halt_on_error, chunk)
                .await?;
            pb.set_position(total_rows);
        }

        pb.finish_with_message(format!("Done: {} ({} rows)", table.name, total_rows));
        println!("  {}", table.name);

        if verify_after_write && !dry_run {
            verify_table_data(source, target, table, &multi, &style).await?;
        }
    }

    Ok(())
}

/// Sorts tables by foreign key dependencies using topological sort.
///
/// Ensures that tables are ordered such that referenced tables come before
/// tables that reference them. This is essential for correct data insertion
/// order when foreign key constraints are present.
///
/// # Arguments
///
/// * `schema` - Schema containing tables with foreign key relationships
///
/// # Examples
///
/// ```no_run
/// use fluxforge::{ops, core::ForgeSchema};
///
/// # fn example(schema: &ForgeSchema) -> Result<(), String> {
/// let sorted_tables = ops::sort_tables_by_dependencies(schema)?;
/// for table in sorted_tables {
///     println!("Table: {}", table.name);
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - Circular dependencies are detected (tables reference each other in a cycle)
/// - A foreign key references a non-existent table
pub fn sort_tables_by_dependencies(schema: &ForgeSchema) -> Result<Vec<ForgeTable>, String> {
    let mut graph = DiGraph::<&str, ()>::new();
    let mut nodes = HashMap::new();

    // add tables as nodes
    for table in &schema.tables {
        let node_idx = graph.add_node(&table.name);
        nodes.insert(&table.name, node_idx);
    }

    // make Edges for Foreign Keys
    for table in &schema.tables {
        let from_idx = nodes
            .get(&table.name)
            .ok_or_else(|| format!("Table {} not found in nodes", table.name))?;
        for fk in &table.foreign_keys {
            if let Some(to_idx) = nodes.get(&fk.ref_table) {
                // Kante von Ref-Tabelle zu aktueller Tabelle
                // (Ref-Tabelle muss zuerst existieren)
                graph.add_edge(*to_idx, *from_idx, ());
            }
        }
    }

    // sort to find dependencies
    match toposort(&graph, None) {
        Ok(sorted_indices) => {
            let mut sorted_tables = Vec::new();
            let table_map: HashMap<&str, &ForgeTable> =
                schema.tables.iter().map(|t| (t.name.as_str(), t)).collect();

            for idx in sorted_indices {
                let name = graph[idx];
                if let Some(table) = table_map.get(name) {
                    sorted_tables.push((*table).clone());
                }
            }
            Ok(sorted_tables)
        }
        Err(_) => {
            Err("Circular dependency detected! Die Tabellen hängen im Kreis voneinander ab.".into())
        }
    }
}

/// Logs database data errors to a file.
///
/// Appends error information to `migration_errors.log` in the current directory.
/// Used when `halt_on_error` is false to record failed row insertions without
/// stopping the entire replication process.
///
/// # Arguments
///
/// * `table` - Name of the table where the error occurred
/// * `row_data` - String representation of the row data that failed
/// * `error_msg` - Error message describing the failure
///
/// # Examples
///
/// ```no_run
/// use fluxforge::ops::log_error_to_file;
///
/// log_error_to_file(
///     "users",
///     &"id: 1, name: 'Alice'".to_string(),
///     "Duplicate key violation"
/// );
/// ```
///
/// # Panics
///
/// Panics if the log file cannot be opened or written to.
pub fn log_error_to_file(table: &str, row_data: &String, error_msg: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("migration_errors.log")
        .expect("Konnte Log-Datei nicht öffnen");

    let line = format!("TABLE: {table} | ERROR: {error_msg} | DATA: {row_data:?}\n");
    let _ = file.write_all(line.as_bytes());
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use indexmap::IndexMap;

    struct MockDriver {
        data: HashMap<String, Vec<IndexMap<String, ForgeUniversalValue>>>,
    }

    impl MockDriver {
        fn new(data: HashMap<String, Vec<IndexMap<String, ForgeUniversalValue>>>) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl DatabaseDriver for MockDriver {
        async fn db_is_empty(&self) -> Result<bool, Box<dyn std::error::Error>> {
            Ok(self.data.values().all(std::vec::Vec::is_empty))
        }

        async fn fetch_schema(
            &self,
            _config: &crate::ForgeConfig,
        ) -> Result<ForgeSchema, Box<dyn std::error::Error>> {
            Ok(ForgeSchema::default())
        }

        async fn diff_and_apply_schema(
            &self,
            _schema: &ForgeSchema,
            _config: &crate::ForgeConfig,
            _dry_run: bool,
            _verbose: bool,
            _destructive: bool,
        ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            Ok(Vec::new())
        }

        async fn stream_table_data(
            &self,
            table_name: &str,
        ) -> Result<
            std::pin::Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<IndexMap<String, ForgeUniversalValue>, crate::ForgeError>,
                        > + Send
                        + '_,
                >,
            >,
            Box<dyn std::error::Error>,
        > {
            self.stream_table_data_ordered(table_name, &[]).await
        }

        async fn stream_table_data_ordered(
            &self,
            table_name: &str,
            _order_by: &[String],
        ) -> Result<
            std::pin::Pin<
                Box<
                    dyn futures::Stream<
                            Item = Result<IndexMap<String, ForgeUniversalValue>, crate::ForgeError>,
                        > + Send
                        + '_,
                >,
            >,
            Box<dyn std::error::Error>,
        > {
            let rows = self.data.get(table_name).cloned().unwrap_or_default();
            let stream = async_stream::try_stream! {
                for row in rows {
                    yield row;
                }
            };
            Ok(Box::pin(stream))
        }

        async fn insert_chunk(
            &self,
            _table_name: &str,
            _dry_run: bool,
            _halt_on_error: bool,
            _chunk: Vec<IndexMap<String, ForgeUniversalValue>>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }

        async fn get_table_row_count(
            &self,
            table_name: &str,
        ) -> Result<u64, Box<dyn std::error::Error>> {
            Ok(self
                .data
                .get(table_name)
                .map_or(0, |rows| rows.len() as u64))
        }
    }

    fn build_table() -> ForgeTable {
        let mut table = ForgeTable::new("users");
        let mut id_column = crate::ForgeColumn::new("id", "int");
        id_column.is_primary_key = true;
        table.columns.push(id_column);
        table.columns.push(crate::ForgeColumn::new("name", "text"));
        table
    }

    fn row(id: i64, name: &str) -> IndexMap<String, ForgeUniversalValue> {
        let mut map = IndexMap::new();
        map.insert("id".to_string(), ForgeUniversalValue::Integer(id));
        map.insert(
            "name".to_string(),
            ForgeUniversalValue::Text(name.to_string()),
        );
        map
    }

    #[tokio::test]
    async fn verify_table_data_matches() {
        let mut data = HashMap::new();
        data.insert("users".to_string(), vec![row(1, "Ada"), row(2, "Bob")]);
        let source = MockDriver::new(data.clone());
        let target = MockDriver::new(data);
        let style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} rows ({msg}) {per_sec}",
        )
        .unwrap();
        let multi = MultiProgress::new();

        let result = verify_table_data(&source, &target, &build_table(), &multi, &style).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn verify_table_data_detects_mismatch() {
        let mut source_data = HashMap::new();
        source_data.insert("users".to_string(), vec![row(1, "Ada")]);
        let mut target_data = HashMap::new();
        target_data.insert("users".to_string(), vec![row(1, "Eve")]);
        let source = MockDriver::new(source_data);
        let target = MockDriver::new(target_data);
        let style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} rows ({msg}) {per_sec}",
        )
        .unwrap();
        let multi = MultiProgress::new();

        let result = verify_table_data(&source, &target, &build_table(), &multi, &style).await;

        assert!(result.is_err());
    }
}
