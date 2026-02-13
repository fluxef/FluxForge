use crate::{DatabaseDriver, ForgeSchema, ForgeTable};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

pub async fn migrate_data(
    source: &dyn DatabaseDriver,
    target: &dyn DatabaseDriver,
    schema: &ForgeSchema,
    dry_run: bool,
    verbose: bool,
    halt_on_error: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let multi = MultiProgress::new();

    // Style für die Schmiede-Anzeige
    let style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} rows ({msg}) {per_sec}"
    )?
        .progress_chars("#>-");

    if (verbose) {
        println!("Starte mit Datenreplikation");
    }

    for table in &schema.tables {
        // Wir wissen bei einem Stream oft nicht die Gesamtanzahl (len),
        // es sei denn, wir machen vorher ein SELECT COUNT(*).
        // Hier nutzen wir eine ProgressBar, die mitwächst.
        if (verbose) {
            println!("Starte Replikation von {}", table.name);
        }

        let pb = multi.add(ProgressBar::new_spinner());
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
                if !dry_run {
                    pb.set_position(total_rows);
                }
            }
        }

        // Letzten Rest verarbeiten
        if !chunk.is_empty() {
            target
                .insert_chunk(&table.name, dry_run, halt_on_error, chunk)
                .await?;
            if !dry_run {
                pb.set_position(total_rows);
            }
        }

        pb.finish_with_message(format!("Done: {} ({} rows)", table.name, total_rows));
    }

    Ok(())
}

pub fn sort_tables_by_dependencies(schema: &ForgeSchema) -> Result<Vec<ForgeTable>, String> {
    let mut graph = DiGraph::<&str, ()>::new();
    let mut nodes = HashMap::new();

    // 1. Alle Tabellen als Knoten in den Graphen einfügen
    for table in &schema.tables {
        let node_idx = graph.add_node(&table.name);
        nodes.insert(&table.name, node_idx);
    }

    // 2. Kanten (Edges) für Foreign Keys ziehen
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

    // 3. Topologisch sortieren
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


/// Schreibt problematische Zeilen in eine lokale .log Datei
pub fn log_error_to_file(table: &str, row_data: &String, error_msg: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("migration_errors.log")
        .expect("Konnte Log-Datei nicht öffnen");

    let line = format!(
        "TABLE: {} | ERROR: {} | DATA: {:?}\n",
        table, error_msg, row_data
    );
    let _ = file.write_all(line.as_bytes());
}


