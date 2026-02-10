use crate::cli::Commands;
use fluxforge::config::load_config;
use fluxforge::{drivers, DatabaseDriver, ForgeSchema, ForgeTable};
use futures::StreamExt;
use petgraph::algo::toposort;
use petgraph::graph::DiGraph;
use std::collections::HashMap;
use indicatif::{ProgressBar, ProgressStyle, MultiProgress};
use std::time::Duration;

pub async fn handle_command(
    command: Commands,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Extract {
            source,
            schema,
            config,
        } => {
            println!("Extracting schema from {}...", source);

            // 1. Konfiguration laden (Nutzt Standard, wenn keine Datei angegeben)
            // Wir nutzen hier die Logik mit include_str!, die wir besprochen hatten
            let forge_config = load_config(config);
            if verbose {
                println!(
                    "Configuration loaded (using Mappings for {} types)",
                    forge_config.types.len()
                );
            }

            // 2. Quell-Treiber instanziieren
            let source_driver = drivers::create_driver(&source).await?;

            // 3. Schema extrahieren
            // Der Driver sollte intern die forge_config nutzen, um Typen zu normalisieren
            let extracted_schema = source_driver.fetch_schema(&forge_config).await?;

            if verbose {
                println!(
                    "📊 Extracted {} tables from source.",
                    extracted_schema.tables.len()
                );
            }

            // 4. In JSON-Datei schreiben
            // Wir nutzen serde_json mit "pretty print", damit die Datei lesbar bleibt
            let file = std::fs::File::create(&schema)?;
            serde_json::to_writer_pretty(file, &extracted_schema)?;

            println!("💾 Schema successfully forged and saved to: {:?}", schema);

            Ok(())
        }

        Commands::Migrate {
            source,
            schema,
            target,
            config,
            dry_run,
            schema_only,
        } => {

            // 1. Konfiguration vorab laden (für Mappings)
            let forge_config = load_config(config.clone());

            // 2. Schema-Daten beschaffen (aus Datei ODER Live-DB)
            let mut schema = if let Some(path) = schema {
                // --- Szenario: Laden aus JSON-Datei ---
                println!("📖 Reading schema snapshot from: {:?}", path);

                let file = std::fs::File::open(&path)
                    .map_err(|e| format!("Failed to open schema file {:?}: {}", path, e))?;

                let reader = std::io::BufReader::new(file);

                // Deserialisierung des JSON in unser ForgeSchema Struct
                let int_schema: ForgeSchema = serde_json::from_reader(reader)
                    .map_err(|e| format!("Failed to parse JSON schema from {:?}: {}", path, e))?;

                println!("✅ Snapshot loaded (Source: {}, Version: {})",
                         int_schema.metadata.source_system,
                         int_schema.metadata.forge_version
                );

                int_schema
            } else {
                // --- Szenario: Extraktion aus Live-DB ---
                let src_url = source.as_ref().ok_or("No source URL or schema file provided")?;
                println!("🔍 Analyzing live database structure: {}", src_url);

                let source_driver = drivers::create_driver(src_url).await?;
                let int_schema = source_driver.fetch_schema(&forge_config).await?;

                println!("✅ Live schema extracted ({} tables found)", int_schema.tables.len());

                int_schema
            };


            println!("🔍 Analyzing and sorting table dependencies...");
            match sort_tables_by_dependencies(&schema) {
                Ok(sorted) => schema.tables = sorted,
                Err(e) => return Err(e.into()),
            }

            let target_driver = drivers::create_driver(&target).await?;

            // Jetzt erst apply_schema aufrufen (Postgres freut sich über die Reihenfolge)

            // Wenn dry_run == true, wird execute auf false gesetzt.
            let statements = target_driver.apply_schema(&schema, !dry_run).await?;

            if dry_run {
                println!("📝 --- DRY RUN: Generated SQL Statements ---");
                for sql in statements {
                    println!("{}\n", sql);
                }
                println!("📝 --- End of dry run. No changes were made. ---");
            } else {
                println!("🚀 Schema successfully applied to target.");
            }


            /* TODO
            // 3. Data Migration (Only if Source is present AND not schema_only)
            if !schema_only && !dry_run {
                if let Some(src_url) = source {
                    println!("🚚 Transferring data: {} -> {}", src_url, target);
                    // stream_data(src_url, target)
                } else {
                    println!("⚠️ Skipping data migration: No live --source provided.");
                }
            } else if schema_only {
                println!("ℹ️ Skipping data migration (--schema-only)");
            }
            */

            Ok(())
        }
    }
}

/*

pub async fn handle_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Migrate { source, target, .. } => {
            // Ziel-Treiber (immer Postgres laut deiner Anforderung)
            let target_driver = drivers::create_driver(&target).await?;

            // Quell-Treiber (kann MySQL oder Postgres sein)
            if let Some(src_url) = source {
                let source_driver = drivers::create_driver(&src_url).await?;

                let schema = source_driver.fetch_schema().await?;
                target_driver.apply_schema(&schema, false).await?;
                // ... Datenmigration ...
            }
            Ok(())
        }
        // ... andere Commands ...
    }
}
*/




pub async fn migrate_data(
    source: &dyn DatabaseDriver,
    target: &dyn DatabaseDriver,
    schema: &ForgeSchema,
    verbose: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let multi = MultiProgress::new();

    // Style für die Schmiede-Anzeige
    let style = ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} rows ({msg}) {per_sec}"
    )?
        .progress_chars("#>-");

    for table in &schema.tables {
        // Wir wissen bei einem Stream oft nicht die Gesamtanzahl (len),
        // es sei denn, wir machen vorher ein SELECT COUNT(*).
        // Hier nutzen wir eine ProgressBar, die mitwächst.
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
                target.insert_chunk(&table.name, chunk).await?;
                chunk = Vec::with_capacity(1000);
                pb.set_position(total_rows);
            }
        }

        // Letzten Rest verarbeiten
        if !chunk.is_empty() {
            target.insert_chunk(&table.name, chunk).await?;
            pb.set_position(total_rows);
        }

        pb.finish_with_message(format!("✅ Done: {} ({} rows)", table.name, total_rows));
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
        let from_idx = nodes.get(&table.name).unwrap();
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
