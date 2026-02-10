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

            // In src/business.rs -> match command { Commands::Migrate { ... } }

            // 1. Konfiguration laden
            let forge_config = load_config(config.clone());

            // 2. Schema beschaffen & Modus festlegen
            let (mut schema, can_migrate_data) = if let Some(path) = schema {
                // --- Datei-Modus ---
                let file = std::fs::File::open(&path)
                    .map_err(|e| format!("Fehler beim Öffnen der Schema-Datei {:?}: {}", path, e))?;
                let int_schema: ForgeSchema = serde_json::from_reader(std::io::BufReader::new(file))
                    .map_err(|e| format!("Fehler beim Parsen der JSON-Datei: {}", e))?;

                // In diesem Modus können keine Daten migriert werden
                (int_schema, false)
            } else {
                // --- Live-Modus ---
                let src_url = source.as_ref().unwrap(); // Durch Clap-Gruppe garantiert vorhanden
                let source_driver = drivers::create_driver(src_url).await?;
                let int_schema = source_driver.fetch_schema(&forge_config).await?;

                // Hier ist eine Datenmigration theoretisch möglich
                (int_schema, true)
            };

            // 3. Tabellen sortieren (Abhängigkeiten auflösen)
            sort_tables_by_dependencies(&schema)
                .map(|sorted| schema.tables = sorted)
                .map_err(|e| format!("Abhängigkeitsfehler: {}", e))?;

            // 4. Ziel-Treiber vorbereiten und Struktur anwenden
            let target_driver = drivers::create_driver(&target).await?;

            if !schema_only && can_migrate_data {
                // FALL A: Struktur + Datenmigration
                println!("🛡️ Sicherheitscheck: Prüfe Ziel-Datenbank auf vorhandene Daten...");
                if target_driver.has_data(&schema).await? {
                    return Err("❌ ABBRUCH: Die Zieldatenbank enthält bereits Daten. \
                    Um Datenverlust zu vermeiden, führt FluxForge keine Migration in nicht-leere Datenbanken durch.".into());
                }

                // Wenn leer, dann Struktur anlegen und Daten schieben
                target_driver.apply_schema(&schema, !dry_run).await?;
                if !dry_run {
                    migrate_data(source_driver.as_ref(), target_driver.as_ref(), &schema, verbose).await?;
                }

            } else {
                // FALL B: Nur Struktur-Anpassung (--schema-only oder Datei-Modus)
                println!("🔄 Modus: Struktur-Anpassung (In-Place Evolution).");

                // Hier rufen wir apply_schema auf.
                // Der Postgres-Driver muss hier intern erkennen, ob er CREATE oder ALTER nutzt.
                let statements = target_driver.apply_schema(&schema, !dry_run).await?;

                if dry_run {
                    println!("📝 --- DRY RUN: Geplante Strukturänderungen ---");
                    for sql in statements { println!("{}", sql); }
                }

            Ok(())

        }
    }
}


pub async fn migrate_data(
    source: &dyn DatabaseDriver,
    target: &dyn DatabaseDriver,
    schema: &ForgeSchema,
    verbose: bool,
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
