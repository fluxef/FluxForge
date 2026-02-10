use crate::cli::Commands;
use fluxforge::{drivers, DatabaseDriver, ForgeSchema};
use futures::StreamExt;
use fluxforge::config::load_config;

pub async fn handle_command(command: Commands, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
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
            config: _,
            dry_run,
            schema_only,
        } => {
            // 1. Get Schema (from File OR Source)
            let _internal_schema = if let Some(path) = schema {
                println!("📖 Loading schema from file: {:?}", path);
                // Load from JSON logic
            } else {
                let src_url = source.as_ref().unwrap();
                println!("🔍 Extracting schema live from: {}", src_url);
                // Extract from DB logic
            };

            // 2. Structural Migration / Dry Run
            if dry_run {
                println!("📋 DRY RUN: Generation SQL statements for {}", target);
                // generate_sql(internal_schema, target)
            } else {
                println!("🚀 Applying schema to {}", target);
                // execute_ddl(internal_schema, target)
            }

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
) -> Result<(), Box<dyn std::error::Error>> {
    for table in &schema.tables {
        println!("🚚 Transferring table: {}", table.name);

        let mut data_stream = source.stream_table_data(&table.name).await?;
        let mut chunk = Vec::with_capacity(1000);

        while let Some(row_result) = data_stream.next().await {
            let row = row_result?;
            chunk.push(row);

            if chunk.len() >= 1000 {
                target.insert_chunk(&table.name, chunk).await?;
                chunk = Vec::with_capacity(1000); // Neuer leerer Chunk
            }
        }

        // Restliche Zeilen schreiben
        if !chunk.is_empty() {
            target.insert_chunk(&table.name, chunk).await?;
        }
    }
    Ok(())
}
