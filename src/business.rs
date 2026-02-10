use std::path::PathBuf;
use crate::cli::Commands;
use futures::StreamExt;
use fluxforge::{DatabaseDriver, ForgeSchema};

pub async fn handle_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Extract { source, schema, config } => {
            println!("🛠 Extracting schema from {} into {:?}", source, schema);
            // Quell-Treiber (kann MySQL oder Postgres sein)
            if let Some(src_url) = source {
                let source_driver = drivers::create_driver(&src_url).await?;

                let schema = source_driver.fetch_schema().await?;
                // ausgabe des schema in interne json struct datei

            }
            Ok(())

        }
        Commands::Migrate { source, schema, target, config, dry_run, schema_only } => {
            // 1. Get Schema (from File OR Source)
            let internal_schema = if let Some(path) = schema {
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
    schema: &ForgeSchema
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
