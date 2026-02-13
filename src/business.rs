use crate::cli::Commands;
use fluxforge::config::{get_config_file_path, load_config};
use fluxforge::{drivers, ops, ForgeSchema};

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

            // Konfiguration laden (Nutzt Standard, wenn keine Datei angegeben)
            let forge_config = load_config(config.clone())?;
            if verbose {
                let mysql_count = forge_config
                    .mysql
                    .as_ref()
                    .and_then(|m| m.types.as_ref())
                    .and_then(|t| t.on_read.as_ref())
                    .map(|t| t.len())
                    .unwrap_or(0);
                let pg_count = forge_config
                    .postgres
                    .as_ref()
                    .and_then(|m| m.types.as_ref())
                    .and_then(|t| t.on_read.as_ref())
                    .map(|t| t.len())
                    .unwrap_or(0);
                println!(
                    "Configuration loaded (Mappings: MySQL={}, Postgres={})",
                    mysql_count, pg_count
                );
            }

            // Quell-Treiber instanziieren
            let source_driver = drivers::create_driver(&source).await?;

            // Schema extrahieren
            // Der Driver sollte intern die forge_config nutzen, um Typen zu normalisieren
            let mut extracted_schema = source_driver.fetch_schema(&forge_config).await?;
            extracted_schema.metadata.config_file = get_config_file_path(config.clone());

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

            println!("Schema successfully forged and saved to: {:?}", schema);

            Ok(())
        }

        // only schema diff, NO DATA TRANSFER
        // target-db must exist and but can be non-empty
        // schema-file can be omitted, in which case source-db is used which then becomes mandatory
        Commands::Migrate {
            source,
            schema,
            target,
            config,
            dry_run,
            allow_destructive,
        } => {
            let forge_config = load_config(config.clone())?;

            let mut source_driver = None;

            let mut schema = if let Some(path) = schema {
                // reading schema from file
                let file = std::fs::File::open(&path).map_err(|e| {
                    format!("Fehler beim Öffnen der Schema-Datei {:?}: {}", path, e)
                })?;
                let int_schema: ForgeSchema =
                    serde_json::from_reader(std::io::BufReader::new(file))
                        .map_err(|e| format!("Fehler beim Parsen der JSON-Datei: {}", e))?;

                int_schema
            } else {
                // reading schema from live source database
                let src_url = source
                    .as_ref()
                    .ok_or("Source URL is required in live mode")?;
                let s_driver = drivers::create_driver(src_url).await?;
                let int_schema = s_driver.fetch_schema(&forge_config).await?;
                source_driver = Some(s_driver);

                int_schema
            };

            // Tabellen sortieren (Abhängigkeiten auflösen)
            ops::sort_tables_by_dependencies(&schema)
                .map(|sorted| schema.tables = sorted)
                .map_err(|e| format!("Abhängigkeitsfehler: {}", e))?;

            // Ziel-Treiber vorbereiten und Struktur anwenden
            let target_driver = drivers::create_driver(&target).await?;

            let statements = target_driver
                .diff_schema(&schema, &forge_config, dry_run, allow_destructive)
                .await?;

            if dry_run {
                println!("--- DRY RUN START : Geplante Strukturänderungen ---");
                for sql in statements {
                    println!("{}", sql);
                }
                println!("--- DRY RUN END: Geplante Strukturänderungen ---");
            }

            Ok(())
        }

        // complete transfer of schema and data, target-db must exist and be empty
        // always from source-db, never schema-file
        Commands::Replicate {
            source,
            target,
            config,
            dry_run,
            halt_on_error,
        } => {
            // Konfiguration laden
            let forge_config = load_config(config.clone())?;

            // Ziel-Treiber
            let target_driver = drivers::create_driver(&target).await?;

            if !target_driver.db_is_empty().await? {
                return Err("ABBRUCH: Die Zieldatenbank ist nicht leer.  \
                    Um Datenverlust zu vermeiden, führt FluxForge keine Migration in nicht-leere Datenbanken durch.".into());
            }

            // source schema
            let source_driver = drivers::create_driver(&source).await?;
            let mut source_schema = source_driver.fetch_schema(&forge_config).await?;

            // Tabellen sortieren (Abhängigkeiten auflösen)
            ops::sort_tables_by_dependencies(&source_schema)
                .map(|sorted| source_schema.tables = sorted)
                .map_err(|e| format!("Abhängigkeitsfehler: {}", e))?;

            // target schema in DB erstellen
            let statements = target_driver
                .diff_schema(&source_schema, &forge_config, dry_run, true)
                .await?;

            if dry_run {
                println!("--- DRY RUN START: Geplante Strukturänderungen ---");
                for sql in statements {
                    println!("{}", sql);
                }
                println!("--- DRY RUN END: Geplante Strukturänderungen ---");
            }
            ops::migrate_data(
                source_driver.as_ref(),
                target_driver.as_ref(),
                &source_schema,
                dry_run,
                verbose,
                halt_on_error,
            )
            .await?;

            Ok(())
        }
    }
}
