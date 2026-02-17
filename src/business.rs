use crate::cli::Commands;
use fluxforge::config::{get_config_file_path, load_config};
use fluxforge::{drivers, ops, ForgeSchema};

pub async fn handle_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::Extract {
            source,
            schema,
            config,
            verbose,
        } => {
            println!("Extracting schema from {}...", source);

            // load config, uses internal defaults if not file set
            let forge_config = load_config(config.clone())?;

            let source_driver = drivers::create_driver(&source, &forge_config).await?;

            let mut extracted_schema = source_driver.fetch_schema(&forge_config).await?;
            extracted_schema.metadata.config_file = get_config_file_path(config.clone());

            if verbose {
                println!(
                    "Extracted {} tables from source.",
                    extracted_schema.tables.len()
                );
            }

            let file = std::fs::File::create(&schema)?;
            serde_json::to_writer_pretty(file, &extracted_schema)?;

            if verbose {
                println!("Schema successfully forged and saved to: {:?}", schema);
            }
            Ok(())
        }

        // only schema diff, NO DATA TRANSFER
        // target-db must exist but can be non-empty
        // schema-file can be omitted, in which case source-db is used which then becomes mandatory
        Commands::Migrate {
            source,
            schema,
            target,
            config,
            dry_run,
            verbose,
            allow_destructive,
        } => {
            // source = new state (from source which is file or DB)
            // target state = actual state of DB that will be changed

            let forge_config = load_config(config.clone())?;

            let mut source_driver = None;

            let mut schema = if let Some(path) = schema {
                // reading schema from file
                let file = std::fs::File::open(&path)
                    .map_err(|e| format!("Error opening Schema-File {:?}: {}", path, e))?;
                let int_schema: ForgeSchema =
                    serde_json::from_reader(std::io::BufReader::new(file))
                        .map_err(|e| format!("Error parsing Schema-File {}.", e))?;

                int_schema
            } else {
                // reading schema from source database
                let src_url = source.as_ref().ok_or("Source URL is required.")?;
                let s_driver = drivers::create_driver(src_url, &forge_config).await?;
                let int_schema = s_driver.fetch_schema(&forge_config).await?;
                source_driver = Some(s_driver);

                int_schema
            };

            // sort tables (will become more important when foreign keys are implemented)
            ops::sort_tables_by_dependencies(&schema)
                .map(|sorted| schema.tables = sorted)
                .map_err(|e| format!("Circular Dependency Error: {}", e))?;

            let target_driver = drivers::create_driver(&target, &forge_config).await?;

            // apply schema diff to target
            let statements = target_driver
                .diff_and_apply_schema(&schema, &forge_config, dry_run, verbose, allow_destructive)
                .await?;

            if dry_run {
                println!("--- DRY RUN START : SQL changes ---");
                for sql in statements {
                    println!("{}", sql);
                }
                println!("--- DRY RUN END: SQL changes ---");
            }

            Ok(())
        }

        // complete transfer of schema and data, target-db must exist and be empty
        // always from source-db, never from schema-file
        Commands::Replicate {
            source,
            target,
            config,
            dry_run,
            verbose,
            halt_on_error,
            verify,
        } => {
            // Validation of source and target database combinations
            let source_type = if source.starts_with("mysql://") {
                "mysql"
            } else if source.starts_with("postgres://") || source.starts_with("postgresql://") {
                "postgres"
            } else {
                "unknown"
            };

            let target_type = if target.starts_with("mysql://") {
                "mysql"
            } else if target.starts_with("postgres://") || target.starts_with("postgresql://") {
                "postgres"
            } else {
                "unknown"
            };

            let allowed = match (source_type, target_type) {
                ("mysql", "postgres") => true,
                ("mysql", "mysql") => true,
                ("postgres", "postgres") => true,
                ("postgres", "mysql") => false,
                _ => false,
            };

            if !allowed {
                let msg = format!(
                    "ERROR: Combination {} -> {} is not allowed.\n\
                     Allowed combinations are:\n\
                     - mysql -> postgres\n\
                     - mysql -> mysql\n\
                     - postgres -> postgres",
                    source_type, target_type
                );
                return Err(msg.into());
            }

            let forge_config = load_config(config.clone())?;
            let verify_enabled = verify
                || forge_config
                    .general
                    .as_ref()
                    .and_then(|general| general.verify_after_write)
                    .unwrap_or(false);

            // target database
            let target_driver = drivers::create_driver(&target, &forge_config).await?;

            if !target_driver.db_is_empty().await? {
                return Err("ERROR: Target is not empty!  \
                    For data loss protection the replication is only allowed into an empty database.".into());
            }

            // source database
            let source_driver = drivers::create_driver(&source, &forge_config).await?;
            let mut source_schema = source_driver.fetch_schema(&forge_config).await?;

            // sort tables (will become more important when foreign keys are implemented)
            ops::sort_tables_by_dependencies(&source_schema)
                .map(|sorted| source_schema.tables = sorted)
                .map_err(|e| format!("Circular Dependency Error: {}", e))?;

            // apply schema diff to target
            let statements = target_driver
                .diff_and_apply_schema(&source_schema, &forge_config, dry_run, verbose, true)
                .await?;

            if dry_run {
                println!("--- DRY RUN START: SQL changes ---");
                for sql in statements {
                    println!("{}", sql);
                }
                println!("--- DRY RUN END: SQL changes ---");
            }
            ops::replicate_data(
                source_driver.as_ref(),
                target_driver.as_ref(),
                &source_schema,
                dry_run,
                verbose,
                halt_on_error,
                verify_enabled,
            )
            .await?;

            Ok(())
        }
    }
}
