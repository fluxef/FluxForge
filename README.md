# FluxForge

[<img alt="github" src="https://img.shields.io/badge/github-fluxef%2FFluxForge-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/fluxef/FluxForge)
[<img alt="crates.io" src="https://img.shields.io/crates/v/fluxforge.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/fluxforge)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-fluxforge-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/fluxforge)
[![Build](https://github.com/fluxef/FluxForge/actions/workflows/build.yml/badge.svg)](https://github.com/fluxef/FluxForge/actions/workflows/build.yml)

A database schema converter and migration engine for MySQL and PostgreSQL written in Rust.

## Features

- **Schema Extraction**: Extract complete database schemas including tables, columns and indices (foreign keys are not yet supported)
- **Type Mapping**: Configurable type conversion between MySQL and PostgreSQL with built-in sensible defaults
- **Data Replication**: Stream and replicate data efficiently with chunked transfers and progress tracking
- **Verification**: Optional row-by-row verification to ensure data integrity after migration
- **Dependency Resolution**: Automatic topological sorting of tables based on foreign key relationships
- **Dual Mode**: Use as a standalone CLI tool or integrate as a library in your Rust projects
- **Supported Migrations**:
  - MySQL → PostgreSQL ✅
  - MySQL → MySQL ✅
  - PostgreSQL → PostgreSQL ✅

## Installation

### As a Library

Add FluxForge to your `Cargo.toml`:

```toml
[dependencies]
fluxforge = "0.2.0"
```

### As a Standalone Binary

Install via cargo:

```bash
cargo install fluxforge
```

## Usage

### As a Library

```rust
use fluxforge::{drivers, ops, core::ForgeConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration (or use default)
    let config = ForgeConfig::default();
    
    // Create database drivers
    let source = drivers::create_driver(
        "mysql://user:pass@localhost:3306/source_db",
        &config
    ).await?;
    
    let target = drivers::create_driver(
        "postgres://user:pass@localhost:5432/target_db",
        &config
    ).await?;
    
    // Extract schema from source
    let mut schema = source.fetch_schema(&config).await?;
    
    // Sort tables by dependencies
    let sorted_tables = ops::sort_tables_by_dependencies(&schema)?;
    schema.tables = sorted_tables;
    
    // Apply schema to target
    target.diff_and_apply_schema(&schema, &config, false, false, true).await?;
    
    // Replicate data with verification
    ops::replicate_data(
        source.as_ref(),
        target.as_ref(),
        &schema,
        false,  // dry_run
        false,  // verbose
        true,   // halt_on_error
        true    // verify_after_write
    ).await?;
    
    println!("Migration completed successfully!");
    Ok(())
}
```

### As a CLI Tool

#### Extract Schema to JSON

Extract a database schema into an intermediate JSON format:

```bash
fluxforge extract \
  --source "mysql://user:pass@localhost/mydb" \
  --schema schema.json \
  --config mapping.toml \
  --verbose
```

#### Migrate Schema Only

Apply schema changes without transferring data:

```bash
fluxforge migrate \
  --source "mysql://user:pass@localhost/source_db" \
  --target "postgres://user:pass@localhost/target_db" \
  --config mapping.toml \
  --dry-run \
  --verbose
```

Or migrate from a previously extracted schema file:

```bash
fluxforge migrate \
  --schema schema.json \
  --target "postgres://user:pass@localhost/target_db" \
  --config mapping.toml
```

#### Full Replication (Schema + Data)

Replicate both schema and data from source to target:

```bash
fluxforge replicate \
  --source "mysql://user:pass@localhost/source_db" \
  --target "postgres://user:pass@localhost/target_db" \
  --config mapping.toml \
  --verify \
  --verbose
```

**Note**: The target database must be empty for replication to proceed (data loss protection).

#### Configuration File Example

Create a `mapping.toml` file to customize type mappings and transformation rules:

```toml
[mysql.types.on_read]
"int" = "integer"
"tinyint" = "smallint"
"double" = "double precision"
"datetime" = "timestamp"
"blob" = "bytea"

[mysql.rules.on_read]
unsigned_int_to_bigint = true

[postgres.types.on_write]
"json" = "jsonb"
"datetimetz" = "timestamptz"
```
The "examples" folder contains three suggested configuration files:
- `mysql2postgres.toml`: For MySQL to PostgreSQL
- `mysql2mysql.toml`: For MySQL to MySQL
- `postgres2postgres.toml`: For PostgreSQL to PostgreSQL

The settings from `mysql2postgres.toml` are included in the executable standalone binary as default and are used if no mapping file is selected with `--config`.

## Documentation

Full API documentation is available on [docs.rs/fluxforge](https://docs.rs/fluxforge).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.


## Minimum Supported Rust Version (MSRV)

This crate requires Rust 1.92 or later.

## About Us

FluxForge development is sponsored and led by [INS GmbH](https://www.ins.de/)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
