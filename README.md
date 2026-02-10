# FluxForge
[<img alt="github" src="https://img.shields.io/badge/github-fluxefS%2FFluxForge-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/fluxef/FluxForge)
[<img alt="crates.io" src="https://img.shields.io/crates/v/fluxforge.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/fluxforge)
[![Build](https://github.com/fluxef/FluxForge/actions/workflows/build.yml/badge.svg)](https://github.com/fluxef/FluxForge/actions/workflows/build.yml)

## About FluxForge
FluxForge is a database-tool written in Rust for reading, converting, and comparing database structures. It specializes in "forging" MySQL schemas into an internal format and seamlessly transforming them into PostgreSQL instances – including data migration.

## Key Features
* Schema Extraction: Accurately reads MySQL structures (tables, indexes, constraints).
* Intermediate Representation (IR): Converts the structure into a neutral, internal format ideal for version control or diffing.
* Postgres Transformation: Generates optimized PostgreSQL DDL from the internal representation.
* Database Diffing: Compares two database states (e.g., Live DB vs. Internal Format) and identifies structural discrepancies.
* Data Smithing: Safely and efficiently transfer records from MySQL to PostgreSQL.


## Installation (as Rust Library)
Add FluxForge to your Cargo.toml:

`[dependencies]
fluxforge = "0.1.0"`


## Installation (as Binary)
`cargo install fluxforge`

## Usage (CLI)

### 1. Extract schema into internal format

`fluxforge extract --source "mysql://user@localhost/db" --schema schema.json`
Optional: `--config mapping.toml`
Optional: `--verbose`


### 2. Migrate structure and optionally data

#### Migrate Schema and Data from MySQL to PostgreSQL

`fluxforge migrate --source "mysql://..." --target "postgres://...`

Optional:
* `--config mapping.toml`
* `--verbose`
* `--dry-run`   ensures that no migration takes place; instead, only the SQL statements are output.
*  `--schema-only` ensures that only the schema is migrated, without transferring data.


#### Migrate Schema from internal format to PostgreSQL

`fluxforge migrate --schema schema.json --target "postgres://..."`

Oprtional:

* `--config mapping.toml`
* `--verbose`
* `--dry-run` ensures that no migration takes place; instead, only the SQL statements are output.



Stay tuned for more!
