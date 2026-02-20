#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use crate::common::TestContext;
    use fluxforge::core::ForgeConfig;
    use fluxforge::{drivers, ops};

    use std::env;

    #[tokio::test]
    async fn test_mysql_to_postgres_connection() {
        // creates Ref-Pools and an empty new  Postgres-Target-DB
        let ctx = TestContext::setup().await;

        let source_url = env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE is missing");

        let target_url = format!("{}/{}", ctx.pg_admin_url, ctx.db_name);

        println!("test test_mysql_to_postgres_connectiony() ");
        println!("Mysql Source URL: {source_url}");
        println!("Postgres Target URL: {target_url}");

        // check Reference-DB from init-mysql.sql)
        let source_row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM bla")
            .fetch_one(&ctx.mysql_ref)
            .await
            .expect("Error! unable to read Reference-MySQL");

        println!("MySQL Source 'bla' has {} rows.", source_row.0);
        assert!(source_row.0 > 0);

        // check target (dynamic, empty Postgres-DB)
        sqlx::query("CREATE TABLE heartbeat (id SERIAL PRIMARY KEY)")
            .execute(&ctx.pg_target_pool)
            .await
            .expect("Error creating Table in dynamic Postgres-Target-DB");

        let target_check: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM heartbeat")
            .fetch_one(&ctx.pg_target_pool)
            .await
            .unwrap();

        assert_eq!(target_check.0, 0);
        println!("Dynamic Postgres-Target-DB '{}' is ready.", ctx.db_name);
    }

    #[tokio::test]
    async fn test_mysql_to_postgres_replication_with_verify() {
        // creates Ref-Pools and an empty new  Postgres-Target-DB
        let ctx = TestContext::setup().await;

        let config_toml = r#"

# --- MySQL Sektion ---
[mysql.general]
# default behaviour for unknown types
on_missing_type = "warn" # oder "error"
#
default_charset = "utf8mb4"

[mysql.types.on_read]
# from mysql to internal representation (usually postgres)
"int" = "integer"
"timestamp" = "datetimetz"
"tinyint" = "smallint"
"mediumint" = "integer"
"double" = "double precision"
"tinytext" = "text"
"mediumtext" = "text"
"longtext" = "text"
"tinyblob" = "bytea"
"mediumblob" = "bytea"
"longblob" = "bytea"
"blob" = "bytea"
"binary" = "bytea"
"varbinary" = "bytea"
"datetime" = "timestamp"
"datetimetz" = "timestamptz"
"year" = "smallint"
"bit" = "bytea"
"enum" = "varchar"
"set" = "varchar"
"char" = "varchar"
"biginteger" = "bigint"

[mysql.rules.on_read]
unsigned_int_to_bigint = true

[mysql.rules.on_write]
zero_date = true

# --- Postgres Sektion ---
[postgres.types.on_read]
"cidr" = "inet"
"json" = "json"
"jsonb" = "json"

[postgres.types.on_write]
"json" = "jsonb"
"datetimetz" = "timestamptz"

[postgres.rules.on_read]

[postgres.rules.on_write]

"#;

        let forge_config: ForgeConfig =
            toml::from_str(config_toml).expect("Error parsing postgres postgres config");

        let source_url = env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE is missing");

        let target_url = format!("{}/{}", ctx.pg_admin_url, ctx.db_name);

        println!("test test_mysql_to_postgres_replication_with_verify() ");
        println!("Mysql Source URL: {source_url}");
        println!("Postgres Target URL: {target_url}");

        let source_driver = drivers::create_driver(&source_url, &forge_config)
            .await
            .expect("Error creating source driver");
        let target_driver = drivers::create_driver(&target_url, &forge_config)
            .await
            .expect("Error creating target driver");

        let mut source_schema = source_driver
            .fetch_schema(&forge_config)
            .await
            .expect("Error fetching source schema");

        let sorted_tables = ops::sort_tables_by_dependencies(&source_schema)
            .expect("Error sorting tables by dependencies");
        source_schema.tables = sorted_tables;

        target_driver
            .diff_and_apply_schema(&source_schema, &forge_config, false, false, true)
            .await
            .expect("Error applying schema to target");

        ops::replicate_data(
            source_driver.as_ref(),
            target_driver.as_ref(),
            &source_schema,
            None,
            false,
            false,
            false,
            true,
        )
        .await
        .expect("Error replicating data with verify");
    }
}
