#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use std::env;
    use crate::common::TestContext;
    use sqlx::Row;
    use fluxforge::core::ForgeConfig;
    use fluxforge::{drivers, ops};

    #[tokio::test]
    async fn test_postgres_to_postgres_connection() {
        // creates Ref-Pools and an empty new  Postgres-Target-DB
        let ctx = TestContext::setup().await;

        // check source reference DB  (Referenz-DB from init-postgres.sql)
        let source_row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM foo")
            .fetch_one(&ctx.pg_ref)
            .await
            .expect("Error reading Reference-Postgres");

        println!("Postgres Source 'foo' hst {} Rows.", source_row.0);
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

    /// replicate mysql-reference to dynamic mysql target with verify enabled
    #[tokio::test]
    async fn test_postgres_to_postgres_replication_with_verify() {
        // creates Ref-Pools and an empty new  Postgres-Target-DB
        let ctx = TestContext::setup().await;


        let config_toml = r#"
# standard-file without mapping

"#;

        let forge_config: ForgeConfig = toml::from_str(config_toml)
            .expect("Error parsing postgres postgres config");


        let source_url = env::var("POSTGRES_URL_REFERENCE")
            .expect("POSTGRES_URL_REFERENCE is missing");
        let target_url = format!("{}/{}", ctx.pg_admin_url, ctx.db_name);

        println!("test test_postgres_to_postgres_replicate_with_verify() ");
        println!("Postgres Source URL: {}", source_url);
        println!("Postgres Target URL: {}", target_url);


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

        let sorted_tables =
            ops::sort_tables_by_dependencies(&source_schema)
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
            false,
            false,
            false,
            true,
        )
            .await
            .expect("Error replicating data with verify");
    }




}
