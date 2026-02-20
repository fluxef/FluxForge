#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use crate::common::TestContext;
    use fluxforge::core::ForgeConfig;
    use fluxforge::{drivers, ops};

    use std::env;

    /// test if mysql-test-infrastucture is working
    #[tokio::test]
    async fn test_mysql_to_mysql_connection() {
        // create Ref-Pools and a new , empty MySQL-target-DB
        let ctx = TestContext::setup().await;
        let source_url = env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE is missing");
        let target_url = format!("{}/{}", ctx.mysql_admin_url, ctx.db_name);

        println!("test test_mysql_to_mysql_connection()");
        println!("MySQL Source URL: {source_url}");
        println!("MySQL Target URL: {target_url}");
        println!(" ");

        // check Reference-DB from init-mysql.sql)
        let source_row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM bla")
            .fetch_one(&ctx.mysql_ref)
            .await
            .expect("Error! unable to read Reference-MySQL");

        println!("MySQL Source 'bla' has {} rows.", source_row.0);
        assert!(source_row.0 > 0);

        // check target  (dynamic empty MySQL-DB)
        // we create a small dummy table
        sqlx::query("CREATE TABLE heartbeat (id INT PRIMARY KEY)")
            .execute(&ctx.mysql_target_pool)
            .await
            .expect("Error creating tabke in dynamic MySQL-Target-DB");

        let target_check: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM heartbeat")
            .fetch_one(&ctx.mysql_target_pool)
            .await
            .unwrap();

        assert_eq!(target_check.0, 0);
        println!("Dynamic MySQL-Target-DB '{}' is ready.", ctx.db_name);
    }

    /// replicate mysql-reference to dynamic mysql target with verify enabled
    #[tokio::test]
    async fn test_mysql_to_mysql_replicate_with_verify() {
        let ctx = TestContext::setup().await;

        let config_toml = r"
# standard-file without mapping
[mysql.rules.on_write]
zero_date = true
";

        let forge_config: ForgeConfig =
            toml::from_str(config_toml).expect("Error parsing mysql mysql config");

        let source_url = env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE is missing");
        let target_url = format!("{}/{}", ctx.mysql_admin_url, ctx.db_name);

        println!("test test_mysql_to_mysql_replicate_with_verify() ");
        println!("MySQL Source URL: {source_url}");
        println!("MySQL Target URL: {target_url}");

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
