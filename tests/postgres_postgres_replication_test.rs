#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use crate::common::TestContext;
    use sqlx::Row;

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
}
