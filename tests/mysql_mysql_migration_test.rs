#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use crate::common::TestContext;
    use sqlx::Row;

    /// test if mysql-test-infrastucture is working
    #[tokio::test]
    async fn test_mysql_to_mysql_connection() {
        // create Ref-Pools and a new , empty MySQL-target-DB
        let ctx = TestContext::setup().await;

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





}


