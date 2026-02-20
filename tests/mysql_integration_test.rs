#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[cfg(feature = "integration-tests")]
mod common;

#[cfg(feature = "integration-tests")]
mod tests {
    use crate::common::TestContext;
    use fluxforge::core::ForgeConfig;
    use fluxforge::core::ForgeUniversalDataField;
    use fluxforge::drivers::mysql::MySqlDriver;
    use fluxforge::{drivers, ops};
    use std::env;

    /// test if mysql-test-infrastucture is working
    #[tokio::test]
    async fn test_handle_datetime_special_cases() {
        // create Ref-Pools and a new , empty MySQL-target-DB
        let ctx = TestContext::setup().await;
        let source_url = env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE is missing");
        let target_url = format!("{}/{}", ctx.mysql_admin_url, ctx.db_name);

        println!("test test_handle_datetime_special_cases()");
        println!("MySQL Source URL: {source_url}");
        println!("MySQL Target URL: {target_url}");
        println!(" ");

        // IMPORTANT: set SQL_MODE for this session to relaxed
        let _ = sqlx::query("SET SESSION sql_mode = ''")
            .execute(&ctx.mysql_target_pool)
            .await;

        let _ = sqlx::query(
            "CREATE TABLE test_time (
            id INT PRIMARY KEY,
            col_dt DATETIME,
            col_ts TIMESTAMP NULL,
            col_date DATE,
            col_time TIME
        )",
        )
        .execute(&ctx.mysql_target_pool)
        .await;

        // insert test values, real values , Zero-Dates and NULLs
        // IMPORTANT '0000-00-00' only works, if NO_ZERO_DATE in SQL_MODE is not too strict
        let _ = sqlx::query(
            "INSERT INTO test_time VALUES
        (1, '2024-05-20 12:00:00', '2024-05-20 12:00:00', '2024-05-20', '12:00:00'),
        (2, '0000-00-00 00:00:00', '0000-00-00 00:00:00', '0000-00-00', '00:00:00'),
        (3, NULL, NULL, NULL, NULL)",
        )
        .execute(&ctx.mysql_target_pool)
        .await;

        let rows = sqlx::query("SELECT * FROM test_time ORDER BY id ASC")
            .fetch_all(&ctx.mysql_target_pool)
            .await
            .expect("Failed to fetch rows");

        // we check if we really have 3 rows and the query was successful
        assert_eq!(rows.len(), 3, "Tabelle sollte 3 Zeilen enthalten");

        let mapper = MySqlDriver {
            pool: ctx.mysql_target_pool.clone(),
            zero_date_on_write: true,
        };

        // check row 1: correct types?
        let row1 = mapper
            .map_row_to_universal_values(&rows[0])
            .expect("Mapping failed");
        // col 1 (DATETIME) -> DateTime
        assert!(matches!(row1[1], ForgeUniversalDataField::DateTime(_)));
        // col 4 (TIME) -> Time
        assert!(matches!(row1[4], ForgeUniversalDataField::Time(_)));

        // check row 2: Zero-Dates
        let row2 = mapper
            .map_row_to_universal_values(&rows[1])
            .expect("Mapping failed");
        // all columns with 0000... should become ZeroDateTime
        assert!(matches!(row2[1], ForgeUniversalDataField::ZeroDateTime));
        assert!(matches!(row2[2], ForgeUniversalDataField::ZeroDateTime));
        assert!(matches!(row2[3], ForgeUniversalDataField::ZeroDateTime));
        // Info: TIME '00:00:00' can, depending on SQLx-Version, become Time(0) or ZeroDateTime

        // check row 3: real NULLs ---
        let row3 = mapper
            .map_row_to_universal_values(&rows[2])
            .expect("Mapping failed");
        assert!(matches!(row3[1], ForgeUniversalDataField::Null));
        assert!(matches!(row3[4], ForgeUniversalDataField::Null));
    }
}
