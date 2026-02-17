#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, dead_code)]

use sqlx::{Connection, MySqlConnection, MySqlPool, PgConnection, PgPool};
use uuid::Uuid;

pub struct TestContext {
    /// Persistent read-only Reference-DB for mysql
    pub mysql_ref: MySqlPool,
    /// Persistent read-only Reference-DB for postgres
    pub pg_ref: PgPool,

    /// isolated (per test) dynamic MySQL-DB
    pub mysql_target_pool: MySqlPool,
    /// isolated (per test) dynamic Postgres-DB
    pub pg_target_pool: PgPool,

    /// common name for temporary DBs
    pub db_name: String,
    pub mysql_admin_url: String,
    pub pg_admin_url: String,
}

impl TestContext {
    pub async fn setup() -> Self {
        // create a uniqe db for this test run
        let unique_id = Uuid::new_v4().simple().to_string();
        let db_name = format!("test_db_{unique_id}");

        //  load URLs from environment (set from run_tests.sh)
        let mysql_ref_url =
            std::env::var("MYSQL_URL_REFERENCE").expect("MYSQL_URL_REFERENCE fehlt");
        let pg_ref_url =
            std::env::var("POSTGRES_URL_REFERENCE").expect("POSTGRES_URL_REFERENCE fehlt");

        let mysql_admin_url = std::env::var("MYSQL_URL_ADMIN").expect("MYSQL_URL_ADMIN fehlt");
        let pg_admin_url = std::env::var("POSTGRES_URL_ADMIN").expect("POSTGRES_URL_ADMIN fehlt");

        // create temporary MySQL DN
        let m_admin_result = MySqlConnection::connect(&mysql_admin_url).await;
        let mut m_admin_conn = match m_admin_result {
            Ok(conn) => conn,
            Err(e) => {
                panic!("\n\nMySQL Connect (Admin) failed!\nURL: {mysql_admin_url}\nError: {e}\n")
            }
        };

        sqlx::query(&format!("CREATE DATABASE {db_name}"))
            .execute(&mut m_admin_conn)
            .await
            .expect("Error creating temporary MySQL DB");

        // Create temporary Postgres DB
        let p_admin_result = PgConnection::connect(&pg_admin_url).await;
        let mut p_admin_conn = match p_admin_result {
            Ok(conn) => conn,
            Err(e) => {
                panic!("\n\nPostgres Connect (Admin) failed!\nURL: {pg_admin_url}\nError: {e}\n")
            }
        };

        sqlx::query(&format!("CREATE DATABASE {db_name}"))
            .execute(&mut p_admin_conn)
            .await
            .expect("Error creating temporary Postgres DB");

        // init Pools
        let mysql_ref = MySqlPool::connect(&mysql_ref_url).await.unwrap();
        let pg_ref = PgPool::connect(&pg_ref_url).await.unwrap();

        let mysql_target_pool = MySqlPool::connect(&format!("{mysql_admin_url}/{db_name}"))
            .await
            .unwrap();
        let pg_target_pool = PgPool::connect(&format!("{pg_admin_url}/{db_name}"))
            .await
            .unwrap();

        Self {
            mysql_ref,
            pg_ref,
            mysql_target_pool,
            pg_target_pool,
            db_name,
            mysql_admin_url,
            pg_admin_url,
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let name = self.db_name.clone();
        let m_url = self.mysql_admin_url.clone();
        let p_url = self.pg_admin_url.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                // MySQL Cleanup
                if let Ok(mut conn) = MySqlConnection::connect(&m_url).await {
                    let _ = sqlx::query(&format!("DROP DATABASE {name}"))
                        .execute(&mut conn)
                        .await;
                }

                // Postgres Cleanup
                if let Ok(mut conn) = PgConnection::connect(&p_url).await {
                    let _ = sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
                        .execute(&mut conn)
                        .await;
                }
            });
        })
        .join()
        .unwrap();
    }
}
