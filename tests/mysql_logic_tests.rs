use fluxforge::drivers::MySqlDriver;
use fluxforge::core::{ForgeColumn, ForgeIndex, ForgeTable};

// sqlx lazy pool imports (no real DB connection attempted)
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use std::str::FromStr;

fn mk_driver() -> MySqlDriver {
    // Use a lazy pool that will not connect unless used.
    let opts = MySqlConnectOptions::from_str("mysql://user:pass@localhost:3306/testdb").unwrap();
    let pool = MySqlPoolOptions::new().connect_lazy_with(opts);
    MySqlDriver { pool }
}

fn col(name: &str, data_type: &str) -> ForgeColumn {
    ForgeColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        ..Default::default()
    }
}

fn idx(name: &str, cols: &[&str], unique: bool) -> ForgeIndex {
    ForgeIndex {
        name: name.to_string(),
        columns: cols.iter().map(|s| s.to_string()).collect(),
        is_unique: unique,
    }
}

#[tokio::test]
async fn test_parse_mysql_enum_values() {
    let drv = mk_driver();
    let vals = drv.parse_mysql_enum_values("enum('a','b','c')");
    assert_eq!(vals, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn test_indices_equal() {
    let drv = mk_driver();
    let a = idx("i1", &["a", "b"], false);
    let b_same = idx("i1", &["a", "b"], false);
    let c_diff_order = idx("i1", &["b", "a"], false);
    let d_diff_unique = idx("i1", &["a", "b"], true);

    assert!(drv.indices_equal(&a, &b_same));
    assert!(!drv.indices_equal(&a, &c_diff_order));
    assert!(!drv.indices_equal(&a, &d_diff_unique));
}

#[tokio::test]
async fn test_build_create_index_and_drop_index_sql() {
    let drv = mk_driver();
    let i1 = idx("idx_ab", &["a", "b"], false);
    let sql_create = drv.build_mysql_create_index_sql("users", &i1);
    assert_eq!(sql_create, "CREATE INDEX `idx_ab` ON `users` (`a`, `b`);");

    let sql_drop = drv.build_mysql_drop_index_sql("users", "idx_ab");
    assert_eq!(sql_drop, "DROP INDEX `idx_ab` ON `users`;");

    let i2 = idx("u_email", &["email"], true);
    let sql_create_u = drv.build_mysql_create_index_sql("users", &i2);
    assert_eq!(sql_create_u, "CREATE UNIQUE INDEX `u_email` ON `users` (`email`);");
}

#[tokio::test]
async fn test_field_migration_sql_variants() {
    let drv = mk_driver();

    // integer -> int NOT NULL DEFAULT '0'
    let mut c1 = col("id", "integer");
    c1.is_nullable = false;
    c1.default = Some("0".to_string());
    let sql1 = drv.field_migration_sql(c1);
    assert_eq!(sql1, "`id` int NOT NULL DEFAULT '0'");

    // varchar with length and NULL DEFAULT NULL
    let mut c2 = col("name", "varchar");
    c2.length = Some(255);
    c2.is_nullable = true;
    let sql2 = drv.field_migration_sql(c2);
    assert_eq!(sql2, "`name` varchar(255) NULL DEFAULT NULL");

    // decimal(10,2) NOT NULL DEFAULT '0.00'
    let mut c3 = col("price", "decimal");
    c3.precision = Some(10);
    c3.scale = Some(2);
    c3.is_nullable = false;
    c3.default = Some("0.00".to_string());
    let sql3 = drv.field_migration_sql(c3);
    assert_eq!(sql3, "`price` decimal(10,2) NOT NULL DEFAULT '0.00'");

    // enum('a','b')
    let mut c4 = col("state", "enum");
    c4.enum_values = Some(vec!["a".into(), "b".into()]);
    c4.is_nullable = true;
    let sql4 = drv.field_migration_sql(c4);
    assert_eq!(sql4, "`state` enum('a','b') NULL DEFAULT NULL");

    // auto_increment
    let mut c5 = col("id", "int");
    c5.is_nullable = false;
    c5.auto_increment = true;
    let sql5 = drv.field_migration_sql(c5);
    assert_eq!(sql5, "`id` int NOT NULL AUTO_INCREMENT");

    // CURRENT_TIMESTAMP default with ON UPDATE CURRENT_TIMESTAMP per implementation
    let mut c6 = col("updated_at", "timestamp");
    c6.is_nullable = true;
    c6.default = Some("current_timestamp".into());
    c6.on_update = Some("CURRENT_TIMESTAMP".into());
    let sql6 = drv.field_migration_sql(c6);
    assert_eq!(sql6, "`updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
}

#[tokio::test]
async fn test_build_mysql_create_table_sql_with_pk() {
    let drv = mk_driver();
    let mut t = ForgeTable::new("users");

    let mut id = col("id", "int");
    id.is_primary_key = true;
    id.is_nullable = false;
    t.columns.push(id.clone());

    let mut name = col("name", "varchar");
    name.length = Some(100);
    name.is_nullable = true;
    t.columns.push(name);

    let sql = drv.build_mysql_create_table_sql(&t);
    assert!(sql.starts_with("CREATE TABLE `users` ("));
    assert!(sql.contains("`id` int NOT NULL"));
    assert!(sql.contains("`name` varchar(100) NULL DEFAULT NULL"));
    assert!(sql.contains("PRIMARY KEY (`id`)"));
    assert!(sql.ends_with(";"));
}

#[tokio::test]
async fn test_build_mysql_add_and_modify_and_drop_column_sql() {
    let drv = mk_driver();
    let mut new_col = col("age", "int");
    new_col.is_nullable = false;
    new_col.default = Some("0".into());

    let add_sql = drv.build_mysql_add_column_sql("users", &new_col);
    assert_eq!(add_sql, "ALTER TABLE `users` ADD COLUMN `age` int NOT NULL DEFAULT '0';");

    let mut old_col = col("age", "varchar");
    old_col.is_nullable = true;

    // expect MODIFY to int NOT NULL DEFAULT '0'
    let modify_sql = drv.modify_column_migration("users", &old_col, &new_col, true);
    assert_eq!(modify_sql, "ALTER TABLE `users` MODIFY COLUMN `age` int NOT NULL DEFAULT '0';");

    let drop_sql = drv.drop_column_migration("users", &old_col);
    assert_eq!(drop_sql, "ALTER TABLE `users` DROP COLUMN `age`;");
}

#[tokio::test]
async fn test_create_and_delete_table_migration_sql_and_indices() {
    let drv = mk_driver();
    let mut t = ForgeTable::new("users");
    let mut id = col("id", "int");
    id.is_primary_key = true;
    id.is_nullable = false;
    t.columns.push(id);
    let mut email = col("email", "varchar");
    email.length = Some(200);
    t.columns.push(email);

    t.indices.push(idx("u_email", &["email"], true));

    let stmts = drv.create_table_migration_sql(&t).unwrap();
    assert_eq!(stmts.len(), 2, "should contain CREATE TABLE and CREATE INDEX");
    assert!(stmts[0].starts_with("CREATE TABLE `users`"));
    assert_eq!(stmts[1], "CREATE UNIQUE INDEX `u_email` ON `users` (`email`);");

    let drops = drv.delete_table_migration_sql(&t).unwrap();
    assert_eq!(drops, vec!["DROP TABLE `users`;".to_string()]);
}

#[tokio::test]
async fn test_alter_table_migration_sql_columns_and_indices() {
    let drv = mk_driver();

    // src_table: current DB state
    let mut src = ForgeTable::new("users");
    let mut src_id = col("id", "int");
    src_id.is_nullable = true; // force a MODIFY to NOT NULL in desired state
    src.columns.push(src_id);
    src.columns.push(col("legacy", "int")); // will be dropped in destructive
    src.indices.push(idx("idx_old", &["legacy"], false));

    // dst_table: desired state
    let mut dst = ForgeTable::new("users");
    let mut id = col("id", "int");
    id.is_nullable = false;
    dst.columns.push(id); // same name, but enforces NOT NULL via modify

    let mut name = col("name", "varchar");
    name.length = Some(150);
    name.is_nullable = true;
    dst.columns.push(name); // new column -> add

    // indices: replace idx_old with new, and add unique on name
    dst.indices.push(idx("u_name", &["name"], true)); // new index

    let stmts_non_destructive = drv.alter_table_migration_sql(&src, &dst, false).unwrap();
    // Expect: modify id (to add NOT NULL), add column name, create index u_name
    assert!(stmts_non_destructive.iter().any(|s| s == "ALTER TABLE `users` MODIFY COLUMN `id` int NOT NULL;"));
    assert!(stmts_non_destructive.iter().any(|s| s == "ALTER TABLE `users` ADD COLUMN `name` varchar(150);" || s == "ALTER TABLE `users` ADD COLUMN `name` varchar(150) NULL DEFAULT NULL;"));
    assert!(stmts_non_destructive.iter().any(|s| s == "CREATE UNIQUE INDEX `u_name` ON `users` (`name`);"));
    // Should NOT drop legacy column or idx_old without destructive
    assert!(!stmts_non_destructive.iter().any(|s| s.contains("DROP COLUMN `legacy`")));
    assert!(!stmts_non_destructive.iter().any(|s| s.contains("DROP INDEX `idx_old`")));

    let stmts_destructive = drv.alter_table_migration_sql(&src, &dst, true).unwrap();
    // With destructive: legacy column and idx_old should be dropped
    assert!(stmts_destructive.iter().any(|s| s == "ALTER TABLE `users` DROP COLUMN `legacy`;"));
    assert!(stmts_destructive.iter().any(|s| s == "DROP INDEX `idx_old` ON `users`;"));
}
