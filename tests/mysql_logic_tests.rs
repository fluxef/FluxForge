use fluxforge::core::{
    ForgeColumn, ForgeConfig, ForgeDbConfig, ForgeIndex, ForgeRuleGeneralConfig,
    ForgeRulesDirectionConfig, ForgeTable, ForgeTypeDirectionConfig,
};
use fluxforge::drivers::MySqlDriver;
use std::collections::HashMap;

// sqlx lazy pool imports (no real DB connection attempted)
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use std::str::FromStr;

fn mk_driver() -> MySqlDriver {
    // Use a lazy pool that will not connect unless used.
    let opts = MySqlConnectOptions::from_str("mysql://user:pass@localhost:3306/testdb").unwrap();
    let pool = MySqlPoolOptions::new().connect_lazy_with(opts);
    MySqlDriver { pool }
}

fn mk_config() -> ForgeConfig {
    ForgeConfig::default()
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
    assert_eq!(
        vals,
        vec!["a", "b", "c"],
        "parse_mysql_enum_values failed to extract correct enum values from 'enum('a','b','c')'"
    );
}

#[tokio::test]
async fn test_indices_equal() {
    let drv = mk_driver();
    let a = idx("i1", &["a", "b"], false);
    let b_same = idx("i1", &["a", "b"], false);
    let c_diff_order = idx("i1", &["b", "a"], false);
    let d_diff_unique = idx("i1", &["a", "b"], true);

    assert!(
        drv.indices_equal(&a, &b_same),
        "indices_equal failed: identical indices should be equal"
    );
    assert!(
        !drv.indices_equal(&a, &c_diff_order),
        "indices_equal failed: indices with different column order should NOT be equal"
    );
    assert!(
        !drv.indices_equal(&a, &d_diff_unique),
        "indices_equal failed: indices with different uniqueness should NOT be equal"
    );
}

#[tokio::test]
async fn test_build_create_index_and_drop_index_sql() {
    let drv = mk_driver();
    let i1 = idx("idx_ab", &["a", "b"], false);
    let sql_create = drv.build_mysql_create_index_sql("users", &i1);
    assert_eq!(
        sql_create, "CREATE INDEX `idx_ab` ON `users` (`a`, `b`);",
        "build_mysql_create_index_sql failed for non-unique index"
    );

    let sql_drop = drv.build_mysql_drop_index_sql("users", "idx_ab");
    assert_eq!(
        sql_drop, "DROP INDEX `idx_ab` ON `users`;",
        "build_mysql_drop_index_sql failed"
    );

    let i2 = idx("u_email", &["email"], true);
    let sql_create_u = drv.build_mysql_create_index_sql("users", &i2);
    assert_eq!(
        sql_create_u, "CREATE UNIQUE INDEX `u_email` ON `users` (`email`);",
        "build_mysql_create_index_sql failed for unique index"
    );
}

#[tokio::test]
async fn test_field_migration_sql_variants() {
    let drv = mk_driver();

    // integer -> integer (no fallback anymore)
    let mut c1 = col("id", "int");
    c1.is_nullable = false;
    c1.default = Some("0".to_string());
    let sql1 = drv.field_migration_sql(c1, &mk_config());
    assert_eq!(
        sql1, "`id` int NOT NULL DEFAULT '0'",
        "field_migration_sql failed for basic int column"
    );

    // varchar with length and NULL DEFAULT NULL
    let mut c2 = col("name", "varchar");
    c2.length = Some(255);
    c2.is_nullable = true;
    let sql2 = drv.field_migration_sql(c2, &mk_config());
    assert_eq!(
        sql2, "`name` varchar(255) NULL DEFAULT NULL",
        "field_migration_sql failed for varchar column with length"
    );

    // decimal(10,2) NOT NULL DEFAULT '0.00'
    let mut c3 = col("price", "decimal");
    c3.precision = Some(10);
    c3.scale = Some(2);
    c3.is_nullable = false;
    c3.default = Some("0.00".to_string());
    let sql3 = drv.field_migration_sql(c3, &mk_config());
    assert_eq!(
        sql3, "`price` decimal(10,2) NOT NULL DEFAULT '0.00'",
        "field_migration_sql failed for decimal column"
    );

    // enum('a','b')
    let mut c4 = col("state", "enum");
    c4.enum_values = Some(vec!["a".into(), "b".into()]);
    c4.is_nullable = true;
    let sql4 = drv.field_migration_sql(c4, &mk_config());
    assert_eq!(
        sql4, "`state` enum('a','b') NULL DEFAULT NULL",
        "field_migration_sql failed for enum column"
    );

    // auto_increment
    let mut c5 = col("id", "int");
    c5.is_nullable = false;
    c5.auto_increment = true;
    c5.is_unsigned = true;
    let sql5 = drv.field_migration_sql(c5, &mk_config());
    assert_eq!(
        sql5, "`id` int unsigned NOT NULL AUTO_INCREMENT",
        "field_migration_sql failed for auto_increment unsigned int"
    );

    // CURRENT_TIMESTAMP default with ON UPDATE CURRENT_TIMESTAMP per implementation
    let mut c6 = col("updated_at", "timestamp");
    c6.is_nullable = true;
    c6.default = Some("current_timestamp".into());
    c6.on_update = Some("CURRENT_TIMESTAMP".into());
    let sql6 = drv.field_migration_sql(c6, &mk_config());
    assert_eq!(
        sql6, "`updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "field_migration_sql failed for timestamp with current_timestamp"
    );
}

#[tokio::test]
async fn test_field_migration_sql_comprehensive_coverage() {
    let drv = mk_driver();
    let config = mk_config();

    // Matrix für int/decimal Defaults
    let cases = vec![
        // (Type, Precision, Scale, Default, ExpectedSQL)
        ("int", None, None, Some("0"), "`col` int NULL DEFAULT '0'"),
        ("int", None, None, Some("123"), "`col` int NULL DEFAULT '123'"),
        ("decimal", Some(10), Some(3), Some("0.000"), "`col` decimal(10,3) NULL DEFAULT '0.000'"),
        ("decimal", Some(10), Some(3), Some("123.456"), "`col` decimal(10,3) NULL DEFAULT '123.456'"),
        ("decimal", Some(10), None, Some("123"), "`col` decimal(10) NULL DEFAULT '123'"),
        ("bigint", None, None, Some("0"), "`col` bigint NULL DEFAULT '0'"),
    ];

    for (dtype, prec, scale, def, expected) in cases {
        let mut c = col("col", dtype);
        c.precision = prec;
        c.scale = scale;
        c.default = def.map(|s| s.to_string());
        c.is_nullable = true; // explicitly test NULL variant with default

        let sql = drv.field_migration_sql(c.clone(), &config);
        assert_eq!(sql, expected, "Failed for {} with default {:?}", dtype, def);
    }

    // Test varchar/char length variations
    let mut c_v = col("name", "varchar");
    c_v.length = Some(50);
    c_v.is_nullable = true;
    assert_eq!(drv.field_migration_sql(c_v.clone(), &config), "`name` varchar(50) NULL DEFAULT NULL");
    
    c_v.data_type = "char".to_string();
    assert_eq!(drv.field_migration_sql(c_v.clone(), &config), "`name` char(50) NULL DEFAULT NULL");

    // Test enum variations
    let mut c_e = col("mode", "enum");
    c_e.enum_values = Some(vec!["fast".into(), "slow".into()]);
    c_e.is_nullable = true;
    assert_eq!(drv.field_migration_sql(c_e, &config), "`mode` enum('fast','slow') NULL DEFAULT NULL");

    // Test NOT NULL variations (no default)
    let mut c_nn = col("id", "int");
    c_nn.is_nullable = false;
    assert_eq!(drv.field_migration_sql(c_nn, &config), "`id` int NOT NULL");

    // Test DEFAULT NULL for nullable field
    let mut c_nul = col("note", "text");
    c_nul.is_nullable = true;
    c_nul.default = None;
    assert_eq!(drv.field_migration_sql(c_nul, &config), "`note` text NULL DEFAULT NULL");

    // Test ON UPDATE behavior for non-timestamp (e.g. customized)
    let mut c_upd = col("val", "int");
    c_upd.is_nullable = true;
    c_upd.default = Some("1".into());
    c_upd.on_update = Some("val + 1".into());
    assert_eq!(drv.field_migration_sql(c_upd, &config), "`val` int NULL DEFAULT '1' ON UPDATE val + 1");
}

#[tokio::test]
async fn test_field_migration_sql_unsigned_matrix() {
    let drv = mk_driver();
    let config = mk_config();

    let types = vec!["int", "integer", "bigint"];
    let unsigned_variants = vec![true, false];

    for t in types {
        for is_unsigned in &unsigned_variants {
            let mut c = col("col_name", t);
            c.is_unsigned = *is_unsigned;
            c.is_nullable = false;

            let sql = drv.field_migration_sql(c, &config);

            if *is_unsigned {
                assert!(
                    sql.contains(" unsigned"),
                    "Type {} with is_unsigned=true should contain 'unsigned'. SQL: {}",
                    t,
                    sql
                );
            } else {
                assert!(
                    !sql.contains(" unsigned"),
                    "Type {} with is_unsigned=false should NOT contain 'unsigned'. SQL: {}",
                    t,
                    sql
                );
            }
        }
    }
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

    let sql = drv.build_mysql_create_table_sql(&t, &mk_config());
    assert!(
        sql.starts_with("CREATE TABLE `users` ("),
        "build_mysql_create_table_sql failed: missing CREATE TABLE prefix. SQL: {}",
        sql
    );
    assert!(
        sql.contains("`id` int NOT NULL"),
        "build_mysql_create_table_sql failed: missing 'id' column definition. SQL: {}",
        sql
    );
    assert!(
        sql.contains("`name` varchar(100) NULL DEFAULT NULL"),
        "build_mysql_create_table_sql failed: missing 'name' column definition. SQL: {}",
        sql
    );
    assert!(
        sql.contains("PRIMARY KEY (`id`)"),
        "build_mysql_create_table_sql failed: missing PRIMARY KEY definition. SQL: {}",
        sql
    );
    assert!(
        sql.ends_with(";"),
        "build_mysql_create_table_sql failed: missing semicolon at the end. SQL: {}",
        sql
    );
}

#[tokio::test]
async fn test_build_mysql_add_and_modify_and_drop_column_sql() {
    let drv = mk_driver();
    let mut new_col = col("age", "int");
    new_col.is_nullable = false;
    new_col.default = Some("0".into());

    let add_sql = drv.build_mysql_add_column_sql("users", &new_col, &mk_config());
    assert_eq!(
        add_sql, "ALTER TABLE `users` ADD COLUMN `age` int NOT NULL DEFAULT '0';",
        "build_mysql_add_column_sql failed"
    );

    let mut old_col = col("age", "varchar");
    old_col.is_nullable = true;

    // expect MODIFY to int NOT NULL DEFAULT '0'
    let modify_sql = drv.modify_column_migration("users", &new_col, &old_col, &mk_config(), true);
    assert_eq!(
        modify_sql, "ALTER TABLE `users` MODIFY COLUMN `age` int NOT NULL DEFAULT '0';",
        "modify_column_migration failed"
    );

    let drop_sql = drv.drop_column_migration("users", &old_col.name);
    assert_eq!(
        drop_sql, "ALTER TABLE `users` DROP COLUMN `age`;",
        "drop_column_migration failed"
    );
}

#[tokio::test]
async fn test_modify_column_migration_float_default_comparison() {
    let drv = mk_driver();

    // Case 1: Float with identical numeric defaults, but different string formats
    let mut c_src = col("f1", "float");
    c_src.default = Some("1.0".to_string());
    let mut c_dst = col("f1", "float");
    c_dst.default = Some("1".to_string());

    let sql1 = drv.modify_column_migration("t1", &c_src, &c_dst, &mk_config(), false);
    assert_eq!(
        sql1, "",
        "Numeric equality for float should not trigger migration"
    );

    // Case 2: Float with different numeric defaults
    c_dst.default = Some("1.1".to_string());
    let sql2 = drv.modify_column_migration("t1", &c_src, &c_dst, &mk_config(), false);
    assert!(
        sql2.contains("MODIFY COLUMN `f1` float"),
        "Numeric inequality should trigger migration. SQL: {}",
        sql2
    );
    assert!(
        sql2.contains("DEFAULT '1.0'"),
        "Should use src default. SQL: {}",
        sql2
    );

    // Case 3: Other type (varchar) with identical string content, should not trigger
    let mut v_src = col("v1", "varchar");
    v_src.default = Some("1.0".to_string());
    let mut v_dst = col("v1", "varchar");
    v_dst.default = Some("1.0".to_string());
    let sql3 = drv.modify_column_migration("t1", &v_src, &v_dst, &mk_config(), false);
    assert_eq!(sql3, "");

    // Case 4: Other type (varchar) with different string formats, should trigger (as before)
    v_dst.default = Some("1".to_string());
    let sql4 = drv.modify_column_migration("t1", &v_src, &v_dst, &mk_config(), false);
    assert!(
        sql4.contains("MODIFY COLUMN `v1` varchar"),
        "String inequality should trigger migration for non-float"
    );
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

    let stmts = drv.create_table_migration_sql(&t, &mk_config()).unwrap();
    assert_eq!(
        stmts.len(),
        2,
        "should contain CREATE TABLE and CREATE INDEX. Got: {:?}",
        stmts
    );
    assert!(
        stmts[0].starts_with("CREATE TABLE `users`"),
        "First statement should be CREATE TABLE. Got: {}",
        stmts[0]
    );
    assert_eq!(
        stmts[1], "CREATE UNIQUE INDEX `u_email` ON `users` (`email`);",
        "Second statement should be CREATE INDEX"
    );

    let drops = drv.delete_table_migration_sql(&t).unwrap();
    assert_eq!(
        drops,
        vec!["DROP TABLE `users`;".to_string()],
        "delete_table_migration_sql failed"
    );
}

#[tokio::test]
async fn test_alter_table_migration_sql_columns_and_indices() {
    let drv = mk_driver();

    // src_table: source database with desired state
    let mut src = ForgeTable::new("users");
    let mut src_id = col("id", "int");
    src_id.is_nullable = true; // force a MODIFY to NOT NULL in desired state
    src.columns.push(src_id);
    src.columns.push(col("legacy", "int")); // will be dropped in destructive
    src.indices.push(idx("idx_old", &["legacy"], false));

    // dst_table: target table (actual state)
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

    let stmts_non_destructive = drv
        .alter_table_migration_sql(&dst, &src, &mk_config(), false)
        .unwrap();
    // Expect: modify id (to add NOT NULL), add column name, create index u_name
    assert!(
        stmts_non_destructive
            .iter()
            .any(|s| s == "ALTER TABLE `users` MODIFY COLUMN `id` int NOT NULL;"),
        "Missing MODIFY COLUMN for 'id'. Statements: {:?}",
        stmts_non_destructive
    );
    assert!(
        stmts_non_destructive.iter().any(|s| s
            == "ALTER TABLE `users` ADD COLUMN `name` varchar(150);"
            || s == "ALTER TABLE `users` ADD COLUMN `name` varchar(150) NULL DEFAULT NULL;"),
        "Missing ADD COLUMN for 'name'. Statements: {:?}",
        stmts_non_destructive
    );
    assert!(
        stmts_non_destructive
            .iter()
            .any(|s| s == "CREATE UNIQUE INDEX `u_name` ON `users` (`name`);"),
        "Missing CREATE UNIQUE INDEX for 'u_name'. Statements: {:?}",
        stmts_non_destructive
    );
    // Should NOT drop legacy column or idx_old without destructive
    assert!(
        !stmts_non_destructive
            .iter()
            .any(|s| s.contains("DROP COLUMN `legacy`")),
        "Found unexpected DROP COLUMN 'legacy' in non-destructive mode. Statements: {:?}",
        stmts_non_destructive
    );
    assert!(
        !stmts_non_destructive
            .iter()
            .any(|s| s.contains("DROP INDEX `idx_old`")),
        "Found unexpected DROP INDEX 'idx_old' in non-destructive mode. Statements: {:?}",
        stmts_non_destructive
    );

    let stmts_destructive = drv
        .alter_table_migration_sql(&dst, &src, &mk_config(), true)
        .unwrap();
    // With destructive: legacy column and idx_old should be dropped
    assert!(
        stmts_destructive
            .iter()
            .any(|s| s == "ALTER TABLE `users` DROP COLUMN `legacy`;"),
        "Missing DROP COLUMN 'legacy' in destructive mode. Statements: {:?}",
        stmts_destructive
    );
    assert!(
        stmts_destructive
            .iter()
            .any(|s| s == "DROP INDEX `idx_old` ON `users`;"),
        "Missing DROP INDEX 'idx_old' in destructive mode. Statements: {:?}",
        stmts_destructive
    );
}

#[tokio::test]
async fn test_fetch_columns_mapping_logic() {
    let drv = mk_driver();

    let mut on_read = HashMap::new();
    on_read.insert("integer".to_string(), "int".to_string());
    on_read.insert("timestamp".to_string(), "datetimetz".to_string());
    on_read.insert("unsigned int".to_string(), "bigint".to_string());
    on_read.insert("unsigned integer".to_string(), "bigint".to_string());

    let config = ForgeConfig {
        mysql: Some(ForgeDbConfig {
            types: Some(ForgeTypeDirectionConfig {
                on_read: Some(on_read),
                on_write: None,
            }),
            rules: Some(ForgeRulesDirectionConfig {
                on_read: Some(ForgeRuleGeneralConfig {
                    unsigned_int_to_bigint: Some(true),
                    ..Default::default()
                }),
                on_write: None,
            }),
        }),
        ..Default::default()
    };

    // Tests for "int" variants
    // "int" (not in mapping, stays int)
    assert_eq!(
        drv.map_mysql_type("int", "int", false, &config),
        "int",
        "Test failed: 'int' should remain 'int'"
    );

    // "integer" (mapped to "int")
    assert_eq!(
        drv.map_mysql_type("integer", "integer", false, &config),
        "int",
        "Test failed: 'integer' should be mapped to 'int'"
    );

    // "unsigned int" (mapped to "bigint" via types)
    assert_eq!(
        drv.map_mysql_type("unsigned int", "unsigned int", true, &config),
        "bigint",
        "Test failed: 'unsigned int' should be mapped to 'bigint'"
    );

    // "unsigned integer" (mapped to "bigint" via types)
    assert_eq!(
        drv.map_mysql_type("unsigned integer", "unsigned integer", true, &config),
        "bigint",
        "Test failed: 'unsigned integer' should be mapped to 'bigint'"
    );

    // "bigint" (not in Mapping -> stays "bigint")
    assert_eq!(
        drv.map_mysql_type("bigint", "bigint", false, &config),
        "bigint",
        "Test failed: 'bigint' should remain 'bigint'"
    );

    // "int(11) unsigned" (Base "int", rule unsigned_int_to_bigint active -> "bigint")
    assert_eq!(
        drv.map_mysql_type("int(11) unsigned", "int", true, &config),
        "bigint",
        "Test failed: 'int(11) unsigned' with rule should be mapped to 'bigint'"
    );

    // "timestamp" (mapped to "datetimetz")
    assert_eq!(
        drv.map_mysql_type("timestamp", "timestamp", false, &config),
        "datetimetz",
        "Test failed: 'timestamp' should be mapped to 'datetimetz'"
    );

    // without rule
    let mut config_no_rules = config.clone();
    config_no_rules.mysql.as_mut().unwrap().rules = None;
    assert_eq!(
        drv.map_mysql_type("int(11) unsigned", "int", true, &config_no_rules),
        "int",
        "Testf failed: 'int(11) unsigned' without rule should remain 'int'"
    );

    // rule active but is_unsigned is false (must not become bigint)
    assert_eq!(
        drv.map_mysql_type("int", "int", false, &config),
        "int",
        "Test failed: 'int' (not unsigned) should remain 'int' even if rule is on"
    );
}

#[tokio::test]
async fn test_map_mysql_type_unsigned_matrix() {
    let drv = mk_driver();

    let mut config = ForgeConfig::default();
    config.mysql = Some(ForgeDbConfig {
        rules: Some(ForgeRulesDirectionConfig {
            on_read: Some(ForgeRuleGeneralConfig {
                unsigned_int_to_bigint: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    });

    // Matrix Tests for "int"

    // rule=true, is_unsigned=true -> bigint
    assert_eq!(
        drv.map_mysql_type("int(11) unsigned", "int", true, &config),
        "bigint",
        "Matrix: Rule=true, is_unsigned=true should result in 'bigint'"
    );

    // rule=true, is_unsigned=false -> int
    assert_eq!(
        drv.map_mysql_type("int(11)", "int", false, &config),
        "int",
        "Matrix: Rule=true, is_unsigned=false should result in 'int'"
    );

    // set rule to false
    config
        .mysql
        .as_mut()
        .unwrap()
        .rules
        .as_mut()
        .unwrap()
        .on_read
        .as_mut()
        .unwrap()
        .unsigned_int_to_bigint = Some(false);

    // rule=false, is_unsigned=true -> int
    assert_eq!(
        drv.map_mysql_type("int(11) unsigned", "int", true, &config),
        "int",
        "Matrix: Rule=false, is_unsigned=true should result in 'int'"
    );

    // rule=false, is_unsigned=false -> int
    assert_eq!(
        drv.map_mysql_type("int(11)", "int", false, &config),
        "int",
        "Matrix: Rule=false, is_unsigned=false should result in 'int'"
    );

    // bigint stays always bigint
    assert_eq!(
        drv.map_mysql_type("bigint unsigned", "bigint", true, &config),
        "bigint",
        "Matrix: 'bigint' should always remain 'bigint'"
    );
}
