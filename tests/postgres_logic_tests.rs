#[cfg(test)]
mod tests {
    use fluxforge::core::{ForgeColumn, ForgeConfig, ForgeTable};
    use fluxforge::drivers::postgres::PostgresDriver;

    // Helper to create a driver without a real pool (will fail on DB calls, but ok for pure logic)
    fn mock_driver() -> PostgresDriver {
        PostgresDriver { pool: None }
    }

    #[test]
    fn test_field_migration_sql() {
        let driver = mock_driver();
        let config = ForgeConfig::default();

        let col = ForgeColumn {
            name: "test_col".to_string(),
            data_type: "varchar".to_string(),
            length: Some(255),
            is_nullable: false,
            default: Some("'default'".to_string()),
            ..ForgeColumn::default()
        };

        let sql = driver.field_migration_sql(&col, &config);
        assert_eq!(sql, "test_col varchar(255) NOT NULL DEFAULT 'default'");
    }

    #[test]
    fn test_build_postgres_create_table_sql() {
        let driver = mock_driver();
        let config = ForgeConfig::default();

        let mut table = ForgeTable::new("my_table");
        table.columns.push(ForgeColumn::new("id", "serial"));
        table.columns.push(ForgeColumn::new("name", "text"));

        let sql = driver.build_postgres_create_table_sql(&table, &config);
        let expected = "CREATE TABLE my_table (\n  id serial NOT NULL,\n  name text NOT NULL\n)";
        assert_eq!(sql, expected);
    }

    #[test]
    fn test_map_postgres_type() {
        let driver = mock_driver();
        let mut config = ForgeConfig::default();

        // Mock some config
        use std::collections::HashMap;
        let mut pg_read_map = HashMap::new();
        pg_read_map.insert(
            "timestamp without time zone".to_string(),
            "datetime".to_string(),
        );

        config.postgres = Some(fluxforge::core::ForgeDbConfig {
            types: Some(fluxforge::core::ForgeTypeDirectionConfig {
                on_read: Some(pg_read_map),
                on_write: None,
            }),
            rules: None,
        });

        let mapped = driver.map_postgres_type("timestamp without time zone", &config);
        assert_eq!(mapped, "datetime");

        let unmapped = driver.map_postgres_type("unknown", &config);
        assert_eq!(unmapped, "unknown");
    }

    #[test]
    fn test_alter_table_migration_sql_add_column() {
        let driver = mock_driver();
        let config = ForgeConfig::default();

        let mut source_table = ForgeTable::new("users");
        source_table.columns.push(ForgeColumn::new("id", "int4"));
        source_table
            .columns
            .push(ForgeColumn::new("email", "varchar"));

        let mut target_table = ForgeTable::new("users");
        target_table.columns.push(ForgeColumn::new("id", "int4"));

        let stmts = driver
            .alter_table_migration_sql(&source_table, &target_table, &config, false)
            .unwrap();

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ADD COLUMN email varchar"));
    }

    #[test]
    fn test_alter_table_migration_sql_modify_column() {
        let driver = mock_driver();
        let config = ForgeConfig::default();

        let mut source_table = ForgeTable::new("users");
        let mut col_new = ForgeColumn::new("id", "int8");
        col_new.is_nullable = true;
        source_table.columns.push(col_new);

        let mut target_table = ForgeTable::new("users");
        let mut col_old = ForgeColumn::new("id", "int4");
        col_old.is_nullable = false;
        target_table.columns.push(col_old);

        let stmts = driver
            .alter_table_migration_sql(&source_table, &target_table, &config, false)
            .unwrap();

        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("ALTER COLUMN id TYPE int8"));
        assert!(stmts[0].contains("ALTER COLUMN id DROP NULL"));
    }
}
