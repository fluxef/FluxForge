//! Configuration loading and management.
//!
//! This module provides functions for loading FluxForge configuration from TOML files
//! or using embedded defaults.

use crate::core::ForgeConfig;
use std::error::Error;

// this file will be baked into binary as default if no --config option is used
const DEFAULT_CONFIG_STR: &str = include_str!("../examples/mysql2postgres.toml");

/// Loads configuration from a file or uses the embedded default.
///
/// If no path is provided, the embedded default configuration (mysql2postgres.toml)
/// is used. The configuration defines type mappings, transformation rules, and
/// other settings for database migration.
///
/// # Arguments
///
/// * `user_path` - Optional path to a custom configuration file
///
/// # Examples
///
/// ```no_run
/// use fluxforge::config::load_config;
/// use std::path::PathBuf;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Load default configuration
/// let config = load_config(None)?;
///
/// // Load custom configuration
/// let custom_config = load_config(Some(PathBuf::from("my_config.toml")))?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The specified file cannot be read
/// - The TOML content is invalid or cannot be parsed
/// - Required configuration fields are missing
pub fn load_config(user_path: Option<std::path::PathBuf>) -> Result<ForgeConfig, Box<dyn Error>> {
    let config_content = match user_path {
        Some(path) => std::fs::read_to_string(path)?,
        None => DEFAULT_CONFIG_STR.to_string(),
    };

    let config: ForgeConfig = toml::from_str(&config_content)?;
    Ok(config)
}

/// Returns the configuration file path as a string.
///
/// This is primarily used for metadata and logging purposes to track which
/// configuration was used for a migration.
///
/// # Arguments
///
/// * `user_path` - Optional path to a custom configuration file
///
/// # Examples
///
/// ```
/// use fluxforge::config::get_config_file_path;
/// use std::path::PathBuf;
///
/// let default_path = get_config_file_path(None);
/// assert_eq!(default_path, "../examples/mysql2postgres.toml");
///
/// let custom_path = get_config_file_path(Some(PathBuf::from("/etc/fluxforge.toml")));
/// assert_eq!(custom_path, "/etc/fluxforge.toml");
/// ```
#[must_use]
pub fn get_config_file_path(user_path: Option<std::path::PathBuf>) -> String {
    match user_path {
        Some(path) => path.to_string_lossy().to_string(),
        None => "../examples/mysql2postgres.toml".to_string(),
    }
}
