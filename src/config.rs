use crate::core::ForgeConfig;
use std::error::Error;

// this file will be baked into binary as default if no --config option is used
const DEFAULT_CONFIG_STR: &str = include_str!("../examples/mysql2postgres.toml");

/// load config file
pub fn load_config(user_path: Option<std::path::PathBuf>) -> Result<ForgeConfig, Box<dyn Error>> {
    let config_content = match user_path {
        Some(path) => std::fs::read_to_string(path)?,
        None => DEFAULT_CONFIG_STR.to_string(),
    };

    let config: ForgeConfig = toml::from_str(&config_content)?;
    Ok(config)
}

#[must_use] 
pub fn get_config_file_path(user_path: Option<std::path::PathBuf>) -> String {
    match user_path {
        Some(path) => path.to_string_lossy().to_string(),
        //        None => "default_mapping.toml".to_string(),
        None => "../examples/mysql2postgres.toml".to_string(),
    }
}
