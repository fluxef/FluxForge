use crate::core::ForgeConfig;

use std::error::Error;

// Diese Datei wird beim Kompilieren in das Binary "gebacken"
const DEFAULT_CONFIG_STR: &str = include_str!("default_mapping.toml");

pub fn load_config(user_path: Option<std::path::PathBuf>) -> Result<ForgeConfig, Box<dyn Error>> {
    let config_content = match user_path {
        // Falls der User eine Datei angegeben hat, lade diese
        Some(path) => std::fs::read_to_string(path)?,
        // Ansonsten nimm den einkompilierten String
        None => DEFAULT_CONFIG_STR.to_string(),
    };

    // Den TOML-String in das Struct parsen
    let config: ForgeConfig = toml::from_str(&config_content)?;
    Ok(config)
}

pub fn get_config_file_path(user_path: Option<std::path::PathBuf>) -> String {
    match user_path {
        Some(path) => path.to_string_lossy().to_string(),
        None => "default_mapping.toml".to_string(),
    }
}
