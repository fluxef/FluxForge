use crate::core::ForgeConfig;

// Diese Datei wird beim Kompilieren in das Binary "gebacken"
const DEFAULT_CONFIG_STR: &str = include_str!("default_mapping.toml");

pub fn load_config(user_path: Option<std::path::PathBuf>) -> ForgeConfig {
    let config_content = match user_path {
        // Falls der User eine Datei angegeben hat, lade diese
        Some(path) => std::fs::read_to_string(path)
            .expect("Could not read user config file"),
        // Ansonsten nimm den einkompilierten String
        None => DEFAULT_CONFIG_STR.to_string(),
    };

    // Den TOML-String in das Struct parsen
    toml::from_str(&config_content).expect("Invalid TOML in configuration")
}
