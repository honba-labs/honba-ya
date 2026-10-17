use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BacktestConfig {
    pub feed: FeedConfig,
    pub strategy: StrategyConfig,
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub output: OutputConfig,
}

#[derive(Debug, Deserialize)]
pub struct FeedConfig {
    pub path: String,
    #[serde(default = "default_feed_format")]
    pub format: String,
}

fn default_feed_format() -> String {
    "csv".to_string()
}

#[derive(Debug, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    #[serde(default)]
    pub params: toml::Value,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_execution_mode")]
    pub mode: String,
}

fn default_execution_mode() -> String {
    "paper".to_string()
}

#[derive(Debug, Deserialize, Default)]
pub struct OutputConfig {
    pub format: Option<String>,
}
