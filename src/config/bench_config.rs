use alloy::primitives::U256;
use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer, Serialize};
use std::path::Path;

/// Address pool type selection
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AddressPoolType {
    #[default]
    Random,
    Weighted,
}

/// Complete configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchConfig {
    pub nodes: Vec<NodeConfig>,
    pub faucet: FaucetConfig,
    pub accounts: AccountConfig,
    pub performance: PerformanceConfig,
    pub contract_config_path: String,
    pub num_tokens: usize,
    pub target_tps: u64,
    pub enable_swap_token: bool,
    #[serde(default)]
    pub address_pool_type: AddressPoolType,
    #[serde(default = "default_log_path")]
    pub log_path: String,
    /// Max fee per gas in Gwei (default: 5000)
    #[serde(default = "default_max_fee_per_gas")]
    pub max_fee_per_gas: u64,
    /// Max priority fee per gas in Gwei (default: 500)
    #[serde(default = "default_max_priority_fee_per_gas")]
    pub max_priority_fee_per_gas: u64,
    /// Re-faucet amount in ETH when account balance runs low (default: 1)
    #[serde(default = "default_refaucet_amount")]
    pub refaucet_amount: u64,
}

fn default_log_path() -> String {
    "./log.log".to_string()
}

fn default_max_fee_per_gas() -> u64 {
    5000
}

fn default_max_priority_fee_per_gas() -> u64 {
    500
}

fn default_refaucet_amount() -> u64 {
    1
}

/// Node and chain configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub rpc_url: String,
    pub chain_id: u64,
}



/// Parse a decimal string as ETH and convert to wei (× 1e18).
/// Example: "100" → 100 ETH → 100000000000000000000 wei
fn from_eth_str_to_u256<'de, D>(deserializer: D) -> Result<U256, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let eth: f64 = s.parse().map_err(serde::de::Error::custom)?;
    let wei = (eth * 1e18) as u128;
    Ok(U256::from(wei))
}

/// Faucet and deployer account configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FaucetConfig {
    pub private_key: String,
    pub faucet_level: u32,
    pub wait_duration_secs: u64,
    #[serde(deserialize_with = "from_eth_str_to_u256")]
    pub faucet_eth_balance: U256,
}

/// Load testing account configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountConfig {
    pub num_accounts: usize,
}

/// Performance and stress configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    /// Number of concurrent transaction sending tasks inside TxnConsumer
    pub num_senders: usize,
    /// Maximum capacity of the transaction pool inside Consumer
    pub max_pool_size: usize,
    /// Duration of the benchmark in seconds
    pub duration_secs: u64,
    /// Sampling configuration: "full" or integer size (default: 10)
    #[serde(default)]
    pub sampling: SamplingPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SamplingPolicy {
    Full(String),
    Partial(usize),
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        SamplingPolicy::Partial(10)
    }
}

impl SamplingPolicy {
    pub fn is_full(&self) -> bool {
        match self {
            SamplingPolicy::Full(s) => s.eq_ignore_ascii_case("full"),
            _ => false,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            SamplingPolicy::Partial(n) => *n,
            _ => 10, // Default fallback if needed, though is_full should be checked first
        }
    }
}

impl BenchConfig {
    /// Load configuration from TOML file
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: BenchConfig =
            toml::from_str(&content).with_context(|| "Failed to parse config file as TOML")?;

        Ok(config)
    }
}
