use alloy::{
    consensus::{SignableTransaction, TxEnvelope},
    network::{TransactionBuilder, TxSignerSync},
    primitives::{Address, Bytes, U256},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use anyhow::Result;
use std::sync::OnceLock;
use tracing::debug;

/// Default `tx_gas_price` when omitted from the config (60 Gwei in wei).
///
/// Sized to fit chains with low minimum base fees. For Gravity (50 Gwei
/// protocol minimum) the user should override to 100 Gwei or higher to
/// leave headroom for transient base-fee spikes under load.
pub const DEFAULT_TX_GAS_PRICE_WEI: u128 = 60_000_000_000;

/// Worst-case gas units used to size the per-txn cost budget reserved during
/// faucet distribution (covers ERC20/contract calls; ETH transfers are 21k).
const WORST_CASE_GAS_LIMIT: u128 = 100_000;

/// Fixed priority fee (tip) target. Clamped to `max_fee_per_gas` so configs
/// with sub-1-Gwei `tx_gas_price` stay valid (`tip <= max_fee`).
const TARGET_PRIORITY_FEE_PER_GAS: u128 = 1_000_000_000;

/// Resolved gas pricing, populated once from config at startup.
pub struct GasConfig {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub gas_cost_per_txn_budget: u128,
}

static GAS_CONFIG: OnceLock<GasConfig> = OnceLock::new();

/// Initialize the global gas config. Call once at startup, before any
/// transaction is built. Subsequent calls are ignored.
pub fn init_gas_config(tx_gas_price_wei: u128) {
    let max_fee = tx_gas_price_wei;
    let tip = std::cmp::min(TARGET_PRIORITY_FEE_PER_GAS, max_fee);
    let budget = max_fee.saturating_mul(WORST_CASE_GAS_LIMIT);
    let _ = GAS_CONFIG.set(GasConfig {
        max_fee_per_gas: max_fee,
        max_priority_fee_per_gas: tip,
        gas_cost_per_txn_budget: budget,
    });
}

fn gas_config() -> &'static GasConfig {
    GAS_CONFIG
        .get()
        .expect("gas config not initialized; call eth::init_gas_config at startup")
}

pub fn max_fee_per_gas() -> u128 {
    gas_config().max_fee_per_gas
}

pub fn max_priority_fee_per_gas() -> u128 {
    gas_config().max_priority_fee_per_gas
}

pub fn gas_cost_per_txn_budget() -> u128 {
    gas_config().gas_cost_per_txn_budget
}

/// TxnBuilder - Build and sign transactions
pub struct TxnBuilder;

impl TxnBuilder {
    /// Build and sign transaction
    pub fn build_and_sign_transaction(
        tx_request: TransactionRequest,
        signer: &PrivateKeySigner,
    ) -> Result<TxEnvelope> {
        debug!(
            "Building and signing transaction with request: {:?}",
            tx_request
        );
        debug!("Signer address: {:?}", signer.address());
        let mut unsigned_tx = tx_request.build_unsigned().unwrap();
        let sig = signer.sign_transaction_sync(&mut unsigned_tx)?;
        let tx_envelope = unsigned_tx.into_signed(sig);

        debug!("Transaction built and signed successfully");
        Ok(tx_envelope.into())
    }

    /// Build Uniswap V2 ETH for Token transaction request
    pub fn build_swap_exact_eth_for_tokens_request(
        router_address: Address,
        amount_out_min: U256,
        path: Vec<Address>,
        to: Address,
        deadline: U256,
        eth_amount: U256,
        nonce: u64,
        chain_id: u64,
    ) -> Result<TransactionRequest> {
        use crate::config::IUniswapV2Router;
        use alloy::sol_types::SolCall;

        let swap_call = IUniswapV2Router::swapExactETHForTokensCall {
            amountOutMin: amount_out_min,
            path,
            to,
            deadline,
        };

        let call_data = swap_call.abi_encode();
        let call_data = Bytes::from(call_data);

        let tx_request = TransactionRequest::default()
            .with_to(router_address)
            .with_input(call_data)
            .with_value(eth_amount)
            .with_nonce(nonce)
            .with_chain_id(chain_id)
            .with_max_priority_fee_per_gas(max_priority_fee_per_gas())
            .with_max_fee_per_gas(max_fee_per_gas())
            .with_gas_limit(300_000);

        Ok(tx_request)
    }

    #[allow(unused)]
    pub fn eth_transfer_request(
        from: Address,
        to: Address,
        amount: U256,
        nonce: u64,
        chain_id: u64,
    ) -> Result<TransactionRequest> {
        let tx_request = TransactionRequest::default()
            .with_from(from)
            .with_to(to)
            .with_value(amount)
            .with_nonce(nonce)
            .with_chain_id(chain_id)
            .with_max_priority_fee_per_gas(max_priority_fee_per_gas())
            .with_max_fee_per_gas(max_fee_per_gas())
            .with_gas_limit(100_000);

        Ok(tx_request)
    }

    #[allow(unused)]
    pub fn self_eth_transfer_request(
        to: Address,
        amount: U256,
        nonce: u64,
        chain_id: u64,
    ) -> Result<TransactionRequest> {
        let tx_request = TransactionRequest::default()
            .with_to(to)
            .with_value(amount)
            .with_nonce(nonce)
            .with_chain_id(chain_id)
            .with_max_priority_fee_per_gas(max_priority_fee_per_gas())
            .with_max_fee_per_gas(max_fee_per_gas())
            .with_gas_limit(100_000);

        Ok(tx_request)
    }
}
