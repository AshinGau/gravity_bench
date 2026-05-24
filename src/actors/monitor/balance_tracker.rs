use alloy::primitives::{Address, U256};
use std::collections::HashMap;
use tracing::{debug, info};

/// Tracks per-account ETH balance in receipt mode.
/// Balance is updated by subtracting gas costs from transaction receipts.
pub struct BalanceTracker {
    balances: HashMap<Address, U256>,
    /// Below this threshold, re-faucet is triggered (in wei)
    low_balance_threshold: U256,
    /// Amount to re-faucet (in wei)
    refaucet_amount: U256,
}

impl BalanceTracker {
    /// Create a new BalanceTracker.
    ///
    /// `low_balance_threshold_gwei`: threshold in Gwei
    /// `refaucet_eth`: re-faucet amount in whole ETH
    pub fn new(low_balance_threshold_gwei: u64, refaucet_eth: u64) -> Self {
        Self {
            balances: HashMap::new(),
            low_balance_threshold: U256::from(low_balance_threshold_gwei) * U256::from(1_000_000_000u64),
            refaucet_amount: U256::from(refaucet_eth) * U256::from(10u64).pow(U256::from(18)),
        }
    }

    /// Set initial balance for an account (after faucet distribution)
    pub fn set_balance(&mut self, address: Address, balance: U256) {
        self.balances.insert(address, balance);
    }

    /// Deduct gas cost from a transaction receipt.
    /// Returns the updated balance, or None if the account is not tracked.
    pub fn deduct_gas(&mut self, address: &Address, gas_used: u128, effective_gas_price: u128) -> Option<U256> {
        let gas_cost = U256::from(gas_used) * U256::from(effective_gas_price);
        if let Some(balance) = self.balances.get_mut(address) {
            *balance = balance.saturating_sub(gas_cost);
            debug!("Balance updated for {}: deducted {} wei, remaining {} wei", address, gas_cost, balance);
            Some(*balance)
        } else {
            None
        }
    }

    /// Check if an account needs re-faucet
    pub fn needs_refaucet(&self, address: &Address) -> bool {
        self.balances.get(address)
            .map(|b| *b < self.low_balance_threshold)
            .unwrap_or(false)
    }

    /// Credit balance after re-faucet
    pub fn credit(&mut self, address: &Address, amount: U256) {
        if let Some(balance) = self.balances.get_mut(address) {
            *balance += amount;
            info!("Balance credited for {}: +{} wei, total {} wei", address, amount, balance);
        }
    }

    /// Get the re-faucet amount in wei
    pub fn refaucet_amount(&self) -> U256 {
        self.refaucet_amount
    }

    /// Get current tracked balance for an address
    #[allow(unused)]
    pub fn get_balance(&self, address: &Address) -> Option<U256> {
        self.balances.get(address).copied()
    }
}
