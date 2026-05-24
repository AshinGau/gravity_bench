pub mod balance_tracker;
mod mempool_tracker;

pub mod monitor_actor;
mod txn_tracker;

use actix::Message;
use alloy::primitives::{Address, TxHash};
use std::{sync::Arc, time::Instant};

use crate::txn_plan::{PlanId, TxnMetadata};

// Monitor Messages
#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterProducer {
    pub addr: actix::Addr<crate::actors::producer::Producer>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterConsumer {
    pub addr: actix::Addr<crate::actors::consumer::Consumer>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RegisterPlan {
    pub plan_id: PlanId,
    pub plan_name: String,
}

#[derive(Debug)]
pub enum SubmissionResult {
    NonceTooLow {
        tx_hash: TxHash,
        expect_nonce: u64,
        actual_nonce: u64,
        from_account: Arc<Address>,
    },
    ErrorWithRetry,
    Success(TxHash),
    /// Transaction confirmed on-chain with receipt data.
    /// Contains gas cost info for balance tracking.
    SuccessWithReceipt {
        tx_hash: TxHash,
        gas_used: u128,
        effective_gas_price: u128,
        status: bool,
    },
    /// Insufficient balance to submit transaction. The transaction was never sent.
    /// Nonce should NOT be advanced; account should be retried after re-faucet.
    InsufficientBalance,
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct UpdateSubmissionResult {
    pub metadata: Arc<TxnMetadata>,
    pub result: Arc<SubmissionResult>,
    pub rpc_url: String,
    #[allow(unused)]
    pub send_time: Instant,
    /// Signed transaction bytes for retry support
    pub signed_bytes: Arc<Vec<u8>>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Tick;

// Monitor Messages
#[derive(Message)]
#[rtype(result = "()")]
pub struct ReportProducerStats {
    pub ready_accounts: u64,
    pub sending_txns: u64,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PlanCompleted {
    pub plan_id: PlanId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PlanFailed {
    pub plan_id: PlanId,
    pub reason: String,
}

/// Message to retry a timed-out transaction
#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct RetryTxn {
    pub signed_bytes: Arc<Vec<u8>>,
    pub metadata: Arc<TxnMetadata>,
}

/// Message sent to Producer when an account needs re-faucet
#[derive(Message)]
#[rtype(result = "()")]
pub struct RefaucetNeeded {
    pub account: Address,
    #[allow(unused)]
    pub account_id: crate::util::gen_account::AccountId,
}

/// Message to initialize balance tracking for all leaf accounts after faucet distribution
#[derive(Message)]
#[rtype(result = "()")]
pub struct InitBalances {
    /// Addresses of all leaf accounts
    pub addresses: Vec<Address>,
    /// Initial balance for each account (in wei)
    pub balance_per_account: alloy::primitives::U256,
}

/// Information for correcting account nonce
#[derive(Debug, Clone)]
pub struct NonceCorrectionInfo {
    pub account: Address,
    pub expected_nonce: u64,
}

/// Message to correct nonces based on txpool_content analysis
#[derive(Message)]
#[rtype(result = "()")]
pub struct CorrectNonces {
    pub corrections: Vec<NonceCorrectionInfo>,
}

pub use monitor_actor::Monitor;
