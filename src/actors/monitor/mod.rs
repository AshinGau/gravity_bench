mod mempool_tracker;

pub mod monitor_actor;
mod txn_tracker;

use actix::Message;
use alloy::primitives::{Address, TxHash};
use std::time::{Duration, Instant};
use std::sync::Arc;

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
    SuccessWithReceipt {
        tx_hash: TxHash,
        gas_used: u128,
        effective_gas_price: u128,
        status: bool,
    },
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

// --- New messages for BlockMonitor-driven receipt mode ---

/// Message from BlockMonitor when a transaction receipt is confirmed
#[derive(Message)]
#[rtype(result = "()")]
pub struct TxnConfirmed {
    pub tx_hash: TxHash,
    pub latency: Duration,
    pub gas_used: u128,
    pub effective_gas_price: u128,
    pub status: bool,
    pub metadata: Arc<TxnMetadata>,
}

/// Message from Consumer: tx submitted but waiting for BlockMonitor confirmation
#[derive(Message)]
#[rtype(result = "()")]
pub struct TxnSubmitted {
    pub tx_hash: TxHash,
    pub metadata: Arc<TxnMetadata>,
    pub rpc_url: String,
    pub send_time: Instant,
    pub signed_bytes: Arc<Vec<u8>>,
}

/// Message from Monitor to Producer: receipt confirmed, unlock nonce
#[derive(Message)]
#[rtype(result = "()")]
pub struct ReceiptConfirmed {
    pub metadata: Arc<TxnMetadata>,
    pub status: bool,
}

pub use monitor_actor::Monitor;
