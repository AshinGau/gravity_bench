use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::Addr;
use alloy::primitives::TxHash;
use futures::FutureExt;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::actors::monitor::{Monitor, TxnConfirmed};
use crate::eth::EthHttpCli;
use crate::txn_plan::TxnMetadata;

const WARN_PENDING_SECS: u64 = 30;
const STUCK_SECS: u64 = 120;
const TIMEOUT_SECS: u64 = 300;
const MAX_PENDING_TXNS: usize = 200_000;
const BACKPRESSURE_RESUME_THRESHOLD: usize = 160_000;

/// Information about a pending transaction waiting for receipt confirmation
pub struct PendingTxInfo {
    pub tx_hash: TxHash,
    pub metadata: Arc<TxnMetadata>,
    pub submit_time: Instant,
    pub rpc_url: String,
}

/// Block-driven receipt monitor.
/// Runs as a standalone tokio task, polling blocks and confirming receipts.
/// Replaces per-tx receipt polling in Consumer for the --receipt mode.
pub struct BlockMonitor {
    pending: HashMap<TxHash, PendingTxInfo>,
    monitor_addr: Addr<Monitor>,
    clients: Vec<Arc<EthHttpCli>>,
    last_seen_block: u64,
    register_rx: mpsc::Receiver<PendingTxInfo>,
    /// Channel to signal backpressure when pending set is too large
    backpressure_tx: Option<mpsc::Sender<bool>>,
}

impl BlockMonitor {
    pub fn new(
        monitor_addr: Addr<Monitor>,
        clients: Vec<Arc<EthHttpCli>>,
        register_rx: mpsc::Receiver<PendingTxInfo>,
        backpressure_tx: Option<mpsc::Sender<bool>>,
    ) -> Self {
        Self {
            pending: HashMap::new(),
            monitor_addr,
            clients,
            last_seen_block: 0,
            register_rx,
            backpressure_tx,
        }
    }

    /// Main loop: poll blocks, match pending txs, confirm receipts
    pub async fn run(mut self) {
        let mut summary_interval = tokio::time::interval(Duration::from_secs(10));
        let mut confirmed_10s: u64 = 0;
        let mut backpressure_signaled = false;

        // Get initial block number
        if let Some(client) = self.clients.first() {
            match client.get_block_number().await {
                Ok(n) => {
                    self.last_seen_block = n;
                    info!("BlockMonitor started at block {}", n);
                }
                Err(e) => {
                    error!("BlockMonitor: failed to get initial block number: {}", e);
                }
            }
        }

        loop {
            // Drain all pending registrations
            while let Ok(info) = self.register_rx.try_recv() {
                self.pending.insert(info.tx_hash, info);
            }

            // Process new blocks and confirm receipts
            if let Some(client) = self.clients.first().cloned() {
                let confirmed = self.process_new_blocks(&client).await;
                confirmed_10s += confirmed as u64;
            }

            // Check for stuck/timeout txs
            self.check_stuck_txs().await;

            // Check backpressure
            if self.pending.len() > MAX_PENDING_TXNS && !backpressure_signaled {
                backpressure_signaled = true;
                warn!(
                    "BlockMonitor: pending {} > {}, signaling backpressure",
                    self.pending.len(),
                    MAX_PENDING_TXNS
                );
                if let Some(tx) = &self.backpressure_tx {
                    let _ = tx.try_send(true);
                }
            } else if self.pending.len() < BACKPRESSURE_RESUME_THRESHOLD && backpressure_signaled {
                backpressure_signaled = false;
                info!(
                    "BlockMonitor: pending {} < {}, signaling resume",
                    self.pending.len(),
                    BACKPRESSURE_RESUME_THRESHOLD
                );
                if let Some(tx) = &self.backpressure_tx {
                    let _ = tx.try_send(false);
                }
            }

            // Log periodic summary
            if summary_interval.tick().now_or_never().is_some() {
                info!(
                    "BlockMonitor: pending={}, confirmed_10s={}, last_block={}",
                    self.pending.len(),
                    confirmed_10s,
                    self.last_seen_block
                );
                confirmed_10s = 0;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Fetch new blocks, cross-reference with pending set, confirm receipts
    async fn process_new_blocks(&mut self, client: &Arc<EthHttpCli>) -> usize {
        // Get current block number
        let current_block = match client.get_block_number().await {
            Ok(n) => n,
            Err(e) => {
                warn!("BlockMonitor: failed to get block number: {}", e);
                return 0;
            }
        };

        if current_block <= self.last_seen_block {
            return 0;
        }

        // Fetch all missed blocks concurrently
        let blocks_to_fetch: Vec<u64> = (self.last_seen_block + 1..=current_block).collect();
        self.last_seen_block = current_block;

        let block_futures: Vec<_> = blocks_to_fetch
            .iter()
            .map(|&n| {
                let c = client.clone();
                async move { (n, c.get_block_by_number(n).await) }
            })
            .collect();

        let block_results = futures::future::join_all(block_futures).await;

        // Collect tx hashes from blocks
        let mut block_tx_hashes = HashSet::new();
        for (_block_num, result) in block_results {
            match result {
                Ok(Some(block)) => {
                    match &block.transactions {
                        alloy::rpc::types::BlockTransactions::Full(txs) => {
                            for tx in txs {
                                block_tx_hashes.insert(*tx.inner.hash());
                            }
                        }
                        alloy::rpc::types::BlockTransactions::Hashes(hashes) => {
                            for hash in hashes {
                                block_tx_hashes.insert(*hash);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(None) => {
                    debug!("BlockMonitor: block {} returned None", _block_num);
                }
                Err(e) => {
                    warn!("BlockMonitor: failed to fetch block {}: {}", _block_num, e);
                }
            }
        }

        // Cross-reference with pending set
        let matched_hashes: Vec<TxHash> = block_tx_hashes
            .iter()
            .filter(|h| self.pending.contains_key(*h))
            .copied()
            .collect();

        if matched_hashes.is_empty() {
            return 0;
        }

        // Fetch receipts for matched txs concurrently (in batches of 200)
        let mut confirmed = 0;
        for chunk in matched_hashes.chunks(200) {
            let receipt_futures: Vec<_> = chunk
                .iter()
                .map(|&hash| {
                    let c = client.clone();
                    async move { (hash, c.get_transaction_receipt(hash).await) }
                })
                .collect();

            let receipt_results = futures::future::join_all(receipt_futures).await;

            for (tx_hash, result) in receipt_results {
                match result {
                    Ok(Some(receipt)) => {
                        if let Some(info) = self.pending.remove(&tx_hash) {
                            let latency = info.submit_time.elapsed();
                            self.monitor_addr.do_send(TxnConfirmed {
                                tx_hash,
                                latency,
                                gas_used: receipt.gas_used as u128,
                                effective_gas_price: receipt.effective_gas_price as u128,
                                status: receipt.status(),
                                metadata: info.metadata,
                            });
                            confirmed += 1;
                        }
                    }
                    Ok(None) => {
                        // Receipt not found even though tx hash was in block
                        // This can happen due to race conditions; tx will be picked up next cycle
                        debug!("BlockMonitor: receipt not yet available for {}", tx_hash);
                    }
                    Err(e) => {
                        warn!("BlockMonitor: failed to get receipt for {}: {}", tx_hash, e);
                    }
                }
            }
        }

        if confirmed > 0 {
            debug!(
                "BlockMonitor: confirmed {} txs from blocks {}..={}",
                confirmed,
                current_block.saturating_sub(blocks_to_fetch.len() as u64 - 1),
                current_block
            );
        }

        confirmed
    }

    /// Log warnings/errors for stuck transactions
    async fn check_stuck_txs(&self) {
        for (hash, info) in &self.pending {
            let elapsed_secs = info.submit_time.elapsed().as_secs();
            if elapsed_secs >= TIMEOUT_SECS {
                error!(
                    "BlockMonitor: tx {} timeout after {}s (account={:?}, nonce={})",
                    hash, elapsed_secs, info.metadata.from_account, info.metadata.nonce
                );
            } else if elapsed_secs >= STUCK_SECS {
                error!(
                    "BlockMonitor: tx {} stuck for {}s (account={:?}, nonce={})",
                    hash, elapsed_secs, info.metadata.from_account, info.metadata.nonce
                );
            } else if elapsed_secs >= WARN_PENDING_SECS {
                warn!(
                    "BlockMonitor: tx {} pending for {}s (account={:?}, nonce={})",
                    hash, elapsed_secs, info.metadata.from_account, info.metadata.nonce
                );
            }
        }
    }
}
