//! Dynamic best transactions iterator that pulls from a lock-free atomic cache.
//!
//! Uses `crossbeam_skiplist` for lock-free ordered access to transactions.
//! Updates are received via the existing [`PendingPool`] broadcast channel.
//!
//! This provides an alternative to the snapshot-based `BestTransactions` iterator,
//! allowing the iterator to see new transactions as they arrive in the pool.

use crate::{
    error::InvalidPoolTransactionError,
    identifier::{SenderId, TransactionId},
    pool::pending::PendingTransaction,
    traits::BestTransactions,
    TransactionOrdering, ValidPoolTransaction,
};
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::{
    collections::HashSet,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
};
use tokio::sync::broadcast;

/// Shared lock-free cache of best transactions.
///
/// This cache is continuously updated via a background task that subscribes
/// to the pool's transaction broadcast channel. No pool modifications needed!
///
/// All operations are lock-free using `crossbeam_skiplist`.
pub struct BestTransactionsCache<T: TransactionOrdering> {
    /// Independent transactions sorted by priority.
    /// Only contains transactions with current on-chain nonce.
    independent: SkipSet<PendingTransaction<T>>,

    /// All transactions including dependents, keyed by TransactionId.
    /// Used for nonce unlocking when independent tx is consumed.
    all: SkipMap<TransactionId, PendingTransaction<T>>,

    /// Tracks the current independent nonce per sender.
    /// Used to determine if incoming tx is independent or dependent.
    sender_nonces: SkipMap<SenderId, AtomicU64>,
}

impl<T: TransactionOrdering> BestTransactionsCache<T> {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self { independent: SkipSet::new(), all: SkipMap::new(), sender_nonces: SkipMap::new() }
    }

    /// Create a new cache and spawn background task to receive updates.
    ///
    /// Subscribes to the pool's existing broadcast channel - no pool modifications!
    pub fn new_with_receiver(receiver: broadcast::Receiver<PendingTransaction<T>>) -> Arc<Self>
    where
        T: TransactionOrdering + Send + Sync + 'static,
        T::Transaction: Send + Sync,
        T::PriorityValue: Send + Sync,
    {
        let cache = Arc::new(Self::new());
        let cache_clone = Arc::clone(&cache);

        // Spawn background task to process incoming transactions
        tokio::spawn(async move {
            cache_update_task(cache_clone, receiver).await;
        });

        cache
    }

    /// Handle a new transaction from the broadcast channel.
    /// Determines independence and inserts appropriately.
    ///
    /// Lock-free: uses atomic CAS internally.
    pub fn on_new_transaction(&self, tx: PendingTransaction<T>) {
        let sender_id = tx.transaction.sender_id();
        let nonce = tx.transaction.nonce();
        let tx_id = *tx.transaction.id();

        // Always add to all map
        self.all.insert(tx_id, tx.clone());

        // Determine if this tx is independent
        match self.sender_nonces.get(&sender_id) {
            None => {
                // First tx from this sender - it's independent
                self.sender_nonces.insert(sender_id, AtomicU64::new(nonce));
                self.independent.insert(tx);
            }
            Some(entry) => {
                let current_nonce = entry.value().load(AtomicOrdering::Relaxed);
                if nonce == current_nonce {
                    // This IS the current independent tx (shouldn't happen normally
                    // but handle it for safety)
                    self.independent.insert(tx);
                }
                // else: dependent tx, already in `all`, will be promoted when predecessor pops
            }
        }
    }

    /// Pop the best (highest priority) transaction.
    /// Handles filtering and dependent promotion.
    ///
    /// Lock-free: uses atomic CAS internally.
    pub fn pop_best(
        &self,
        invalid: &HashSet<SenderId>,
        skip_blobs: bool,
    ) -> Option<Arc<ValidPoolTransaction<T::Transaction>>> {
        loop {
            // pop_back returns highest priority (due to Ord impl on PendingTransaction)
            let entry = self.independent.pop_back()?;
            let tx = entry.value();
            let sender_id = tx.transaction.sender_id();

            // Check filters
            if invalid.contains(&sender_id) {
                // Remove from all map as well since this sender is invalid
                self.all.remove(tx.transaction.id());
                continue;
            }
            if skip_blobs && tx.transaction.is_eip4844() {
                // Remove from all map as well
                self.all.remove(tx.transaction.id());
                continue;
            }

            // Promote dependent (nonce + 1) if it exists in all map
            let next_id = tx.transaction.id().descendant();
            if let Some(next_entry) = self.all.get(&next_id) {
                self.independent.insert(next_entry.value().clone());
            }

            // Update sender's expected nonce for future independence checks
            if let Some(nonce_entry) = self.sender_nonces.get(&sender_id) {
                let next_nonce = tx.transaction.nonce() + 1;
                nonce_entry.value().store(next_nonce, AtomicOrdering::Relaxed);
            }

            // Remove from all map
            self.all.remove(tx.transaction.id());

            return Some(tx.transaction.clone());
        }
    }

    /// Clear all transactions from the cache.
    pub fn clear(&self) {
        while self.independent.pop_front().is_some() {}
        while self.all.pop_front().is_some() {}
        while self.sender_nonces.pop_front().is_some() {}
    }

    /// Create a new iterator over this cache.
    pub fn best(self: &Arc<Self>) -> DynamicBestTransactions<T> {
        DynamicBestTransactions {
            cache: Arc::clone(self),
            invalid: HashSet::new(),
            skip_blobs: false,
        }
    }

    /// Returns the number of independent transactions in the cache.
    pub fn len_independent(&self) -> usize {
        self.independent.len()
    }

    /// Returns the total number of transactions in the cache.
    pub fn len(&self) -> usize {
        self.all.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.all.is_empty()
    }
}

impl<T: TransactionOrdering> Default for BestTransactionsCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TransactionOrdering> fmt::Debug for BestTransactionsCache<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BestTransactionsCache")
            .field("independent_len", &self.independent.len())
            .field("all_len", &self.all.len())
            .field("sender_nonces_len", &self.sender_nonces.len())
            .finish()
    }
}

/// Background task that receives transactions from broadcast and updates cache.
async fn cache_update_task<T: TransactionOrdering>(
    cache: Arc<BestTransactionsCache<T>>,
    mut receiver: broadcast::Receiver<PendingTransaction<T>>,
) {
    loop {
        match receiver.recv().await {
            Ok(tx) => {
                cache.on_new_transaction(tx);
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // Missed some transactions due to slow processing
                // This is acceptable - we'll get future ones
                tracing::warn!(target: "txpool", missed = n, "Dynamic cache updater lagged");
            }
            Err(broadcast::error::RecvError::Closed) => {
                // Channel closed, pool is shutting down
                break;
            }
        }
    }
}

/// Dynamic iterator that pulls from the shared lock-free cache.
///
/// Multiple iterators can safely share the same cache - all operations
/// are lock-free and use atomic CAS internally.
pub struct DynamicBestTransactions<T: TransactionOrdering> {
    cache: Arc<BestTransactionsCache<T>>,
    invalid: HashSet<SenderId>,
    skip_blobs: bool,
}

impl<T: TransactionOrdering> fmt::Debug for DynamicBestTransactions<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicBestTransactions")
            .field("cache", &self.cache)
            .field("invalid", &self.invalid)
            .field("skip_blobs", &self.skip_blobs)
            .finish()
    }
}

impl<T: TransactionOrdering> Iterator for DynamicBestTransactions<T> {
    type Item = Arc<ValidPoolTransaction<T::Transaction>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cache.pop_best(&self.invalid, self.skip_blobs)
    }
}

impl<T: TransactionOrdering> BestTransactions for DynamicBestTransactions<T>
where
    T::Transaction: Send + Sync,
{
    fn mark_invalid(&mut self, tx: &Self::Item, _kind: &InvalidPoolTransactionError) {
        self.invalid.insert(tx.sender_id());
    }

    fn no_updates(&mut self) {
        // No-op for dynamic iterator - the cache is updated by a separate task.
        // Unlike the snapshot-based iterator, we can't stop receiving updates
        // since they're handled by the background task.
    }

    fn set_skip_blobs(&mut self, skip_blobs: bool) {
        self.skip_blobs = skip_blobs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::{MockOrdering, MockTransaction, MockTransactionFactory},
        Priority,
    };

    #[test]
    fn test_cache_basic_operations() {
        let cache = BestTransactionsCache::<MockOrdering>::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.len_independent(), 0);
    }

    #[test]
    fn test_cache_add_independent_transaction() {
        let cache = BestTransactionsCache::<MockOrdering>::new();
        let mut factory = MockTransactionFactory::default();

        // Create a transaction
        let tx = factory.validated_arc(MockTransaction::eip1559());
        let pending_tx = PendingTransaction {
            submission_id: 0,
            transaction: tx.clone(),
            priority: Priority::Value(0),
        };

        // Add to cache
        cache.on_new_transaction(pending_tx);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.len_independent(), 1);
    }

    #[test]
    fn test_cache_pop_best() {
        let cache = Arc::new(BestTransactionsCache::<MockOrdering>::new());
        let mut factory = MockTransactionFactory::default();

        // Create and add a transaction
        let tx = factory.validated_arc(MockTransaction::eip1559());
        let pending_tx = PendingTransaction {
            submission_id: 0,
            transaction: tx.clone(),
            priority: Priority::Value(0),
        };
        cache.on_new_transaction(pending_tx);

        // Pop it
        let invalid = HashSet::new();
        let popped = cache.pop_best(&invalid, false);

        assert!(popped.is_some());
        assert_eq!(popped.unwrap().hash(), tx.hash());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_nonce_promotion() {
        let cache = Arc::new(BestTransactionsCache::<MockOrdering>::new());
        let mut factory = MockTransactionFactory::default();

        // Create a base transaction - must reuse the same base for same sender
        let base_tx = MockTransaction::eip1559();

        // Create sender's first transaction (nonce 0)
        let tx1 = factory.validated_arc(base_tx.clone());

        // Create sender's second transaction (nonce 1) from same base
        let tx2 = factory.validated_arc(base_tx.inc_nonce());

        let pending_tx1 = PendingTransaction {
            submission_id: 0,
            transaction: tx1.clone(),
            priority: Priority::Value(0),
        };
        let pending_tx2 = PendingTransaction {
            submission_id: 1,
            transaction: tx2.clone(),
            priority: Priority::Value(0),
        };

        // Add both transactions
        cache.on_new_transaction(pending_tx1);
        cache.on_new_transaction(pending_tx2);

        assert_eq!(cache.len(), 2);
        // Only first tx is independent
        assert_eq!(cache.len_independent(), 1);

        // Pop first transaction
        let invalid = HashSet::new();
        let popped1 = cache.pop_best(&invalid, false);
        assert!(popped1.is_some());
        assert_eq!(popped1.unwrap().hash(), tx1.hash());

        // Second transaction should now be independent and poppable
        let popped2 = cache.pop_best(&invalid, false);
        assert!(popped2.is_some());
        assert_eq!(popped2.unwrap().hash(), tx2.hash());

        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_skip_invalid_sender() {
        let cache = Arc::new(BestTransactionsCache::<MockOrdering>::new());
        let mut factory = MockTransactionFactory::default();

        // Create a transaction
        let tx = factory.validated_arc(MockTransaction::eip1559());
        let sender_id = tx.sender_id();

        let pending_tx = PendingTransaction {
            submission_id: 0,
            transaction: tx.clone(),
            priority: Priority::Value(0),
        };
        cache.on_new_transaction(pending_tx);

        // Mark sender as invalid
        let mut invalid = HashSet::new();
        invalid.insert(sender_id);

        // Pop should return None (only tx is from invalid sender)
        let popped = cache.pop_best(&invalid, false);
        assert!(popped.is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = BestTransactionsCache::<MockOrdering>::new();
        let mut factory = MockTransactionFactory::default();

        // Add some transactions
        for _ in 0..5 {
            let tx = factory.validated_arc(MockTransaction::eip1559().rng_hash());
            let pending_tx = PendingTransaction {
                submission_id: 0,
                transaction: tx,
                priority: Priority::Value(0),
            };
            cache.on_new_transaction(pending_tx);
        }

        assert!(!cache.is_empty());

        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.len_independent(), 0);
    }

    #[test]
    fn test_iterator_best_transactions_trait() {
        let cache = Arc::new(BestTransactionsCache::<MockOrdering>::new());
        let mut factory = MockTransactionFactory::default();

        let tx = factory.validated_arc(MockTransaction::eip1559());
        let pending_tx = PendingTransaction {
            submission_id: 0,
            transaction: tx.clone(),
            priority: Priority::Value(0),
        };
        cache.on_new_transaction(pending_tx);

        let mut iter = cache.best();

        // Test skip_blobs
        iter.set_skip_blobs(false);
        assert!(!iter.skip_blobs);
        iter.set_skip_blobs(true);
        assert!(iter.skip_blobs);

        // Reset and test no_updates (no-op for dynamic iterator)
        iter.set_skip_blobs(false);
        iter.no_updates(); // Should not panic
    }
}
