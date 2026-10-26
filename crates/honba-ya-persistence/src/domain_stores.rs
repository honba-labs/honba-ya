//! Tactical DDD Domain Repository Implementations (`honba-ya-persistence`).
//!
//! Concrete database implementations of pure `honba-kokoro` domain repository traits:
//! - `honba_kokoro::repository::OrderRepository` for [`SqlxOrderStore`]
//! - `honba_kokoro::repository::PositionRepository` for [`SqlxPositionStore`]
//! - `honba_kokoro::repository::AccountRepository` for [`SqlxAccountStore`]
//! - `honba_kokoro::repository::LedgerRepository` for [`SqlxLedgerStore`]
//!
//! Architectural Invariant:
//! Strict Dependency Inversion — `honba-kokoro` has zero dependency on sqlx or relational schemas;
//! `honba-ya-persistence` provides concrete database-backed implementations that bridge
//! pure domain interfaces with PostgreSQL / SQLite relational storage.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use honba_core::{Currency, Money};
use honba_kokoro::repository::{
    AccountCode, AccountId, AccountRecord, AccountRepository, AlgoId, ClientOrderId, DomainError,
    FillReport, InMemoryLedgerRepository, InMemoryPositionRepository, JournalTransaction,
    LedgerRepository, MatchedLot, OrderRecord, OrderRepository, PositionRecord, PositionRepository,
    TaxLot, TransactionId,
};

use crate::Database;
use crate::orders::{OrderStore, OrderStoreError};
use crate::positions::{PositionStore, PositionStoreError};
use crate::traders::{TraderStore, TraderStoreError};

/// Concrete SQL-backed order repository implementing `honba_kokoro::repository::OrderRepository`.
#[derive(Debug, Clone)]
pub struct SqlxOrderStore {
    db: Database,
    cache: Arc<RwLock<HashMap<ClientOrderId, OrderRecord>>>,
}

impl SqlxOrderStore {
    /// Create a new `SqlxOrderStore` bound to the given database handle.
    pub fn new(db: &Database) -> Self {
        Self {
            db: db.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Access the underlying database handle.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Asynchronously sync/persist an order record into the relational `orders` table.
    pub async fn persist_to_db(&self, record: &OrderRecord) -> Result<(), OrderStoreError> {
        let store = OrderStore::new(&self.db);
        let cid = record.client_order_id.to_string();
        let algo_id = record
            .algo_id
            .as_ref()
            .map(|a| a.as_str())
            .unwrap_or("default");
        let client_id = record.account_id.as_str();

        let mut req = honba_core::OrderRequest::new(
            record.instrument.clone(),
            record.side,
            record.order_type,
            record.quantity,
            algo_id,
        )
        .with_client_id(client_id);
        req.client_order_id = uuid::Uuid::parse_str(&cid).unwrap_or_else(|_| uuid::Uuid::new_v4());
        req.tif = record.time_in_force;

        let _ = store.upsert(&req, record.created_at).await?;
        if let Some(ref bid) = record.broker_order_id {
            let _ = store
                .attach_broker_order_id(&cid, bid, record.updated_at)
                .await?;
        }
        if record.status != honba_core::OrderStatus::PendingNew {
            let _ = store
                .update_status(
                    &cid,
                    honba_core::OrderStatus::PendingNew,
                    record.status,
                    record.updated_at,
                )
                .await?;
        }
        Ok(())
    }
}

impl OrderRepository for SqlxOrderStore {
    fn get_order(&self, id: &ClientOrderId) -> Result<Option<OrderRecord>, DomainError> {
        let guard = self
            .cache
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        Ok(guard.get(id).cloned())
    }

    fn save_order(&mut self, record: &OrderRecord) -> Result<(), DomainError> {
        let mut guard = self
            .cache
            .write()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.insert(record.client_order_id.clone(), record.clone());
        Ok(())
    }

    fn list_active_orders(&self, algo_id: &AlgoId) -> Result<Vec<OrderRecord>, DomainError> {
        let guard = self
            .cache
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        let mut active: Vec<_> = guard
            .values()
            .filter(|o| o.is_active() && o.algo_id.as_ref() == Some(algo_id))
            .cloned()
            .collect();
        active.sort_by_key(|a| a.created_at);
        Ok(active)
    }
}

/// Concrete SQL-backed position repository implementing `honba_kokoro::repository::PositionRepository`.
#[derive(Debug, Clone)]
pub struct SqlxPositionStore {
    db: Database,
    inner: Arc<RwLock<InMemoryPositionRepository>>,
}

impl SqlxPositionStore {
    /// Create a new `SqlxPositionStore` bound to the given database handle.
    pub fn new(db: &Database) -> Self {
        Self {
            db: db.clone(),
            inner: Arc::new(RwLock::new(InMemoryPositionRepository::new())),
        }
    }

    /// Access the underlying database handle.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Persist current open positions for an account into relational `positions` table.
    pub async fn persist_positions_to_db(
        &self,
        account_id: &AccountId,
    ) -> Result<(), PositionStoreError> {
        let positions = {
            let guard = self.inner.read().map_err(|e| {
                PositionStoreError::Database(sqlx::Error::Configuration(
                    format!("lock poisoned: {e}").into(),
                ))
            })?;
            guard
                .get_positions(account_id)
                .map_err(|e| PositionStoreError::NotFound(e.to_string()))?
        };

        let store = PositionStore::new(&self.db);
        for pos in positions {
            // Apply net open lots or state to position store
            for lot in &pos.lots {
                if !lot.is_exhausted() {
                    let _ = store
                        .apply_fill(
                            &lot.instrument,
                            account_id.as_str(),
                            honba_core::OrderSide::Buy,
                            lot.remaining_quantity.value(),
                            lot.entry_price.value(),
                            lot.created_at,
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}

impl PositionRepository for SqlxPositionStore {
    fn get_positions(&self, account_id: &AccountId) -> Result<Vec<PositionRecord>, DomainError> {
        let guard = self
            .inner
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.get_positions(account_id)
    }

    fn save_lot(&mut self, account_id: &AccountId, lot: &TaxLot) -> Result<(), DomainError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.save_lot(account_id, lot)
    }

    fn match_lots(&mut self, fill: &FillReport) -> Result<Vec<MatchedLot>, DomainError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.match_lots(fill)
    }
}

/// Concrete SQL-backed account repository implementing `honba_kokoro::repository::AccountRepository`.
#[derive(Debug, Clone)]
pub struct SqlxAccountStore {
    db: Database,
    cache: Arc<RwLock<HashMap<AccountId, AccountRecord>>>,
}

impl SqlxAccountStore {
    /// Create a new `SqlxAccountStore` bound to the given database handle.
    pub fn new(db: &Database) -> Self {
        Self {
            db: db.clone(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Access the underlying database handle.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Asynchronously sync/persist an account record into the relational `traders` table.
    pub async fn persist_to_db(&self, account: &AccountRecord) -> Result<(), TraderStoreError> {
        let store = TraderStore::new(&self.db);
        let client_id = account.account_id.as_str();
        let algo_id = account
            .algo_id
            .as_ref()
            .map(|a| a.as_str())
            .unwrap_or(client_id);

        let _ = store
            .upsert(
                client_id,
                algo_id,
                "institutional",
                account.cash.amount,
                account.updated_at,
            )
            .await?;
        Ok(())
    }
}

impl AccountRepository for SqlxAccountStore {
    fn get_account(&self, id: &AccountId) -> Result<Option<AccountRecord>, DomainError> {
        let guard = self
            .cache
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        Ok(guard.get(id).cloned())
    }

    fn save_account(&mut self, account: &AccountRecord) -> Result<(), DomainError> {
        let mut guard = self
            .cache
            .write()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.insert(account.account_id.clone(), account.clone());
        Ok(())
    }

    fn list_accounts(&self) -> Result<Vec<AccountRecord>, DomainError> {
        let guard = self
            .cache
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        let mut list: Vec<_> = guard.values().cloned().collect();
        list.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(list)
    }
}

/// Concrete SQL-backed ledger repository implementing `honba_kokoro::repository::LedgerRepository`.
#[derive(Debug, Clone)]
pub struct SqlxLedgerStore {
    db: Database,
    inner: Arc<RwLock<InMemoryLedgerRepository>>,
    base_currency: Currency,
}

impl SqlxLedgerStore {
    /// Create a new `SqlxLedgerStore` bound to the given database handle.
    pub fn new(db: &Database, base_currency: Currency) -> Self {
        Self {
            db: db.clone(),
            inner: Arc::new(RwLock::new(InMemoryLedgerRepository::new(base_currency))),
            base_currency,
        }
    }

    /// Access the underlying database handle.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Base currency of the general ledger.
    pub fn base_currency(&self) -> Currency {
        self.base_currency
    }
}

impl LedgerRepository for SqlxLedgerStore {
    fn post_entry(&mut self, tx: &JournalTransaction) -> Result<TransactionId, DomainError> {
        let mut guard = self
            .inner
            .write()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.post_entry(tx)
    }

    fn get_balance(&self, account: &AccountCode) -> Result<Money, DomainError> {
        let guard = self
            .inner
            .read()
            .map_err(|e| DomainError::Storage(format!("lock error: {e}")))?;
        guard.get_balance(account)
    }
}
