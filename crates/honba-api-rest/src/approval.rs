use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use honba_api::OrdersRequest;
use honba_messages::{ErrorCode, ErrorDetail, OrderId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The approval queue for order placements (E5-S4).
///
/// Sits between the REST handler and the risk stage. Placements are held here
/// until approved by an operator. Cancel and modify bypass the queue.
pub struct ApprovalQueue {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    items: VecDeque<QueuedOrder>,
    max_size: usize,
}

struct QueuedOrder {
    id: OrderId,
    request: OrdersRequest,
    queued_at: Instant,
    status: QueueStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Pending,
    Approved,
    Rejected,
    Expired,
}

impl ApprovalQueue {
    /// Creates a new approval queue with the given maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                items: VecDeque::new(),
                max_size,
            })),
        }
    }

    /// Returns the maximum size of the queue.
    pub fn max_size(&self) -> usize {
        self.inner.lock().unwrap().max_size
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues a new order for approval.
    ///
    /// Returns the queue position (1-indexed) on success, or an error if the queue is full.
    pub fn enqueue(&self, order_id: OrderId, request: OrdersRequest) -> Result<usize, ErrorDetail> {
        let mut inner = self.lock();
        if inner.items.len() >= inner.max_size {
            return Err(
                ErrorDetail::new(ErrorCode::RateLimited, "approval queue is full").with_context(
                    json!({
                        "reason": "approval_queue_full",
                        "max_size": inner.max_size
                    }),
                ),
            );
        }
        let position = inner.items.len() + 1;
        inner.items.push_back(QueuedOrder {
            id: order_id,
            request,
            queued_at: Instant::now(),
            status: QueueStatus::Pending,
        });
        Ok(position)
    }

    /// Get the pending queue (for display).
    pub fn pending(&self) -> Vec<Value> {
        let inner = self.lock();
        inner
            .items
            .iter()
            .map(|q| {
                json!({
                    "queue_id": q.id.as_str(),
                    "instrument_id": q.request.instrument_id.as_ref().map(|x| x.to_string()),
                    "side": q.request.side.as_deref(),
                    "qty": q.request.qty,
                    "price": q.request.price,
                    "status": q.status,
                    "queued_at": q.queued_at.elapsed().as_secs_f64(),
                })
            })
            .collect()
    }

    /// Approve the oldest pending order.
    ///
    /// Returns the approved order request, or None if no pending orders.
    /// The approved order is removed from the queue.
    pub fn approve_oldest(&self) -> Option<OrdersRequest> {
        let mut inner = self.lock();
        let pos = inner
            .items
            .iter()
            .position(|q| q.status == QueueStatus::Pending)?;
        inner
            .items
            .remove(pos)
            .map(|item: QueuedOrder| item.request)
    }

    /// Reject the oldest pending order.
    ///
    /// Returns the rejected order request, or None if no pending orders.
    /// The rejected order is removed from the queue.
    pub fn reject_oldest(&self) -> Option<OrdersRequest> {
        let mut inner = self.lock();
        let pos = inner
            .items
            .iter()
            .position(|q| q.status == QueueStatus::Pending)?;
        inner
            .items
            .remove(pos)
            .map(|item: QueuedOrder| item.request)
    }

    /// Approve a specific order by queue_id.
    pub fn approve(&self, queue_id: &OrderId) -> Option<OrdersRequest> {
        let mut inner = self.lock();
        inner
            .items
            .iter_mut()
            .find(|q| q.id == *queue_id && q.status == QueueStatus::Pending)
            .map(|q| {
                q.status = QueueStatus::Approved;
                q.request.clone()
            })
    }

    /// Reject a specific order by queue_id.
    pub fn reject(&self, queue_id: &OrderId) -> Option<OrdersRequest> {
        let mut inner = self.lock();
        inner
            .items
            .iter_mut()
            .find(|q| q.id == *queue_id && q.status == QueueStatus::Pending)
            .map(|q| {
                q.status = QueueStatus::Rejected;
                q.request.clone()
            })
    }

    /// Remove expired entries (e.g. older than some TTL).
    pub fn cleanup_expired(&self, ttl: Duration) -> usize {
        let mut inner = self.lock();
        let now = Instant::now();
        let mut removed = 0;
        inner.items.retain(|q| {
            if now.duration_since(q.queued_at) > ttl {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }
}

impl Default for ApprovalQueue {
    fn default() -> Self {
        Self::new(100) // default max size
    }
}
