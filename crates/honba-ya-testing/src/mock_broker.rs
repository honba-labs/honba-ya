//! Simple mock [`BrokerExecutor`] for tests.

use async_trait::async_trait;
use honba_core::{OrderAck, OrderRequest, OrderStatus};
use honba_te_core::{BrokerError, BrokerExecutor, BrokerHealth};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A minimal mock broker executor for testing.
///
/// If `should_succeed` is true, all orders are accepted and filled immediately.
/// If false, all orders are rejected.
#[derive(Clone)]
pub struct MockBrokerExecutor {
    inner: Arc<MockBrokerInner>,
}

struct MockBrokerInner {
    should_succeed: bool,
    orders: Mutex<Vec<OrderRequest>>,
}

impl MockBrokerExecutor {
    /// Creates a new mock broker.
    ///
    /// `should_succeed` determines whether `place_order` returns `Ok` or `Err`.
    pub fn new(should_succeed: bool) -> Self {
        Self {
            inner: Arc::new(MockBrokerInner {
                should_succeed,
                orders: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns all orders that have been placed through this mock.
    pub async fn placed_orders(&self) -> Vec<OrderRequest> {
        self.inner.orders.lock().await.clone()
    }
}

#[async_trait]
impl BrokerExecutor for MockBrokerExecutor {
    fn name(&self) -> &str {
        "mock"
    }

    async fn health_check(&self) -> Result<BrokerHealth, honba_te_core::BrokerError> {
        Ok(BrokerHealth::session_ok("mock", "healthy"))
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, BrokerError> {
        self.inner.orders.lock().await.push(req.clone());

        if !self.inner.should_succeed {
            return Err(BrokerError::Rejected(
                "mock broker configured to fail".into(),
            ));
        }

        let ack = OrderAck::new(
            format!("MOCK-{}", honba_core::uuid::Uuid::new_v4()),
            req.client_order_id,
            OrderStatus::New,
            chrono::Utc::now(),
        );
        Ok(ack)
    }

    async fn cancel_order(&self, _order_id: &str) -> Result<(), BrokerError> {
        if !self.inner.should_succeed {
            return Err(BrokerError::Rejected(
                "mock broker configured to fail".into(),
            ));
        }
        Ok(())
    }

    async fn get_order_status(
        &self,
        _order_id: &str,
    ) -> Result<honba_core::OrderStatus, BrokerError> {
        Ok(OrderStatus::New)
    }
}
