//! Unit tests for `crate::handle`.

use std::future::Future;

use async_trait::async_trait;
use honba_engine::{Engine, TradingState};
use honba_messages::{InstrumentId, Message};
use honba_ports::{MarketDataFeed, PortResult};
use honba_testing::VecMessageFeed;

use crate::handle::engine_task;
use crate::{AsyncError, BoxedFeed, Command, EngineHandle};

fn _assert_send<T: Send>() {}
fn _assert_send_future<F: Future + Send>(_: F) {}

#[test]
fn the_handle_is_send() {
    _assert_send::<EngineHandle>();
}

#[test]
fn the_task_future_is_send() {
    let (_tx, rx) = tokio::sync::mpsc::channel(1);
    _assert_send_future(engine_task(
        Engine::new(),
        Box::new(VecMessageFeed::empty()),
        rx,
    ));
}

struct BlockedFeed {
    _gate: tokio::sync::oneshot::Sender<()>,
    released: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl BlockedFeed {
    fn new() -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel();
        Self {
            _gate: tx,
            released: Some(rx),
        }
    }
}

#[async_trait]
impl MarketDataFeed for BlockedFeed {
    async fn subscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn unsubscribe(&mut self, _symbols: &[InstrumentId]) -> PortResult<()> {
        Ok(())
    }

    async fn next(&mut self) -> PortResult<Option<Message>> {
        let released = self.released.take();
        match released {
            Some(released) => {
                let _ = released.await;
            }
            None => std::future::pending().await,
        }
        Ok(None)
    }
}

async fn yield_until_not_running(handle: &EngineHandle) {
    for _ in 0..64 {
        if !handle.is_running() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the engine task was still running after 64 yields");
}

#[tokio::test]
async fn try_submit_into_a_full_channel_is_rejected_rather_than_blocking() {
    let handle = EngineHandle::spawn_with_capacity(Engine::new(), Box::new(BlockedFeed::new()), 1);
    assert!(handle.is_running());

    handle.try_submit(Command::Stop).unwrap();
    let err = handle
        .try_submit(Command::State(TradingState::Halted))
        .unwrap_err();
    assert_eq!(
        err,
        AsyncError::Rejected("the command channel is full".to_string())
    );
}

#[tokio::test]
async fn try_submit_succeeds_while_the_channel_has_room() {
    let handle = EngineHandle::spawn_with_capacity(Engine::new(), Box::new(BlockedFeed::new()), 2);

    handle
        .try_submit(Command::State(TradingState::Halted))
        .unwrap();
    handle
        .try_submit(Command::State(TradingState::Active))
        .unwrap();
}

#[tokio::test]
async fn a_task_whose_feed_ran_out_is_not_running() {
    let handle = EngineHandle::spawn(
        Engine::new(),
        Box::new(VecMessageFeed::empty()) as BoxedFeed,
    );

    assert!(handle.is_running(), "the task has not been polled yet");
    yield_until_not_running(&handle).await;
    assert!(!handle.is_running());
}

#[tokio::test]
async fn a_command_channel_the_task_dropped_reports_closed() {
    let handle =
        EngineHandle::spawn_with_capacity(Engine::new(), Box::new(VecMessageFeed::empty()), 4);
    yield_until_not_running(&handle).await;

    let err = handle.try_submit(Command::Stop).unwrap_err();
    assert_eq!(err, AsyncError::Closed);
    assert_eq!(
        handle.try_submit(Command::Stop).unwrap_err(),
        AsyncError::Closed
    );
}

#[tokio::test]
async fn shutdown_returns_the_tasks_own_result() {
    let handle = EngineHandle::spawn(Engine::new(), Box::new(VecMessageFeed::empty()));
    handle.shutdown().await.unwrap();
}
