#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(unsafe_code)]

//! Test fixtures and helpers for the Honba platform.

pub mod assert;
pub mod feed;
pub mod fixtures;
pub mod ports;
pub mod recorder;
pub mod scripted_gateway;

pub use assert::{assert_close, assert_close_slice};
pub use feed::VecFeed;
pub use ports::{
    Clock, CONTRACT_PROBE_KEY, CONTRACT_PROBE_SECRET, ExecutionGateway, FeedItem, FixedClock,
    InstrumentInfo, InstrumentKind, InstrumentMaster, MapInstrumentMaster, MarketDataFeed,
    PortError, PortResult, RecordingSink, SecretStore, Sink, StubGateway, StubSecretStore,
    VecMessageFeed, check_clock_contract, check_feed_contract, check_gateway_contract,
    check_master_contract, check_secret_contract, check_sink_contract,
};
pub use recorder::{Handler, HandlerError, HandlerResult, Recorder};
pub use scripted_gateway::ScriptedGateway;

pub mod mock_broker;
pub use mock_broker::MockBrokerExecutor;

#[cfg(test)]
mod tests;
