use honba_core::{Exchange, InstrumentId};
use honba_feed::ports::{TickReader, TickRequest};
use honba_feed::wire::{AggressorSide, UnixNanos};

use crate::ticks_file::parse;

const HEAD: &str = "{\"honba_ticks\":1}\n";
const QUOTE: &str = "{\"kind\":\"quote\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":200,\"bid_price\":99.9,\"ask_price\":100.1,\"bid_size\":500,\"ask_size\":400}\n";
const TRADE: &str = "{\"kind\":\"trade\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":100,\"ts_init\":150,\"price\":100.0,\"size\":50,\"aggressor\":\"buyer\",\"trade_id\":\"T-1\"}\n";

fn tcs() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "TCS")
}

fn err(text: &str) -> String {
    format!("{:#}", parse(text).expect_err("expected an error"))
}

#[tokio::test]
async fn parses_quotes_and_trades_and_defaults_ts_init() {
    let reader = parse(&format!("{HEAD}\n{QUOTE}{TRADE}")).unwrap();
    let req = TickRequest::new(tcs(), None, None).unwrap();
    let quotes = reader.read_quotes(&req).await.unwrap();
    let trades = reader.read_trades(&req).await.unwrap();
    assert_eq!(quotes.len(), 1);
    assert_eq!(quotes[0].bid_price(), 99.9);
    assert_eq!(quotes[0].ts_init(), UnixNanos::from_u64(200));
    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].ts_init(), UnixNanos::from_u64(150));
    assert_eq!(trades[0].aggressor_side(), AggressorSide::Buyer);
}

#[test]
fn rejects_a_missing_or_wrong_header_with_the_line() {
    assert!(err("").contains("empty"));
    assert!(err(QUOTE).contains("line 1"));
    let e = err("{\"honba_ticks\":2}\n");
    assert!(e.contains("line 1") && e.contains("version 2"), "{e}");
    assert!(err(HEAD).contains("no ticks"));
}

#[test]
fn a_bad_line_names_its_line_number() {
    let e = err(&format!("{HEAD}{QUOTE}not json\n"));
    assert!(e.contains("line 3"), "{e}");
    let e = err(&format!("{HEAD}\n{QUOTE}{{\"kind\":\"depth\"}}\n"));
    assert!(e.contains("line 4"), "{e}");
}

#[test]
fn unknown_fields_and_invariant_violations_are_refused() {
    let extra = QUOTE.replace("\"ts_event\":200", "\"ts_event\":200,\"bogus\":1");
    assert!(err(&format!("{HEAD}{extra}")).contains("line 2"));
    let crossed = QUOTE.replace("\"bid_price\":99.9", "\"bid_price\":101.0");
    let e = err(&format!("{HEAD}{crossed}"));
    assert!(e.contains("line 2") && e.contains("invalid quote"), "{e}");
    let neg = TRADE.replace("\"size\":50", "\"size\":-1");
    assert!(err(&format!("{HEAD}{neg}")).contains("invalid trade"));
}
