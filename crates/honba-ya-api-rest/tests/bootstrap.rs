//! End-to-end server bootstrap tests over a Parquet data directory, bound to an ephemeral
//! loopback port, called with a plain TCP client, and stopped through graceful shutdown.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use honba_api_rest::{
    ServeConfig, ServeOutcome, import_ticks_ndjson, parse_fill_model, parse_trading_state,
    prepare_server,
};
use honba_config::FillModel;
use honba_feed::import::parquet_source::bar_schema;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use tokio::sync::oneshot;

fn data_dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(bar_schema());
    let col = |v: f64| -> ArrayRef { Arc::new(Float64Array::from(vec![v; 2])) };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![60_000_000_000_i64, 120_000_000_000])),
        col(10.0),
        col(12.0),
        col(9.0),
        col(11.0),
        col(100.0),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let file = File::create(dir.join("TCS.NSE.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    dir
}

struct RunningServer {
    addr: SocketAddr,
    warning: Option<String>,
    stop: Option<oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<anyhow::Result<ServeOutcome>>>,
}

impl RunningServer {
    fn start(mut cfg: ServeConfig) -> Self {
        // Isolate per-test profile/portfolio/journal directories inside data_dir unless customized.
        if cfg.runs.journals_dir == PathBuf::from("data/journals") {
            cfg.runs.journals_dir = cfg.data_dir.join("_journals");
        }
        if cfg.runs.profiles_dir == PathBuf::from("data/profiles") {
            cfg.runs.profiles_dir = cfg.data_dir.join("_profiles");
        }
        if cfg.runs.portfolios_dir == PathBuf::from("data/portfolios") {
            cfg.runs.portfolios_dir = cfg.data_dir.join("_portfolios");
        }
        let prepared = prepare_server(&cfg).expect("server preparation must succeed");
        let (bound_tx, bound_rx) = std::sync::mpsc::channel::<(SocketAddr, Option<String>)>();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let join = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(prepared.serve_with_shutdown(
                move |addr, warn| {
                    let _ = bound_tx.send((addr, warn.map(str::to_owned)));
                },
                async move {
                    let _ = stop_rx.await;
                },
            ))
        });

        let (addr, warning) = bound_rx.recv().expect("server must bind and announce");
        Self {
            addr,
            warning,
            stop: Some(stop_tx),
            join: Some(join),
        }
    }

    fn shutdown(mut self) -> ServeOutcome {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        self.join
            .take()
            .unwrap()
            .join()
            .expect("server thread panicked")
            .expect("graceful shutdown must succeed")
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn base_config(data_dir: &Path) -> ServeConfig {
    let mut cfg = ServeConfig::new(data_dir);
    cfg.addr = "127.0.0.1:0".parse().unwrap();
    cfg
}

fn get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn head_with_origin(addr: SocketAddr, origin: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(
        stream,
        "GET /health HTTP/1.0\r\nHost: {addr}\r\nOrigin: {origin}\r\n\r\n"
    )
    .unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    text.split_once("\r\n\r\n").unwrap().0.to_ascii_lowercase()
}

fn post(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(
        stream,
        "POST {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn put(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(
        stream,
        "PUT {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

#[test]
fn serve_answers_health_and_instruments_then_exits_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "ok");
    let srv = RunningServer::start(base_config(&dir));

    let (status, body) = get(srv.addr, "/health");
    assert_eq!((status, &body["data"]["status"]), (200, &Value::from("ok")));
    let (status, body) = get(srv.addr, "/instruments");
    assert_eq!(status, 200);
    assert_eq!(body["data"]["instruments"][0]["id"], "NSE:TCS");
    let (status, body) = get(srv.addr, "/quotes?symbols=TCS");
    assert_eq!(status, 200);
    assert_eq!(body["data"]["quotes"][0]["bid"], "11");

    let outcome = srv.shutdown();
    assert_eq!(outcome.bound_addr.ip().to_string(), "127.0.0.1");
}

#[test]
fn a_missing_data_dir_fails_before_listening() {
    let cfg = base_config(Path::new("/nonexistent/honba-bars"));
    let err = format!("{:#}", prepare_server(&cfg).err().expect("must fail"));
    assert!(err.contains("data directory"), "{err}");
}

#[test]
fn cors_is_off_by_default_and_opt_in_per_origin() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "cors");

    let srv = RunningServer::start(base_config(&dir));
    let head = head_with_origin(srv.addr, "https://app.example");
    assert!(!head.contains("access-control-allow-origin"), "{head}");
    srv.shutdown();

    let mut cfg = base_config(&dir);
    cfg.cors_origins = vec!["https://app.example".to_string()];
    let srv = RunningServer::start(cfg);
    let head = head_with_origin(srv.addr, "https://app.example");
    assert!(
        head.contains("access-control-allow-origin: https://app.example"),
        "{head}"
    );
    let head = head_with_origin(srv.addr, "https://evil.example");
    assert!(!head.contains("access-control-allow-origin"), "{head}");
    srv.shutdown();
}

#[test]
fn an_invalid_cors_origin_fails_before_listening() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "badcors");
    let mut cfg = base_config(&dir);
    cfg.cors_origins = vec!["bad\norigin".to_string()];
    let err = format!("{:#}", prepare_server(&cfg).err().expect("must fail"));
    assert!(err.contains("cors"), "{err}");
}

#[test]
fn a_non_loopback_address_warns_and_loopback_does_not() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "warn");

    let mut wild = base_config(&dir);
    wild.addr = "0.0.0.0:0".parse().unwrap();
    let srv = RunningServer::start(wild);
    let err = srv.warning.clone().expect("must warn on 0.0.0.0");
    assert!(
        err.contains("warning") && err.contains("auth") && err.contains("TLS"),
        "{err}"
    );
    srv.shutdown();

    let srv = RunningServer::start(base_config(&dir));
    assert!(srv.warning.is_none());
    srv.shutdown();
}

#[test]
fn serve_runs_backtests_into_the_journals_dir_and_stops_with_no_open_run() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "runs");
    let journals = dir.join("journals");
    let mut cfg = base_config(&dir);
    cfg.runs.journals_dir = journals.clone();
    cfg.runs.max_concurrent_runs = Some(1);
    cfg.runs.shutdown_grace_secs = 0;

    let srv = RunningServer::start(cfg);

    let (status, body) = get(srv.addr, "/capabilities");
    assert_eq!(status, 200);
    let flagged = body["data"]["capabilities"]["not_implemented"].to_string();
    assert!(!flagged.contains("/backtests"), "{flagged}");
    assert!(!flagged.contains("/journals"), "{flagged}");
    let (status, body) = post(srv.addr, "/backtests", "{}");
    assert_eq!(status, 422, "{body}");
    let (status, body) = get(srv.addr, "/backtests/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    assert_eq!(status, 404, "{body}");
    assert!(!journals.exists(), "rejected requests must write nothing");

    let (status, body) = post(
        srv.addr,
        "/backtests",
        r#"{"strategy":"buy_and_hold","universe":"TCS.NSE","start":"1970-01-01","end":"1970-01-02","bar_spec":"1m","seed":1}"#,
    );
    assert_eq!(status, 200, "{body}");
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let manifest = journals.join(&id).join("manifest.json");
    assert!(
        manifest.exists(),
        "the run is journaled under --journals-dir"
    );

    srv.shutdown();
    let text = std::fs::read_to_string(manifest).unwrap();
    let status = serde_json::from_str::<Value>(&text).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        ["completed", "failed", "cancelled"].contains(&status.as_str()),
        "left {status}"
    );
}

#[test]
fn trading_state_gates_post_orders() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "trading-state");
    let order = r#"{"instrument_id":{"symbol":"TCS","exchange":"NSE"},"side":"buy","qty":1.0}"#;

    let srv = RunningServer::start(base_config(&dir));
    let (status, body) = post(srv.addr, "/orders", order);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["status"], Value::from("submitted"));
    srv.shutdown();

    let mut halted = base_config(&dir);
    halted.trading_state = parse_trading_state("halted").unwrap();
    let srv = RunningServer::start(halted);
    let (status, body) = post(srv.addr, "/orders", order);
    assert_eq!(status, 422, "{body}");
    assert_eq!(body["error"]["code"], Value::from("conflict"));
    srv.shutdown();
}

#[test]
fn operator_journal_records_a_trading_state_change() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "operator-journal");
    let journal = dir.join("operator.ndjson");

    let mut cfg = base_config(&dir);
    cfg.runs.operator_journal = Some(journal.clone());
    let srv = RunningServer::start(cfg);
    let (status, body) = put(
        srv.addr,
        "/trading-state",
        r#"{"state":"halted","reason":"e2e"}"#,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["data"],
        serde_json::json!({"state": "halted", "previous": "active"})
    );
    srv.shutdown();

    let text = std::fs::read_to_string(&journal).unwrap();
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 1, "{text}");
    assert_eq!(lines[0]["event"], Value::from("trading_state.change"));
    assert_eq!(lines[0]["from"], Value::from("active"));
    assert_eq!(lines[0]["to"], Value::from("halted"));
    assert_eq!(lines[0]["reason"], Value::from("e2e"));
}

fn bar_body(ts: u64, close: f64) -> String {
    use honba_core::{Exchange, InstrumentId, Price, Quantity};
    let instrument = InstrumentId::new(Exchange::Nse, "TCS");
    let ts_dt = chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(ts as i64);
    let bar = honba_core::Bar {
        instrument,
        open: Price::from_rupees(&close.to_string()).unwrap(),
        high: Price::from_rupees(&close.to_string()).unwrap(),
        low: Price::from_rupees(&close.to_string()).unwrap(),
        close: Price::from_rupees(&close.to_string()).unwrap(),
        volume: Quantity::from_u64(10),
        timestamp: ts_dt,
    };
    serde_json::json!({ "bars": [bar] }).to_string()
}

#[test]
fn bar_ingest_flag_turns_on_post_bars_and_the_quote_follows() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "bar-ingest");

    let srv = RunningServer::start(base_config(&dir));
    let (status, body) = post(srv.addr, "/bars", &bar_body(180_000_000_000, 77.0));
    assert_eq!(status, 503, "{body}");
    assert_eq!(
        body["error"]["context"]["reason"],
        Value::from("bar_ingest_disabled")
    );
    srv.shutdown();

    let mut cfg = base_config(&dir);
    cfg.ingest.bar_ingest = true;
    cfg.ingest.bar_ingest_capacity = 16;
    let srv = RunningServer::start(cfg);
    let (status, body) = post(srv.addr, "/bars", &bar_body(180_000_000_000, 77.0));
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["accepted"], Value::from(1));

    let mut bid = Value::Null;
    for _ in 0..100 {
        bid = get(srv.addr, "/quotes?symbols=TCS").1["data"]["quotes"][0]["bid"].clone();
        if bid == "77" {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(bid, "77");

    let outcome = srv.shutdown();
    let report = outcome.bar_ingest.expect("bar ingest report");
    assert_eq!((report.bars, report.errors), (1, 0));
}

#[test]
fn risk_config_sets_the_rest_limits_and_defaults_are_unchanged_without_it() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "risk-config");
    let big = r#"{"instrument_id":{"symbol":"TCS","exchange":"NSE"},"side":"buy","qty":1.0,"order_type":"limit","price":100.0}"#;

    let srv = RunningServer::start(base_config(&dir));
    let (status, body) = post(srv.addr, "/orders", big);
    assert_eq!(status, 200, "{body}");
    srv.shutdown();

    let risk_path = tmp.path().join("risk-ok.toml");
    std::fs::write(
        &risk_path,
        "[risk]\nmax_notional = 50.0\nprice_collar_bps = 200\n",
    )
    .unwrap();
    let mut cfg = base_config(&dir);
    cfg.risk.risk_config = Some(risk_path);
    let srv = RunningServer::start(cfg);
    let (status, body) = post(srv.addr, "/orders", big);
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"]["code"],
        Value::from("risk_max_notional_exceeded")
    );
    assert_eq!(
        body["error"]["context"]["reason"],
        Value::from("max_notional_exceeded")
    );
    srv.shutdown();
}

#[test]
fn an_invalid_risk_config_fails_before_listening_and_names_the_field() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "risk-config-bad");
    let risk_path = tmp.path().join("risk-bad.toml");
    std::fs::write(&risk_path, "[risk]\nprice_collar_bps = 0\n").unwrap();
    let mut cfg = base_config(&dir);
    cfg.risk.risk_config = Some(risk_path);
    let err = format!("{:#}", prepare_server(&cfg).err().expect("must fail"));
    assert!(err.contains("price_collar_bps"), "{err}");
}

const TICKS_OK: &str = "{\"honba_ticks\":1}\n\
{\"kind\":\"quote\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":60000000000,\"bid_price\":9.9,\"ask_price\":10.1,\"bid_size\":1000000,\"ask_size\":1000000}\n\
{\"kind\":\"quote\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":120000000000,\"bid_price\":10.9,\"ask_price\":11.1,\"bid_size\":1000000,\"ask_size\":1000000}\n\
{\"kind\":\"trade\",\"symbol\":\"TCS\",\"exchange\":\"NSE\",\"ts_event\":121000000000,\"price\":11.0,\"size\":100,\"aggressor\":\"buyer\",\"trade_id\":\"T-1\"}\n";

const BACKTEST: &str = r#"{"strategy":"buy_and_hold","universe":"TCS.NSE","start":"1970-01-01","end":"1970-01-02","bar_spec":"1m","seed":1}"#;

fn finished_run(addr: SocketAddr, id: &str) -> Value {
    for _ in 0..500 {
        let (_, body) = get(addr, &format!("/backtests/{id}"));
        if matches!(
            body["data"]["status"].as_str(),
            Some("completed" | "failed")
        ) {
            return body;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("run {id} never finished");
}

#[test]
fn fill_model_l2_with_ticks_runs_a_backtest_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "l2-ticks");
    let ticks = tmp.path().join("l2-ticks.ticks.ndjson");
    std::fs::write(&ticks, TICKS_OK).unwrap();

    let mut cfg = base_config(&dir);
    cfg.runs.journals_dir = dir.join("journals");
    cfg.fill.fill_model = Some(FillModel::L2);
    cfg.fill.ticks = Some(ticks);

    let srv = RunningServer::start(cfg);
    let (status, body) = post(srv.addr, "/backtests", BACKTEST);
    assert_eq!(status, 200, "{body}");
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let done = finished_run(srv.addr, &id);
    srv.shutdown();
    assert_eq!(done["data"]["status"], "failed", "{done}");
    assert_eq!(done["data"]["error"]["code"], "not_implemented", "{done}");
}

#[test]
fn fill_model_l2_with_a_parquet_tick_store_runs_a_backtest_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "l2-parquet-ticks");
    let ticks = tmp.path().join("l2-parquet-ticks.ticks.ndjson");
    std::fs::write(&ticks, TICKS_OK).unwrap();
    let store = tmp.path().join("l2-parquet-ticks.store");
    std::fs::create_dir_all(&store).unwrap();

    let (quotes, trades) = import_ticks_ndjson(&ticks, &store).unwrap();
    assert_eq!((quotes, trades), (2, 1));

    let mut cfg = base_config(&dir);
    cfg.runs.journals_dir = dir.join("journals-parquet");
    cfg.fill.fill_model = Some(FillModel::L2);
    cfg.fill.ticks = Some(store);

    let srv = RunningServer::start(cfg);
    let (status, body) = post(srv.addr, "/backtests", BACKTEST);
    assert_eq!(status, 200, "{body}");
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let done = finished_run(srv.addr, &id);
    srv.shutdown();
    assert_eq!(done["data"]["status"], "failed", "{done}");
    assert_eq!(done["data"]["error"]["code"], "not_implemented", "{done}");
}

#[test]
fn fill_model_l2_without_ticks_fails_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "l2-no-ticks");
    let mut cfg = base_config(&dir);
    cfg.fill.fill_model = Some(FillModel::L2);
    let err = format!("{:#}", prepare_server(&cfg).err().expect("must fail"));
    assert!(
        err.contains("--fill-model l2") && err.contains("--ticks"),
        "{err}"
    );
}

#[test]
fn a_bad_ticks_file_fails_at_startup_naming_the_line() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = data_dir(tmp.path(), "bad-ticks");
    let ticks = tmp.path().join("bad-ticks.ticks.ndjson");
    std::fs::write(&ticks, format!("{TICKS_OK}not json\n")).unwrap();
    let mut cfg = base_config(&dir);
    cfg.fill.fill_model = Some(FillModel::L2);
    cfg.fill.ticks = Some(ticks);
    let err = format!("{:#}", prepare_server(&cfg).err().expect("must fail"));
    assert!(err.contains("--ticks") && err.contains("line 5"), "{err}");
}

#[test]
fn an_unknown_fill_model_is_refused_by_the_parser() {
    let err = parse_fill_model("none").unwrap_err();
    assert!(err.contains("bar_fill_through"), "{err}");
}
