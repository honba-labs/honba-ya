//! `honba serve` end to end: the real binary over a Parquet data directory,
//! bound to an ephemeral loopback port, called with a plain TCP client and
//! stopped with SIGINT. Hermetic: loopback only, files under Cargo's scratch dir.
#![cfg(unix)]

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::record_batch::RecordBatch;
use honba_feed::import::parquet_source::bar_schema;
use parquet::arrow::ArrowWriter;
use serde_json::Value;

fn data_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-cli-serve")
        .join(name);
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

/// A spawned `honba serve` that is killed and reaped on drop, so a failed assertion cannot leak it.
struct Server(Child);

impl std::ops::Deref for Server {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Server {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts `honba serve` and returns the child plus the address it announced.
fn start(args: &[&str]) -> (Server, SocketAddr) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_honba"))
        .arg("serve")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map(Server)
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut line)
        .unwrap();
    let addr = line
        .trim()
        .strip_prefix("listening on http://")
        .unwrap_or_else(|| panic!("unexpected banner {line:?}"))
        .parse()
        .unwrap();
    (child, addr)
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

#[test]
fn serve_answers_health_and_instruments_then_exits_cleanly_on_sigint() {
    let dir = data_dir("ok");
    let (mut child, addr) = start(&["--data-dir", dir.to_str().unwrap(), "--addr", "127.0.0.1:0"]);

    let (status, body) = get(addr, "/health");
    assert_eq!((status, &body["data"]["status"]), (200, &Value::from("ok")));
    let (status, body) = get(addr, "/instruments");
    assert_eq!(status, 200);
    assert_eq!(body["data"]["instruments"][0]["id"]["symbol"], "TCS");
    let (status, body) = get(addr, "/quotes?symbols=TCS");
    assert_eq!(status, 200);
    assert_eq!(body["data"]["quotes"][0]["bid_price"], 11.0);

    let sent = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success());
    let exit = child.wait().unwrap();
    assert!(exit.success(), "graceful shutdown exits 0, got {exit:?}");
}

#[test]
fn a_missing_data_dir_fails_before_listening() {
    let output = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args(["serve", "--data-dir", "/nonexistent/honba-bars"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("data directory"));
}

/// Sends `GET /health` with an `Origin` header and returns the raw response head.
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

fn stop(mut child: Server) {
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    child.wait().unwrap();
}

#[test]
fn cors_is_off_by_default_and_opt_in_per_origin() {
    let dir = data_dir("cors");
    let d = dir.to_str().unwrap();
    let (child, addr) = start(&["--data-dir", d, "--addr", "127.0.0.1:0"]);
    let head = head_with_origin(addr, "https://app.example");
    assert!(!head.contains("access-control-allow-origin"), "{head}");
    stop(child);

    let (child, addr) = start(&[
        "--data-dir",
        d,
        "--addr",
        "127.0.0.1:0",
        "--cors-origin",
        "https://app.example",
    ]);
    let head = head_with_origin(addr, "https://app.example");
    assert!(
        head.contains("access-control-allow-origin: https://app.example"),
        "{head}"
    );
    let head = head_with_origin(addr, "https://evil.example");
    assert!(!head.contains("access-control-allow-origin"), "{head}");
    stop(child);
}

#[test]
fn an_invalid_cors_origin_fails_before_listening() {
    let dir = data_dir("badcors");
    let output = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "serve",
            "--data-dir",
            dir.to_str().unwrap(),
            "--cors-origin",
            "bad\norigin",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cors"));
}

#[test]
fn a_non_loopback_address_warns_on_stderr_and_loopback_does_not() {
    let dir = data_dir("warn");
    let d = dir.to_str().unwrap();
    let (mut child, _) = start(&["--data-dir", d, "--addr", "0.0.0.0:0"]);
    // The banner is printed after the warning, so the warning is already written.
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    child.wait().unwrap();
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(
        err.contains("warning") && err.contains("auth") && err.contains("TLS"),
        "{err}"
    );

    let (mut child, _) = start(&["--data-dir", d, "--addr", "127.0.0.1:0"]);
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    child.wait().unwrap();
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(!err.contains("warning"), "{err}");
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

#[test]
fn serve_runs_backtests_into_the_journals_dir_and_stops_with_no_open_run() {
    let dir = data_dir("runs");
    let journals = dir.join("journals");
    let (mut child, addr) = start(&[
        "--data-dir",
        dir.to_str().unwrap(),
        "--addr",
        "127.0.0.1:0",
        "--journals-dir",
        journals.to_str().unwrap(),
        "--max-concurrent-runs",
        "1",
        "--shutdown-grace-secs",
        "0",
    ]);

    let (status, body) = get(addr, "/capabilities");
    assert_eq!(status, 200);
    let flagged = body["data"]["capabilities"]["not_implemented"].to_string();
    assert!(!flagged.contains("/backtests"), "{flagged}");
    assert!(!flagged.contains("/journals"), "{flagged}");
    let (status, body) = post(addr, "/backtests", "{}");
    assert_eq!(status, 422, "{body}");
    let (status, body) = get(addr, "/backtests/01ARZ3NDEKTSV4RRFFQ69G5FAV");
    assert_eq!(status, 404, "{body}");
    assert!(!journals.exists(), "rejected requests must write nothing");

    let (status, body) = post(
        addr,
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

    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(child.wait().unwrap().success());
    // Graceful shutdown leaves no pending or running manifest behind (decision 5).
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
    let dir = data_dir("trading-state");
    let d = dir.to_str().unwrap();
    let order = r#"{"instrument_id":{"symbol":"TCS","exchange":"NSE"},"side":"buy","qty":1.0}"#;

    // Default state is active: an order passes the risk stage and is acknowledged.
    let (child, addr) = start(&["--data-dir", d, "--addr", "127.0.0.1:0"]);
    let (status, body) = post(addr, "/orders", order);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["status"], Value::from("submitted"));
    stop(child);

    // Halted: the same order is refused by the state rule before any shape rule.
    let (child, addr) = start(&[
        "--data-dir",
        d,
        "--addr",
        "127.0.0.1:0",
        "--trading-state",
        "halted",
    ]);
    let (status, body) = post(addr, "/orders", order);
    assert_eq!(status, 422, "{body}");
    assert_eq!(body["error"]["code"], Value::from("risk_trading_halted"));
    assert_eq!(
        body["error"]["context"]["rule"],
        Value::from("trading_halted")
    );
    stop(child);
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

/// `--operator-journal` records a trading-state change as NDJSON (ADR 0018 decision 7); with the
/// flag absent it writes nothing.
#[test]
fn operator_journal_records_a_trading_state_change() {
    let dir = data_dir("operator-journal");
    let d = dir.to_str().unwrap();
    let journal = dir.join("operator.ndjson");

    let (child, addr) = start(&[
        "--data-dir",
        d,
        "--addr",
        "127.0.0.1:0",
        "--operator-journal",
        journal.to_str().unwrap(),
    ]);
    let (status, body) = put(
        addr,
        "/trading-state",
        r#"{"state":"halted","reason":"e2e"}"#,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["data"],
        serde_json::json!({"state": "halted", "previous": "active"})
    );
    stop(child);

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
    use honba_messages::{
        Bar, BarAggregation, BarSpecification, BarType, Exchange, InstrumentId, PriceType,
        UnixNanos,
    };
    let id = InstrumentId::new("TCS", Exchange::new("NSE"));
    let bar_type = BarType::new(
        id,
        BarSpecification::new(1, BarAggregation::Minute, PriceType::Last),
    );
    let t = UnixNanos::from_u64(ts);
    let bar = Bar::new(bar_type, close, close, close, close, 10.0, t, t);
    serde_json::json!({ "bars": [bar] }).to_string()
}

#[test]
fn bar_ingest_flag_turns_on_post_bars_and_the_quote_follows() {
    let dir = data_dir("bar-ingest");
    let d = dir.to_str().unwrap();

    // Off by default: the route exists but says why it cannot take bars.
    let (child, addr) = start(&["--data-dir", d, "--addr", "127.0.0.1:0"]);
    let (status, body) = post(addr, "/bars", &bar_body(180_000_000_000, 77.0));
    assert_eq!(status, 503, "{body}");
    assert_eq!(
        body["error"]["context"]["reason"],
        Value::from("bar_ingest_disabled")
    );
    stop(child);

    let (mut child, addr) = start(&[
        "--data-dir",
        d,
        "--addr",
        "127.0.0.1:0",
        "--bar-ingest",
        "--bar-ingest-capacity",
        "16",
    ]);
    let (status, body) = post(addr, "/bars", &bar_body(180_000_000_000, 77.0));
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["accepted"], Value::from(1));
    // The pump applies the bar asynchronously; the quote follows within a moment.
    let mut bid = Value::Null;
    for _ in 0..100 {
        bid = get(addr, "/quotes?symbols=TCS").1["data"]["quotes"][0]["bid_price"].clone();
        if bid == 77.0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert_eq!(bid, 77.0);

    // Shutdown drops the sender and waits for the pump, then reports on stderr.
    Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(child.wait().unwrap().success());
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(err.contains("bar ingest: 1 bars, 0 errors"), "{err}");
}

fn risk_toml(name: &str, text: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-cli-serve")
        .join(format!("{name}.toml"));
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn risk_config_sets_the_rest_limits_and_defaults_are_unchanged_without_it() {
    let dir = data_dir("risk-config");
    let d = dir.to_str().unwrap();
    // 100.0 * 1 = 100 notional, above a 50 ceiling.
    let big = r#"{"instrument_id":{"symbol":"TCS","exchange":"NSE"},"side":"buy","qty":1.0,
        "order_type":"limit","price":100.0}"#;

    // No flag: no limits, the order is accepted.
    let (child, addr) = start(&["--data-dir", d, "--addr", "127.0.0.1:0"]);
    let (status, body) = post(addr, "/orders", big);
    assert_eq!(status, 200, "{body}");
    stop(child);

    let cfg = risk_toml(
        "risk-ok",
        "[risk]\nmax_notional = 50.0\nprice_collar_bps = 200\n",
    );
    let (child, addr) = start(&[
        "--data-dir",
        d,
        "--addr",
        "127.0.0.1:0",
        "--risk-config",
        cfg.to_str().unwrap(),
    ]);
    let (status, body) = post(addr, "/orders", big);
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        body["error"]["code"],
        Value::from("risk_max_notional_exceeded")
    );
    assert_eq!(
        body["error"]["context"]["rule"],
        Value::from("max_notional")
    );
    stop(child);
}

#[test]
fn an_invalid_risk_config_fails_before_listening_and_names_the_field() {
    let dir = data_dir("risk-config-bad");
    let cfg = risk_toml("risk-bad", "[risk]\nprice_collar_bps = 0\n");
    let out = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "serve",
            "--data-dir",
            dir.to_str().unwrap(),
            "--addr",
            "127.0.0.1:0",
        ])
        .args(["--risk-config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "must not listen");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("price_collar_bps"), "{err}");
}

/// Writes a `--ticks` file (format v1, ADR 0030) beside the bars: quotes bracketing both bars.
fn ticks_file(name: &str, body: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-cli-serve")
        .join(format!("{name}.ticks.ndjson"));
    std::fs::write(&path, body).unwrap();
    path
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
    let dir = data_dir("l2-ticks");
    let ticks = ticks_file("l2-ticks", TICKS_OK);
    let journals = dir.join("journals");
    let (child, addr) = start(&[
        "--data-dir",
        dir.to_str().unwrap(),
        "--addr",
        "127.0.0.1:0",
        "--journals-dir",
        journals.to_str().unwrap(),
        "--fill-model",
        "l2",
        "--ticks",
        ticks.to_str().unwrap(),
    ]);
    let (status, body) = post(addr, "/backtests", BACKTEST);
    assert_eq!(status, 200, "{body}");
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let done = finished_run(addr, &id);
    stop(child);
    assert_eq!(done["data"]["status"], "completed", "{done}");
}

/// A Parquet tick store built from the NDJSON file, kept beside the data dir (never inside it,
/// so its file is not mistaken for a bar source).
fn tick_store(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("honba-cli-serve")
        .join(format!("{name}.store"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn fill_model_l2_with_a_parquet_tick_store_runs_a_backtest_end_to_end() {
    let dir = data_dir("l2-parquet-ticks");
    let ticks = ticks_file("l2-parquet-ticks", TICKS_OK);
    let store = tick_store("l2-parquet-ticks");

    let imported = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "data",
            "ticks-import",
            "--ticks",
            ticks.to_str().unwrap(),
            "--out",
            store.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        imported.status.success(),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let report = String::from_utf8_lossy(&imported.stdout);
    assert!(report.contains("2 quote and 1 trade"), "{report}");

    let journals = dir.join("journals-parquet");
    let (child, addr) = start(&[
        "--data-dir",
        dir.to_str().unwrap(),
        "--addr",
        "127.0.0.1:0",
        "--journals-dir",
        journals.to_str().unwrap(),
        "--fill-model",
        "l2",
        "--ticks",
        store.to_str().unwrap(),
    ]);
    let (status, body) = post(addr, "/backtests", BACKTEST);
    assert_eq!(status, 200, "{body}");
    let id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let done = finished_run(addr, &id);
    stop(child);
    assert_eq!(done["data"]["status"], "completed", "{done}");
}

#[test]
fn fill_model_l2_without_ticks_fails_at_startup() {
    let dir = data_dir("l2-no-ticks");
    let output = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "serve",
            "--data-dir",
            dir.to_str().unwrap(),
            "--fill-model",
            "l2",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "must not listen");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("--fill-model l2") && err.contains("--ticks"),
        "{err}"
    );
}

#[test]
fn a_bad_ticks_file_fails_at_startup_naming_the_line() {
    let dir = data_dir("bad-ticks");
    let ticks = ticks_file("bad-ticks", &format!("{TICKS_OK}not json\n"));
    let output = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "serve",
            "--data-dir",
            dir.to_str().unwrap(),
            "--fill-model",
            "l2",
            "--ticks",
            ticks.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "must not listen");
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(err.contains("--ticks") && err.contains("line 5"), "{err}");
}

#[test]
fn an_unknown_fill_model_is_refused_by_the_parser() {
    let dir = data_dir("bad-fill-model");
    let output = Command::new(env!("CARGO_BIN_EXE_honba"))
        .args([
            "serve",
            "--data-dir",
            dir.to_str().unwrap(),
            "--fill-model",
            "none",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("bar_fill_through"));
}
